package groverclient

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/groverserver"
	"github.com/jgoldverg/grover/pkg/udpwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type GroverClient struct {
	CredService     *CredentialService
	ResourceService *FileResourceService
	HeartBeatClient *HeartBeatService
	ServerClient    *GroverServerCommands
	TransferClient  *TransferService
	conn            *grpc.ClientConn
	cfg             internal.AppConfig
}

func NewGroverClient(cfg internal.AppConfig) *GroverClient { return &GroverClient{cfg: cfg} }

func (c *GroverClient) Initialize(ctx context.Context, policy RoutePolicy) error {
	var (
		cc         *grpc.ClientConn         // the real conn pointer (may stay nil)
		ci         grpc.ClientConnInterface // interface we pass to services
		err        error
		wantRemote = policy == RouteForceRemote ||
			(policy == RouteAuto && strings.TrimSpace(c.cfg.ServerURL) != "")
	)

	if wantRemote {
		cc, err = c.dialTLS(ctx, c.cfg.ServerURL, c.cfg.CACertFile)
		if err != nil {
			return err
		}
		ci = cc
	}

	c.conn = cc

	var e error
	c.ResourceService, e = NewFileResourceService(&c.cfg, ci, policy)
	if e != nil {
		return e
	}
	c.CredService, e = NewCredentialService(&c.cfg, ci, policy)
	if e != nil {
		return e
	}
	c.HeartBeatClient = NewHeartBeatService(&c.cfg, c.conn)
	c.ServerClient = NewGroverServerCommands(&c.cfg, c.conn)
	c.TransferClient, e = NewClientTransferService(&c.cfg, ci, policy)
	if e != nil {
		return e
	}
	return nil
}

func (c *GroverClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *GroverClient) dialTLS(ctx context.Context, target, caPath string) (*grpc.ClientConn, error) {
	// Build root pool: system roots by default; add custom CA if provided.
	roots, _ := x509.SystemCertPool()
	if caPath != "" {
		pem, err := os.ReadFile(os.ExpandEnv(caPath))
		if err != nil {
			return nil, err
		}
		if roots == nil {
			roots = x509.NewCertPool()
		}
		roots.AppendCertsFromPEM(pem)
	}
	creds := credentials.NewTLS(&tls.Config{RootCAs: roots})

	// Give dialing a sane default timeout if the caller didn’t.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(creds),
	)
}

// DiscoverPMTU probes the server and returns the largest UDP payload size (bytes)
// that round-trips using your udpwire MTU probe/ack. It automatically handles
// IPv6 or IPv4 based on the host and DNS.
func (g *GroverClient) DiscoverPMTU(
	ctx context.Context,
	server string,
	port int,
	minSize, maxSize int,
	perTry time.Duration,
) (int, error) {
	host, tgtPort, err := splitHostPortDefault(server, port)
	if err != nil {
		return 0, fmt.Errorf("target parse: %w", err)
	}

	// Build candidates (prefer v6 if host is an IPv6 literal or has AAAA)
	cands, err := resolveUDPCandidates(ctx, host, tgtPort)
	if err != nil {
		return 0, err
	}
	if len(cands) == 0 {
		return 0, fmt.Errorf("no address candidates for %s", server)
	}

	// Defaults
	if minSize <= 0 {
		minSize = 1200
	}
	if maxSize <= 0 || maxSize > 65507 {
		maxSize = 1400 // conservative default
	}
	if perTry <= 0 {
		perTry = 250 * time.Millisecond
	}

	internal.Info("mtu probe starting", internal.Fields{
		"server":   server,
		"host":     host,
		"port":     tgtPort,
		"min_size": minSize,
		"max_size": maxSize,
		"per_try":  perTry,
	})

	var lastErr error
	for _, c := range cands {
		internal.Debug("mtu probe routing candidate", internal.Fields{
			"network": c.network,
			"addr":    c.addr.String(),
		})
		best, err := probeOnCandidate(ctx, c.network, c.addr, minSize, maxSize, perTry)
		if err == nil {
			internal.Info("mtu probe candidate succeeded", internal.Fields{
				"network": c.network,
				"addr":    c.addr.String(),
				"mtu":     best,
			})
			return best, nil
		}
		internal.Warn("mtu probe candidate failed", internal.Fields{
			"network": c.network,
			"addr":    c.addr.String(),
			"error":   err,
		})
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("all candidates failed")
	}
	internal.Error("mtu probe failed", internal.Fields{
		"server": server,
		"error":  lastErr,
	})
	return 0, lastErr
}

type udpCand struct {
	network string // "udp4" or "udp6"
	addr    *net.UDPAddr
}

// resolveUDPCandidates returns a preferred-order list of UDP candidates.
// Order: if host is an IPv6 literal -> v6 only; if IPv4 literal -> v4 only;
// if hostname -> IPv6 addresses first (AAAA), then IPv4 (A). We’ll gracefully
// fall back if a family isn’t usable on the local machine.
func resolveUDPCandidates(ctx context.Context, host string, port int) ([]udpCand, error) {
	// Literal?
	if ip := net.ParseIP(host); ip != nil {
		if ip.To4() != nil {
			return []udpCand{{network: "udp4", addr: &net.UDPAddr{IP: ip, Port: port}}}, nil
		}
		return []udpCand{{network: "udp6", addr: &net.UDPAddr{IP: ip, Port: port}}}, nil
	}

	// Hostname: resolve both families.
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("dns lookup %q: %w", host, err)
	}
	var v6, v4 []udpCand
	for _, a := range addrs {
		ip := a.IP
		if ip == nil {
			continue
		}
		if ip.To4() != nil {
			v4 = append(v4, udpCand{"udp4", &net.UDPAddr{IP: ip, Port: port}})
		} else {
			v6 = append(v6, udpCand{"udp6", &net.UDPAddr{IP: ip, Port: port, Zone: a.Zone}})
		}
	}
	// Prefer v6 then v4; caller will fall back automatically.
	return append(v6, v4...), nil
}

func probeOnCandidate(
	ctx context.Context,
	network string, // "udp4" or "udp6"
	raddr *net.UDPAddr,
	minSize, maxSize int,
	perTry time.Duration,
) (int, error) {
	// Family-matched local socket
	pc, err := net.ListenPacket(network, ":0")
	if err != nil {
		return 0, fmt.Errorf("listen %s: %w", network, err)
	}
	defer pc.Close()
	if uc, ok := pc.(*net.UDPConn); ok {
		_ = uc.SetWriteBuffer(1 << 20)
		_ = uc.SetReadBuffer(1 << 20)
	}

	token := randU64()
	probeBase := randU32()

	internal.Debug("mtu probe candidate start", internal.Fields{
		"network":    network,
		"remote":     raddr.String(),
		"token":      token,
		"probe_base": probeBase,
	})

	try := func(target int) (bool, error) {
		internal.Debug("mtu probe attempt", internal.Fields{
			"network": network,
			"remote":  raddr.String(),
			"size":    target,
		})
		probeID := probeBase + uint32(target)
		pkt, err := udpwire.BuildMtuProbe(token, probeID, target)
		if err != nil {
			internal.Error("mtu probe build failed", internal.Fields{
				"network": network,
				"remote":  raddr.String(),
				"size":    target,
				"error":   err,
			})
			return false, err
		}
		if _, err := pc.WriteTo(pkt, raddr); err != nil {
			internal.Warn("mtu probe send failed", internal.Fields{
				"network": network,
				"remote":  raddr.String(),
				"size":    target,
				"error":   err,
			})
			if errors.Is(err, syscall.EMSGSIZE) {
				return false, nil
			}
			return false, fmt.Errorf("send(%s %s): %w", network, raddr.String(), err)
		}

		deadline := time.Now().Add(perTry)
		buf := make([]byte, 1500) // ACK is 30B
		for {
			_ = pc.SetReadDeadline(deadline)
			n, from, err := pc.ReadFrom(buf)
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				internal.Debug("mtu probe attempt timeout", internal.Fields{
					"network": network,
					"remote":  raddr.String(),
					"size":    target,
				})
				return false, nil
			}
			if err != nil {
				internal.Warn("mtu probe read failed", internal.Fields{
					"network": network,
					"remote":  raddr.String(),
					"size":    target,
					"error":   err,
				})
				return false, fmt.Errorf("read ack: %w", err)
			}
			if !sameEndpoint(from, raddr) {
				continue
			}
			d := buf[:n]
			if !udpwire.IsMtuProbeAck(d) {
				continue
			}
			tok, pid, observed, _, err := udpwire.ParseMtuProbeAck(d)
			if err != nil || tok != token || pid != probeID {
				continue
			}
			_ = observed // usually equals target; OK if it doesn’t
			internal.Info("mtu probe ack received", internal.Fields{
				"network":  network,
				"remote":   raddr.String(),
				"size":     target,
				"observed": observed,
			})
			return true, nil
		}
	}

	ok, err := try(minSize)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("%s %s: no response at %d bytes", network, raddr, minSize)
	}

	for maxSize < 65507 {
		ok, err = try(maxSize)
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		next := maxSize * 2
		if next > 65507 {
			break
		}
		maxSize = next
	}

	// Binary search
	lo, hi, best := minSize, maxSize, minSize
	for lo <= hi {
		mid := lo + (hi-lo)/2
		ok, err := try(mid)
		if err != nil {
			return 0, err
		}
		if ok {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return best, nil
}

func splitHostPortDefault(target string, fallbackPort int) (host string, port int, err error) {
	port = fallbackPort
	if port <= 0 {
		port = int(groverserver.DefaultMtuPort)
	}

	if strings.Contains(target, "]") || strings.Count(target, ":") == 1 {
		if h, p, e := net.SplitHostPort(target); e == nil {
			if fallbackPort <= 0 {
				if parsed, perr := strconv.Atoi(p); perr == nil && parsed != 0 {
					return h, parsed, nil
				}
			}
			return h, port, nil
		}
	}

	if ip := net.ParseIP(target); ip != nil && ip.To4() == nil {
		return target, port, nil
	}
	return target, port, nil
}

func sameEndpoint(a net.Addr, b *net.UDPAddr) bool {
	ua, ok := a.(*net.UDPAddr)
	if !ok {
		return false
	}
	// net.IP.Equal treats IPv4 and IPv4-mapped IPv6 as equal.
	if ua.Port != b.Port {
		return false
	}
	// Zones may differ; only relevant for link-local v6.
	if ua.IP.Equal(b.IP) {
		return true
	}
	// If b has a zone, ignore zone on comparison (best-effort)
	uai := ua.IP
	bi := b.IP
	if z := b.Zone; z != "" && ua.Zone == "" {
		_ = z // we intentionally ignore zone differences here
	}
	return uai.Equal(bi)
}

func randU64() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint64(b[:])
}
func randU32() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
