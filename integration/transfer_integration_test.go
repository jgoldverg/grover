//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	"github.com/jgoldverg/grover/pkg/gserver"
	"github.com/jgoldverg/grover/pkg/util"
)

func TestTransferRoundTrip(t *testing.T) {
	for _, protocol := range []string{"tcp", "udp"} {
		t.Run(protocol+"_stream", func(t *testing.T) {
			ctx := context.Background()
			tc := startTestServer(t, protocol, 1)
			client := newTestClient(t, ctx, tc)
			defer client.Close()

			src := filepath.Join(tc.tmp, "src.bin")
			dst := filepath.Join(tc.tmp, "dst.bin")
			remote := filepath.Join(tc.tmp, "remote.bin")
			makeDeterministicBlob(t, src, 2*1024*1024)

			uploadFile(t, ctx, client, src, remote)
			downloadFile(t, ctx, client, remote, dst)

			if got, want := sha256File(t, dst), sha256File(t, src); got != want {
				t.Fatalf("%s checksum mismatch: got=%s want=%s", protocol, got, want)
			}
		})
	}
}

func TestTCPParallelChunkUploadAndDownload(t *testing.T) {
	ctx := context.Background()
	tc := startTestServer(t, "tcp", 4)
	client := newTestClient(t, ctx, tc)
	defer client.Close()

	src := filepath.Join(tc.tmp, "src-parallel.bin")
	dst := filepath.Join(tc.tmp, "dst-parallel.bin")
	remote := filepath.Join(tc.tmp, "remote-parallel.bin")
	makeDeterministicBlob(t, src, 8*1024*1024)

	uploadFile(t, ctx, client, src, remote)
	downloadFile(t, ctx, client, remote, dst)

	if got, want := sha256File(t, dst), sha256File(t, src); got != want {
		off, gotByte, wantByte := firstByteDiff(t, dst, src)
		t.Fatalf("tcp parallel checksum mismatch: got=%s want=%s dst_size=%d src_size=%d first_diff=%d got_byte=%d want_byte=%d", got, want, fileSize(t, dst), fileSize(t, src), off, gotByte, wantByte)
	}
}

func TestUDPTransferMultiSizeRoundTrip(t *testing.T) {
	ctx := context.Background()
	tc := startTestServer(t, "udp", 1)
	client := newTestClient(t, ctx, tc)
	defer client.Close()

	sizes := []int{
		1,
		512,
		1500,
		64*1024 + 333,
		2*1024*1024 + 777,
	}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d_bytes", size), func(t *testing.T) {
			src := filepath.Join(tc.tmp, fmt.Sprintf("udp-src-%d.bin", size))
			dst := filepath.Join(tc.tmp, fmt.Sprintf("udp-dst-%d.bin", size))
			remote := filepath.Join(tc.tmp, fmt.Sprintf("udp-remote-%d.bin", size))
			makeDeterministicBlob(t, src, size)

			uploadFile(t, ctx, client, src, remote)
			downloadFile(t, ctx, client, remote, dst)
			assertSameFile(t, "udp multi-size", dst, src)
		})
	}
}

func TestUDPConfiguredParallelSendersRoundTrip(t *testing.T) {
	ctx := context.Background()
	tc := startTestServer(t, "udp", 4)
	client := newTestClient(t, ctx, tc)
	defer client.Close()

	src := filepath.Join(tc.tmp, "udp-src-parallel-config.bin")
	dst := filepath.Join(tc.tmp, "udp-dst-parallel-config.bin")
	remote := filepath.Join(tc.tmp, "udp-remote-parallel-config.bin")
	makeDeterministicBlob(t, src, 4*1024*1024+123)

	uploadFile(t, ctx, client, src, remote)
	downloadFile(t, ctx, client, remote, dst)
	assertSameFile(t, "udp parallel-config", dst, src)
}

func TestConcurrentFileTransfers(t *testing.T) {
	for _, protocol := range []string{"tcp", "udp"} {
		t.Run(protocol, func(t *testing.T) {
			ctx := context.Background()
			tc := startTestServer(t, protocol, 4)
			client := newTestClient(t, ctx, tc)
			defer client.Close()

			const files = 6
			var wg sync.WaitGroup
			errCh := make(chan error, files)
			for i := 0; i < files; i++ {
				i := i
				src := filepath.Join(tc.tmp, fmt.Sprintf("src-%02d.bin", i))
				remote := filepath.Join(tc.tmp, fmt.Sprintf("remote-%02d.bin", i))
				dst := filepath.Join(tc.tmp, fmt.Sprintf("dst-%02d.bin", i))
				makeDeterministicBlob(t, src, 512*1024+i*1024)

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := uploadFileErr(ctx, client, src, remote); err != nil {
						errCh <- err
						return
					}
					if err := downloadFileErr(ctx, client, remote, dst); err != nil {
						errCh <- err
						return
					}
					if got, want := sha256File(t, dst), sha256File(t, src); got != want {
						off, gotByte, wantByte := firstByteDiff(t, dst, src)
						errCh <- fmt.Errorf("%s checksum mismatch for file %d: got=%s want=%s dst_size=%d src_size=%d first_diff=%d got_byte=%d want_byte=%d", protocol, i, got, want, fileSize(t, dst), fileSize(t, src), off, gotByte, wantByte)
					}
				}()
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestUDPConcurrentIndependentSessions(t *testing.T) {
	ctx := context.Background()
	tc := startTestServer(t, "udp", 1)
	client := newTestClient(t, ctx, tc)
	defer client.Close()

	const files = 10
	var wg sync.WaitGroup
	errCh := make(chan error, files)
	for i := 0; i < files; i++ {
		i := i
		src := filepath.Join(tc.tmp, fmt.Sprintf("udp-independent-src-%02d.bin", i))
		remote := filepath.Join(tc.tmp, fmt.Sprintf("udp-independent-remote-%02d.bin", i))
		dst := filepath.Join(tc.tmp, fmt.Sprintf("udp-independent-dst-%02d.bin", i))
		makeDeterministicBlob(t, src, 128*1024+i*17*1024)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := uploadFileErr(ctx, client, src, remote); err != nil {
				errCh <- err
				return
			}
			if err := downloadFileErr(ctx, client, remote, dst); err != nil {
				errCh <- err
				return
			}
			if got, want := sha256File(t, dst), sha256File(t, src); got != want {
				off, gotByte, wantByte := firstByteDiff(t, dst, src)
				errCh <- fmt.Errorf("udp independent checksum mismatch for file %d: got=%s want=%s dst_size=%d src_size=%d first_diff=%d got_byte=%d want_byte=%d", i, got, want, fileSize(t, dst), fileSize(t, src), off, gotByte, wantByte)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

type testServer struct {
	tmp              string
	port             int
	protocol         string
	parallelSenders  uint
	credentialsFile  string
	previousProtocol string
}

func startTestServer(t *testing.T, protocol string, parallelSenders uint) testServer {
	t.Helper()
	_ = internal.ConfigureLogger("warn")
	tmp := t.TempDir()
	port := freePort(t)
	creds := filepath.Join(tmp, "credentials.toml")
	writeFile(t, creds, "[credentials]\n")

	prevProtocol, hadProtocol := os.LookupEnv("GROVER_TRANSFER_PROTOCOL")
	t.Setenv("GROVER_TRANSFER_PROTOCOL", protocol)
	t.Setenv("GUDP_CLIENT_CONFIG_PARALLEL_SENDERS", fmt.Sprint(parallelSenders))
	t.Setenv("GUDP_CLIENT_CONFIG_SOCKET_BUFFER_SIZE", "8388608")
	if !hadProtocol {
		prevProtocol = ""
	}

	cfg := &internal.ServerConfig{
		Port:               port,
		CredentialsFile:    creds,
		TransferProtocol:   protocol,
		InsecureControl:    true,
		UDPReadBufferSize:  8 << 20,
		UDPWriteBufferSize: 8 << 20,
		LogLevel:           "warn",
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := gserver.NewGroverServer(ctx, cfg)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.StartServer(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		server.Stop()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
		}
		if hadProtocol {
			_ = os.Setenv("GROVER_TRANSFER_PROTOCOL", prevProtocol)
		} else {
			_ = os.Unsetenv("GROVER_TRANSFER_PROTOCOL")
		}
	})

	tc := testServer{
		tmp:             tmp,
		port:            port,
		protocol:        protocol,
		parallelSenders: parallelSenders,
		credentialsFile: creds,
	}
	waitForGRPC(t, tc)
	return tc
}

func newTestClient(t *testing.T, ctx context.Context, tc testServer) *gclient.Client {
	t.Helper()
	cfg := internal.AppConfig{
		ServerURL:        fmt.Sprintf("127.0.0.1:%d", tc.port),
		CredentialsFile:  tc.credentialsFile,
		TransferProtocol: tc.protocol,
		InsecureControl:  true,
		Route:            "server",
		LogLevel:         "warn",
	}
	client := gclient.NewClient(cfg)
	if err := client.Initialize(ctx, util.RouteForceRemote); err != nil {
		t.Fatalf("initialize client: %v", err)
	}
	return client
}

func waitForGRPC(t *testing.T, tc testServer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		client := newTestClientNoFatal(tc)
		err := client.Initialize(ctx, util.RouteForceRemote)
		if err == nil {
			_, err = client.Transfer().Enumerate(ctx, tc.tmp, false)
		}
		_ = client.Close()
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server did not become ready on port %d: %v", tc.port, lastErr)
}

func newTestClientNoFatal(tc testServer) *gclient.Client {
	cfg := internal.AppConfig{
		ServerURL:        fmt.Sprintf("127.0.0.1:%d", tc.port),
		CredentialsFile:  tc.credentialsFile,
		TransferProtocol: tc.protocol,
		InsecureControl:  true,
		Route:            "server",
		LogLevel:         "warn",
	}
	return gclient.NewClient(cfg)
}

func uploadFile(t *testing.T, ctx context.Context, client *gclient.Client, src, remote string) {
	t.Helper()
	if err := uploadFileErr(ctx, client, src, remote); err != nil {
		t.Fatal(err)
	}
}

func uploadFileErr(ctx context.Context, client *gclient.Client, src, remote string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	return client.Transfer().Put(ctx, remote, f, st.Size(), backend.ALWAYS)
}

func downloadFile(t *testing.T, ctx context.Context, client *gclient.Client, remote, dst string) {
	t.Helper()
	if err := downloadFileErr(ctx, client, remote, dst); err != nil {
		t.Fatal(err)
	}
}

func downloadFileErr(ctx context.Context, client *gclient.Client, remote, dst string) error {
	var buf bytes.Buffer
	if err := client.Transfer().Get(ctx, remote, &buf); err != nil {
		return err
	}
	return os.WriteFile(dst, buf.Bytes(), 0o644)
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("local listeners are not permitted in this environment: %v", err)
		}
		t.Fatalf("allocate port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func makeDeterministicBlob(t *testing.T, path string, size int) {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*31 + 7) % 251)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func assertSameFile(t *testing.T, label, gotPath, wantPath string) {
	t.Helper()
	if got, want := sha256File(t, gotPath), sha256File(t, wantPath); got != want {
		off, gotByte, wantByte := firstByteDiff(t, gotPath, wantPath)
		t.Fatalf("%s checksum mismatch: got=%s want=%s got_size=%d want_size=%d first_diff=%d got_byte=%d want_byte=%d", label, got, want, fileSize(t, gotPath), fileSize(t, wantPath), off, gotByte, wantByte)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st.Size()
}

func firstByteDiff(t *testing.T, gotPath, wantPath string) (int, byte, byte) {
	t.Helper()
	got, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("read %s: %v", gotPath, err)
	}
	want, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("read %s: %v", wantPath, err)
	}
	limit := len(got)
	if len(want) < limit {
		limit = len(want)
	}
	for i := 0; i < limit; i++ {
		if got[i] != want[i] {
			return i, got[i], want[i]
		}
	}
	if len(got) != len(want) {
		if len(got) < len(want) {
			return len(got), 0, want[len(got)]
		}
		return len(want), got[len(want)], 0
	}
	return -1, 0, 0
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
