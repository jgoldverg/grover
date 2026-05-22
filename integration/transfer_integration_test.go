//go:build integration

package integration

import (
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

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	groverpb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/gserver"
	"github.com/jgoldverg/grover/pkg/util"
)

func TestRoutedTransferJobDirectRoundTrip(t *testing.T) {
	for _, protocol := range []string{"tcp", "udp"} {
		t.Run(protocol, func(t *testing.T) {
			ctx := context.Background()
			sourceServer := startTestServer(t, protocol)
			destServer := startTestServer(t, protocol)
			sourceClient := newTestClient(t, ctx, sourceServer)
			defer sourceClient.Close()
			destClient := newTestClient(t, ctx, destServer)
			defer destClient.Close()

			sourceRoot := filepath.Join(sourceServer.tmp, "source-root")
			destRoot := filepath.Join(destServer.tmp, "dest-root")
			if err := os.MkdirAll(sourceRoot, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(destRoot, 0o755); err != nil {
				t.Fatal(err)
			}
			src := filepath.Join(sourceRoot, "file.bin")
			makeDeterministicBlob(t, src, 1024*1024+333)

			sourceAPI := sourceClient.RoutedTransfer()
			destAPI := destClient.RoutedTransfer()
			if sourceAPI == nil || destAPI == nil {
				t.Fatal("routed transfer service unavailable")
			}
			pbProtocol := groverpb.DataProtocol_DATA_PROTOCOL_TCP
			if protocol == "udp" {
				pbProtocol = groverpb.DataProtocol_DATA_PROTOCOL_UDP
			}
			jobID := "routed-direct-" + protocol
			source, err := sourceAPI.PrepareTransferEndpoint(ctx, &groverpb.PrepareTransferEndpointRequest{
				RouteId:  "routed-direct",
				JobId:    jobID,
				Role:     groverpb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
				Protocol: pbProtocol,
				RootPath: sourceRoot,
			})
			if err != nil {
				t.Fatal(err)
			}
			dest, err := destAPI.PrepareTransferEndpoint(ctx, &groverpb.PrepareTransferEndpointRequest{
				RouteId:  "routed-direct",
				JobId:    jobID,
				Role:     groverpb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
				Protocol: pbProtocol,
				RootPath: destRoot,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sourceAPI.StartTransferJob(ctx, &groverpb.StartTransferJobRequest{
				RouteId:        "routed-direct",
				JobId:          jobID,
				Source:         source,
				Destination:    dest,
				FilesInFlight:  1,
				StreamsPerFile: 3,
			}); err != nil {
				t.Fatal(err)
			}
			waitForRoutedJobDone(t, ctx, sourceAPI, jobID)
			assertSameFile(t, "routed direct "+protocol, filepath.Join(destRoot, "file.bin"), src)
		})
	}
}

func TestRoutedTransferJobDirectMultiSizeUDP(t *testing.T) {
	ctx := context.Background()
	sourceServer := startTestServer(t, "udp")
	destServer := startTestServer(t, "udp")
	sourceClient := newTestClient(t, ctx, sourceServer)
	defer sourceClient.Close()
	destClient := newTestClient(t, ctx, destServer)
	defer destClient.Close()

	sourceRoot := filepath.Join(sourceServer.tmp, "source-root")
	destRoot := filepath.Join(destServer.tmp, "dest-root")
	if err := os.MkdirAll(sourceRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	sizes := []int{1, 512, 1500, 64*1024 + 333, 2*1024*1024 + 777}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d_bytes", size), func(t *testing.T) {
			name := fmt.Sprintf("file-%d.bin", size)
			src := filepath.Join(sourceRoot, name)
			makeDeterministicBlob(t, src, size)
			startRoutedDirectJob(t, ctx, sourceClient.RoutedTransfer(), destClient.RoutedTransfer(), "udp-multi", "udp-multi-"+fmt.Sprint(size), sourceRoot, destRoot, "udp", []string{name}, 1, 3)
			assertSameFile(t, "udp routed multi-size", filepath.Join(destRoot, name), src)
		})
	}
}

func TestRoutedTransferJobConcurrentDirect(t *testing.T) {
	for _, protocol := range []string{"tcp", "udp"} {
		t.Run(protocol, func(t *testing.T) {
			ctx := context.Background()
			sourceServer := startTestServer(t, protocol)
			destServer := startTestServer(t, protocol)
			sourceClient := newTestClient(t, ctx, sourceServer)
			defer sourceClient.Close()
			destClient := newTestClient(t, ctx, destServer)
			defer destClient.Close()

			sourceRoot := filepath.Join(sourceServer.tmp, "source-root")
			destRoot := filepath.Join(destServer.tmp, "dest-root")
			if err := os.MkdirAll(sourceRoot, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(destRoot, 0o755); err != nil {
				t.Fatal(err)
			}

			const files = 6
			names := make([]string, files)
			var wg sync.WaitGroup
			errCh := make(chan error, files)
			for i := 0; i < files; i++ {
				i := i
				names[i] = fmt.Sprintf("src-%02d.bin", i)
				src := filepath.Join(sourceRoot, names[i])
				makeDeterministicBlob(t, src, 512*1024+i*1024)

				wg.Add(1)
				go func() {
					defer wg.Done()
					jobID := fmt.Sprintf("%s-concurrent-%02d", protocol, i)
					err := startRoutedDirectJobErr(ctx, sourceClient.RoutedTransfer(), destClient.RoutedTransfer(), protocol+"-concurrent", jobID, sourceRoot, destRoot, protocol, []string{names[i]}, 1, 2)
					if err != nil {
						errCh <- err
						return
					}
					dst := filepath.Join(destRoot, names[i])
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

type testServer struct {
	tmp             string
	port            int
	protocol        string
	credentialsFile string
}

func startTestServer(t *testing.T, protocol string) testServer {
	t.Helper()
	_ = internal.ConfigureLogger("warn")
	tmp := t.TempDir()
	port := freePort(t)
	creds := filepath.Join(tmp, "credentials.toml")
	writeFile(t, creds, "[credentials]\n")

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
	})

	tc := testServer{
		tmp:             tmp,
		port:            port,
		protocol:        protocol,
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
			_, err = client.RoutedTransfer().ListTransferJobs(ctx, "")
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

func startRoutedDirectJob(t *testing.T, ctx context.Context, sourceAPI gclient.RoutedTransferAPI, destAPI gclient.RoutedTransferAPI, routeID, jobID, sourceRoot, destRoot, protocol string, files []string, filesInFlight, streamsPerFile uint32) {
	t.Helper()
	if err := startRoutedDirectJobErr(ctx, sourceAPI, destAPI, routeID, jobID, sourceRoot, destRoot, protocol, files, filesInFlight, streamsPerFile); err != nil {
		t.Fatal(err)
	}
}

func startRoutedDirectJobErr(ctx context.Context, sourceAPI gclient.RoutedTransferAPI, destAPI gclient.RoutedTransferAPI, routeID, jobID, sourceRoot, destRoot, protocol string, files []string, filesInFlight, streamsPerFile uint32) error {
	if sourceAPI == nil || destAPI == nil {
		return fmt.Errorf("routed transfer service unavailable")
	}
	pbProtocol := groverpb.DataProtocol_DATA_PROTOCOL_TCP
	if protocol == "udp" {
		pbProtocol = groverpb.DataProtocol_DATA_PROTOCOL_UDP
	}
	source, err := sourceAPI.PrepareTransferEndpoint(ctx, &groverpb.PrepareTransferEndpointRequest{
		RouteId:  routeID,
		JobId:    jobID,
		Role:     groverpb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_SOURCE,
		Protocol: pbProtocol,
		RootPath: sourceRoot,
	})
	if err != nil {
		return err
	}
	dest, err := destAPI.PrepareTransferEndpoint(ctx, &groverpb.PrepareTransferEndpointRequest{
		RouteId:  routeID,
		JobId:    jobID,
		Role:     groverpb.TransferEndpointRole_TRANSFER_ENDPOINT_ROLE_DESTINATION,
		Protocol: pbProtocol,
		RootPath: destRoot,
	})
	if err != nil {
		return err
	}
	for _, name := range files {
		if _, err := os.Stat(filepath.Join(sourceRoot, name)); err != nil {
			return err
		}
	}
	if _, err := sourceAPI.StartTransferJob(ctx, &groverpb.StartTransferJobRequest{
		RouteId:        routeID,
		JobId:          jobID,
		Source:         source,
		Destination:    dest,
		Paths:          files,
		FilesInFlight:  filesInFlight,
		StreamsPerFile: streamsPerFile,
	}); err != nil {
		return err
	}
	return waitForRoutedJobDoneErr(ctx, sourceAPI, jobID)
}

func waitForRoutedJobDone(t *testing.T, ctx context.Context, api gclient.RoutedTransferAPI, jobID string) {
	t.Helper()
	if err := waitForRoutedJobDoneErr(ctx, api, jobID); err != nil {
		t.Fatal(err)
	}
}

func waitForRoutedJobDoneErr(ctx context.Context, api gclient.RoutedTransferAPI, jobID string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := api.GetTransferJob(ctx, jobID)
		if err != nil {
			return err
		}
		switch job.GetState() {
		case groverpb.RuntimeState_RUNTIME_STATE_DONE:
			return nil
		case groverpb.RuntimeState_RUNTIME_STATE_FAILED, groverpb.RuntimeState_RUNTIME_STATE_ABORTED, groverpb.RuntimeState_RUNTIME_STATE_EXPIRED:
			return fmt.Errorf("routed job %s ended in %s: %s", jobID, job.GetState(), job.GetErrorMessage())
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("routed job %s did not finish before deadline", jobID)
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
