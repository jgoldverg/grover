// Package transfer runs transfer jobs on a groverd node.
//
// A node plays two roles at once:
//
//   - Source: Manager.StartJob plans files (plan.go), schedules them with live
//     concurrency (manager.go), and hands each file to a FileExecutor
//     (executor.go: local copy, tcp_send.go, udp_send.go).
//   - Destination: prepared endpoints (endpoints.go) accept TCP ranges
//     (tcp_recv.go) or UDP file sessions (udp_recv.go) and report progress into
//     receive records (receive.go), which GetJob also serves.
//
// Jobs never create network paths. A job either copies locally or runs over a
// route session (looked up through the Sessions interface) that names the
// data endpoint, its token, and who dials. Job logs are written by history.go.
package transfer
