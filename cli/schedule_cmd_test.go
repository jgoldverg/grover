package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func TestParseScheduleRowsSupportsColumnOrder(t *testing.T) {
	input := `route,allocated_bytes,job_id,source_node,destination_node,flow_count,forecast_id,slot_duration_seconds
tacc_buff,100030000.0,2309,tacc,buff,32,3,300.0
chi_buff,42,7,chi,buff,,,
`
	rows, err := parseScheduleRows(strings.NewReader(input), "test.csv")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].JobID != "2309" || rows[0].RouteKey != "tacc_buff" || rows[0].AllocatedBytes != 100030000 {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[0].FlowCount != 32 {
		t.Fatalf("first row flow count = %d, want 32", rows[0].FlowCount)
	}
	if rows[0].ForecastIndex != 3 || rows[0].SlotDuration != 5*time.Minute {
		t.Fatalf("first row timing = forecast %d slot %s", rows[0].ForecastIndex, rows[0].SlotDuration)
	}
	if rows[1].AllocatedBytes != 42 {
		t.Fatalf("second row bytes = %d, want 42", rows[1].AllocatedBytes)
	}
	if rows[1].FlowCount != 0 {
		t.Fatalf("second row flow count = %d, want 0", rows[1].FlowCount)
	}
}

func TestScheduleEventsGroupRowsByForecastSlot(t *testing.T) {
	rows := []scheduleRow{
		{JobID: "late", ForecastIndex: 3, SlotDuration: 5 * time.Minute},
		{JobID: "now-1", ForecastIndex: 1, SlotDuration: 5 * time.Minute},
		{JobID: "mid", ForecastIndex: 2, SlotDuration: 5 * time.Minute},
		{JobID: "now-2"},
	}
	events := scheduleEvents(rows)
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}
	if events[0].Offset != 0 || len(events[0].Rows) != 2 {
		t.Fatalf("first event = %+v, want two immediate rows", events[0])
	}
	if events[1].Offset != 5*time.Minute || events[1].Rows[0].JobID != "mid" {
		t.Fatalf("second event = %+v", events[1])
	}
	if events[2].Offset != 10*time.Minute || events[2].Rows[0].JobID != "late" {
		t.Fatalf("third event = %+v", events[2])
	}
}

func TestParseScheduleRowsRejectsMissingRequiredColumns(t *testing.T) {
	_, err := parseScheduleRows(strings.NewReader("job_id,route\n1,tacc_buff\n"), "bad.csv")
	if err == nil || !strings.Contains(err.Error(), "allocated_bytes") {
		t.Fatalf("error = %v, want missing allocated_bytes", err)
	}
}

func TestParseScheduleRowsRejectsFractionalFlowCount(t *testing.T) {
	input := `job_id,route,allocated_bytes,flow_count
1,tacc_buff,10,3.5
`
	_, err := parseScheduleRows(strings.NewReader(input), "bad.csv")
	if err == nil || !strings.Contains(err.Error(), "flow_count must be an integer") {
		t.Fatalf("error = %v, want fractional flow_count validation", err)
	}
}

func TestSyntheticScheduleSourceIsStableAndParseable(t *testing.T) {
	src := syntheticScheduleSource(1234, "tacc/buff", "job 7")
	want := "synthetic://1234/schedule/tacc_buff/job-job_7.bin"
	if src != want {
		t.Fatalf("synthetic source = %q, want %q", src, want)
	}
}

func TestScheduleDestinationFilePathTargetsOnlySyntheticRow(t *testing.T) {
	got := scheduleDestinationFilePath("192.5.86.166:22444:/home/cc/data/grover-dst/schedules", "tacc_uc", "job 7")
	want := "/home/cc/data/grover-dst/schedules/schedule/tacc_uc/job-job_7.bin"
	if got != want {
		t.Fatalf("destination cleanup path = %q, want %q", got, want)
	}
}

func TestScheduleServerRouteTemplateRequiresDestinationRoot(t *testing.T) {
	route := &pb.RouteConfig{
		Name:             "tacc_buff",
		Source:           "10.137.1.2:22444",
		Destination:      "10.137.132.2:22444",
		Via:              []string{"10.133.3.2:22444"},
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_TCP,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE,
		DataDirection:    pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION,
	}
	if _, err := scheduleTemplateFromServerRoute(route, "", &scheduleRunOptions{}); err == nil || !strings.Contains(err.Error(), "--destination-root") {
		t.Fatalf("scheduleTemplateFromServerRoute error = %v, want destination root validation", err)
	}
}

func TestScheduleServerRouteTemplateMapsNetworkRouteToSyntheticTransferRoute(t *testing.T) {
	route := &pb.RouteConfig{
		Name:             "tacc_buff",
		Source:           "10.137.1.2:22444",
		Destination:      "10.137.132.2:22444",
		Via:              []string{"10.133.3.2:22444"},
		Protocol:         pb.DataProtocol_DATA_PROTOCOL_UDP,
		ConnectionOrigin: pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE,
		DataDirection:    pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION,
	}
	tmpl, err := scheduleTemplateFromServerRoute(route, "/home/ubuntu/data/grover-dst/schedule", &scheduleRunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.Source != "10.137.1.2:22444:/unused/synthetic-source-root" {
		t.Fatalf("source = %q", tmpl.Source)
	}
	if tmpl.Destination != "10.137.132.2:22444:/home/ubuntu/data/grover-dst/schedule" {
		t.Fatalf("destination = %q", tmpl.Destination)
	}
	if tmpl.Protocol != "udp" || tmpl.Concurrency != 1 || tmpl.ParallelStreams != 1 {
		t.Fatalf("template options = %+v", tmpl)
	}
	if len(tmpl.Via) != 1 || tmpl.Via[0] != "10.133.3.2:22444" {
		t.Fatalf("via = %+v", tmpl.Via)
	}
}

func TestParseScheduledRunAt(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	runAt, err := parseScheduledRunAt("", 5*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if !runAt.Equal(now.Add(5 * time.Minute)) {
		t.Fatalf("runAt = %s", runAt)
	}
	runAt, err = parseScheduledRunAt("2026-06-01T13:30:00Z", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	if runAt.Hour() != 13 || runAt.Minute() != 30 {
		t.Fatalf("runAt = %s", runAt)
	}
	if _, err := parseScheduledRunAt("", 0, now); err == nil {
		t.Fatal("expected missing time error")
	}
	if _, err := parseScheduledRunAt("2026-06-01T13:30:00Z", time.Second, now); err == nil {
		t.Fatal("expected at/delay conflict")
	}
}

func TestScheduledTransferStoreLifecycle(t *testing.T) {
	store, err := newScheduledTransferStore(filepath.Join(t.TempDir(), "schedule.json"))
	if err != nil {
		t.Fatal(err)
	}
	entry, err := store.add(context.Background(), scheduledTransferEntry{
		ID:          "job-1",
		Route:       "uc-to-edu",
		Source:      "/src/file.bin",
		Destination: "/dst/file.bin",
		RunAt:       time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		State:       scheduledTransferPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	if entry.ID != "job-1" || entry.State != scheduledTransferPending {
		t.Fatalf("entry = %+v", entry)
	}
	listed, err := store.list()
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Route != "uc-to-edu" {
		t.Fatalf("listed = %+v", listed)
	}
	listed[0].State = scheduledTransferDone
	listed[0].TransferJobID = "transfer-1"
	if err := store.update(context.Background(), listed[0]); err != nil {
		t.Fatal(err)
	}
	updated, err := store.list()
	if err != nil {
		t.Fatal(err)
	}
	if updated[0].State != scheduledTransferDone || updated[0].TransferJobID != "transfer-1" {
		t.Fatalf("updated = %+v", updated[0])
	}
}

func TestScheduledTransferDue(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if !scheduledTransferDue(scheduledTransferEntry{State: scheduledTransferPending, RunAt: now.Add(-time.Second)}, now) {
		t.Fatal("past pending transfer should be due")
	}
	if scheduledTransferDue(scheduledTransferEntry{State: scheduledTransferPending, RunAt: now.Add(time.Second)}, now) {
		t.Fatal("future pending transfer should not be due")
	}
	if scheduledTransferDue(scheduledTransferEntry{State: scheduledTransferDone, RunAt: now.Add(-time.Second)}, now) {
		t.Fatal("done transfer should not be due")
	}
}
