package gserver

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/energy"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type nodeEnergyLog struct {
	path    string
	file    *os.File
	writer  *csv.Writer
	monitor *energy.RAPLMonitor
	tick    uint64
	mu      sync.Mutex
}

type activeEnergyJob struct {
	jobID   string
	routeID string
}

func (m *TransferJobManager) nodeEnergyLoop() {
	defer close(m.energyDone)
	interval := m.energyEvery
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			m.appendNodeEnergy(now)
		}
	}
}

func (m *TransferJobManager) appendNodeEnergy(now time.Time) {
	if m == nil || m.nodeEnergy == nil {
		return
	}
	m.nodeEnergy.append(now, m.activeEnergyJobs())
}

func (m *TransferJobManager) activeEnergyJobs() []activeEnergyJob {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	runtimes := make([]*transferJobRuntime, 0, len(m.jobs))
	for _, runtime := range m.jobs {
		runtimes = append(runtimes, runtime)
	}
	m.mu.RUnlock()

	jobs := make([]activeEnergyJob, 0, len(runtimes))
	for _, runtime := range runtimes {
		runtime.mu.Lock()
		active := runtime.state == pb.RuntimeState_RUNTIME_STATE_PREPARING || runtime.state == pb.RuntimeState_RUNTIME_STATE_RUNNING
		jobID := runtime.jobID
		routeID := runtime.routeID
		runtime.mu.Unlock()
		if active {
			jobs = append(jobs, activeEnergyJob{jobID: jobID, routeID: routeID})
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].jobID == jobs[j].jobID {
			return jobs[i].routeID < jobs[j].routeID
		}
		return jobs[i].jobID < jobs[j].jobID
	})
	return jobs
}

func newNodeEnergyLog(baseDir string, monitor *energy.RAPLMonitor) (*nodeEnergyLog, error) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return nil, errors.New("job log directory is empty")
	}
	if monitor == nil {
		return nil, errors.New("energy monitor is required")
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(baseDir, "energy.csv")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	log := &nodeEnergyLog{
		path:    path,
		file:    file,
		writer:  csv.NewWriter(file),
		monitor: monitor,
	}
	header := []string{"timestamp_ns", "tick", "active_job_count", "job_id", "route_id"}
	header = append(header, monitor.EnergyColumnNames()...)
	if err := log.writer.Write(header); err != nil {
		_ = file.Close()
		return nil, err
	}
	log.writer.Flush()
	if err := log.writer.Error(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return log, nil
}

func (l *nodeEnergyLog) append(now time.Time, jobs []activeEnergyJob) {
	if l == nil || l.monitor == nil {
		return
	}
	energies, sumAll, total, err := l.monitor.ReadEnergyValues()
	if err != nil {
		internal.Warn("failed to sample node energy", internal.Fields{
			internal.FieldError: err.Error(),
			"energy_log_path":   l.path,
		})
		return
	}
	jobIDs := make([]string, 0, len(jobs))
	routeIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if strings.TrimSpace(job.jobID) != "" {
			jobIDs = append(jobIDs, job.jobID)
		}
		if strings.TrimSpace(job.routeID) != "" {
			routeIDs = append(routeIDs, job.routeID)
		}
	}
	record := []string{
		fmt.Sprintf("%d", now.UnixNano()),
		fmt.Sprintf("%d", l.tick),
		fmt.Sprintf("%d", len(jobs)),
		strings.Join(jobIDs, ";"),
		strings.Join(routeIDs, ";"),
	}
	for _, value := range energies {
		record = append(record, fmt.Sprintf("%d", value))
	}
	record = append(record, fmt.Sprintf("%d", sumAll), fmt.Sprintf("%d", total))

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writer == nil {
		return
	}
	if err := l.writer.Write(record); err != nil {
		internal.Warn("failed to write node energy sample", internal.Fields{
			internal.FieldError: err.Error(),
			"energy_log_path":   l.path,
		})
		return
	}
	l.writer.Flush()
	if err := l.writer.Error(); err != nil {
		internal.Warn("failed to flush node energy sample", internal.Fields{
			internal.FieldError: err.Error(),
			"energy_log_path":   l.path,
		})
		return
	}
	l.tick++
}

func (l *nodeEnergyLog) close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var err error
	if l.writer != nil {
		l.writer.Flush()
		if writerErr := l.writer.Error(); writerErr != nil {
			err = writerErr
		}
		l.writer = nil
	}
	if l.file != nil {
		if closeErr := l.file.Close(); err == nil {
			err = closeErr
		}
		l.file = nil
	}
	return err
}
