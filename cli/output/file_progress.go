package output

import (
	"io"
	"math"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pterm/pterm"
)

// FileProgressManager renders concurrent per-file progress bars using a single
// pterm multi printer area so the CLI stays tidy even with many transfers.
type FileProgressManager struct {
	title   string
	multi   *pterm.MultiPrinter
	bars    map[string]*pterm.ProgressbarPrinter
	started bool
	mu      sync.Mutex
}

func NewFileProgressManager(title string) *FileProgressManager {
	mp := pterm.DefaultMultiPrinter
	return &FileProgressManager{
		title: strings.TrimSpace(title),
		multi: &mp,
		bars:  make(map[string]*pterm.ProgressbarPrinter),
	}
}

// Start activates the shared area for all progress bars.
func (m *FileProgressManager) Start() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return nil
	}
	if m.multi == nil {
		mp := pterm.DefaultMultiPrinter
		m.multi = &mp
	}
	if _, err := m.multi.Start(); err != nil {
		return err
	}
	m.started = true
	return nil
}

// Stop tears down the multi printer area.
func (m *FileProgressManager) Stop() {
	if m == nil {
		return
	}
	m.mu.Lock()
	multi := m.multi
	started := m.started
	m.started = false
	m.multi = nil
	m.bars = nil
	m.mu.Unlock()

	if started && multi != nil {
		_, _ = multi.Stop()
	}
}

// NewSection provides a writer slot inside the shared area (e.g. for telemetry).
func (m *FileProgressManager) NewSection() io.Writer {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started || m.multi == nil {
		return nil
	}
	return m.multi.NewWriter()
}

// WrapWriter decorates writer so every write advances the bar for label.
func (m *FileProgressManager) WrapWriter(label string, total uint64, writer io.Writer) io.Writer {
	bar := m.ensureBar(label, total)
	if bar == nil || writer == nil {
		return writer
	}
	return &countingWriter{
		writer: writer,
		hook: func(n int) {
			if n > 0 {
				bar.Add(n)
			}
		},
	}
}

// WrapReader decorates reader so every read advances the bar for label.
func (m *FileProgressManager) WrapReader(label string, total uint64, reader io.Reader) io.Reader {
	bar := m.ensureBar(label, total)
	if bar == nil || reader == nil {
		return reader
	}
	return &countingReader{
		reader: reader,
		hook: func(n int) {
			if n > 0 {
				bar.Add(n)
			}
		},
	}
}

func (m *FileProgressManager) ensureBar(label string, total uint64) *pterm.ProgressbarPrinter {
	if m == nil || !m.started {
		return nil
	}
	key := strings.TrimSpace(label)
	if key == "" {
		key = "file"
	}
	key = filepath.ToSlash(key)

	m.mu.Lock()
	defer m.mu.Unlock()
	if bar, ok := m.bars[key]; ok {
		return bar
	}

	totalInt := clampToInt(total)
	writer := m.multi.NewWriter()
	bar, err := pterm.DefaultProgressbar.
		WithWriter(writer).
		WithTitle(key).
		WithTotal(totalInt).
		WithShowElapsedTime(false).
		WithShowCount(false).
		WithRemoveWhenDone(true).
		Start()
	if err != nil {
		return nil
	}
	m.bars[key] = bar
	return bar
}

func clampToInt(v uint64) int {
	if v == 0 {
		return 1
	}
	max := uint64(math.MaxInt32)
	if uint64(v) > max {
		return int(max)
	}
	return int(v)
}

type countingWriter struct {
	writer io.Writer
	hook   func(int)
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.writer.Write(p)
	if n > 0 && cw.hook != nil {
		cw.hook(n)
	}
	return n, err
}

type countingReader struct {
	reader io.Reader
	hook   func(int)
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.reader.Read(p)
	if n > 0 && cr.hook != nil {
		cr.hook(n)
	}
	return n, err
}
