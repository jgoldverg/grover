package output

import (
	"sort"
	"sync"

	"github.com/pterm/pterm"
)

// Printer renders structured CLI messages without relying on the logger.
type Printer struct {
	mu sync.Mutex
}

func NewPrinter() *Printer {
	return &Printer{}
}

func (p *Printer) Info(msg string, fields map[string]any) {
	p.printWith(pterm.Info, msg, fields)
}

func (p *Printer) Success(msg string, fields map[string]any) {
	p.printWith(pterm.Success, msg, fields)
}

func (p *Printer) Error(msg string, fields map[string]any) {
	p.printWith(pterm.Error, msg, fields)
}

func (p *Printer) Warn(msg string, fields map[string]any) {
	p.printWith(pterm.Warning, msg, fields)
}

func (p *Printer) printWith(logger pterm.PrefixPrinter, msg string, fields map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if fields == nil || len(fields) == 0 {
		logger.Println(msg)
		return
	}

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	logger.Println(msg)
	for _, k := range keys {
		pterm.Printf("  %s: %v\n", k, fields[k])
	}
}
