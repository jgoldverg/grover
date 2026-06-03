package energy

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPowercapRoot = "/sys/class/powercap"
	powercapRootEnv     = "GROVER_RAPL_ROOT"
)

type RAPLDomain struct {
	Name string
	Path string
}

type RAPLMonitor struct {
	root         string
	domains      []RAPLDomain
	totalIndexes []int
}

func NewRAPLMonitor(root string) (*RAPLMonitor, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		root = strings.TrimSpace(os.Getenv(powercapRootEnv))
	}
	if root == "" {
		root = defaultPowercapRoot
	}
	domains, err := discoverRAPLDomains(root)
	if err != nil {
		return nil, err
	}
	if len(domains) == 0 {
		return nil, fmt.Errorf("no readable RAPL domains found under %s", root)
	}
	monitor := &RAPLMonitor{root: root, domains: domains}
	monitor.totalIndexes = chooseTotalIndexes(domains)
	return monitor, nil
}

func (m *RAPLMonitor) Domains() []RAPLDomain {
	if m == nil {
		return nil
	}
	return append([]RAPLDomain(nil), m.domains...)
}

func (m *RAPLMonitor) WriteCSVHeader(w *csv.Writer) error {
	if m == nil {
		return nil
	}
	header := []string{"timestamp_ns", "tick", "job_id", "route_id"}
	for _, domain := range m.domains {
		header = append(header, "energy_uj_"+domain.Name)
	}
	header = append(header, "energy_uj_sum_all", "energy_uj_total")
	if err := w.Write(header); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func (m *RAPLMonitor) WriteCSVRecord(w *csv.Writer, tick uint64, jobID string, routeID string, now time.Time) error {
	if m == nil {
		return nil
	}
	energies := make([]uint64, 0, len(m.domains))
	for _, domain := range m.domains {
		value, err := readUintFile(domain.Path)
		if err != nil {
			return err
		}
		energies = append(energies, value)
	}
	sumAll := uint64(0)
	for _, value := range energies {
		sumAll += value
	}
	total := uint64(0)
	for _, idx := range m.totalIndexes {
		if idx >= 0 && idx < len(energies) {
			total += energies[idx]
		}
	}
	record := []string{
		strconv.FormatInt(now.UnixNano(), 10),
		strconv.FormatUint(tick, 10),
		jobID,
		routeID,
	}
	for _, value := range energies {
		record = append(record, strconv.FormatUint(value, 10))
	}
	record = append(record, strconv.FormatUint(sumAll, 10), strconv.FormatUint(total, 10))
	if err := w.Write(record); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func discoverRAPLDomains(root string) ([]RAPLDomain, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("RAPL powercap root %s is not a directory", root)
	}
	nameCounts := map[string]int{}
	var domains []RAPLDomain
	var visit func(string) error
	visit = func(dir string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		energyPath := filepath.Join(dir, "energy_uj")
		if stat, err := os.Stat(energyPath); err == nil && !stat.IsDir() {
			if _, err := readUintFile(energyPath); err == nil {
				name := classifyDomain(dir)
				count := nameCounts[name]
				nameCounts[name] = count + 1
				if count > 0 {
					name = fmt.Sprintf("%s_%d", name, count)
				}
				domains = append(domains, RAPLDomain{Name: name, Path: energyPath})
			}
		}
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), "intel-rapl") {
				if err := visit(filepath.Join(dir, entry.Name())); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, top := range globIntelRAPL(root) {
		if err := visit(top); err != nil {
			return nil, err
		}
	}
	return domains, nil
}

func globIntelRAPL(root string) []string {
	matches, _ := filepath.Glob(filepath.Join(root, "intel-rapl:*"))
	sort.Strings(matches)
	return matches
}

func classifyDomain(dir string) string {
	raw, err := os.ReadFile(filepath.Join(dir, "name"))
	if err != nil {
		return "pkg"
	}
	name := strings.ToLower(strings.TrimSpace(string(raw)))
	switch {
	case strings.Contains(name, "dram"):
		return "dram"
	case strings.Contains(name, "core"):
		return "cores"
	case strings.Contains(name, "psys"):
		return "psys"
	case strings.Contains(name, "pkg") || strings.Contains(name, "package"):
		return "pkg"
	default:
		name = strings.Map(func(r rune) rune {
			switch {
			case r >= 'a' && r <= 'z':
				return r
			case r >= '0' && r <= '9':
				return r
			default:
				return '_'
			}
		}, name)
		name = strings.Trim(name, "_")
		if name == "" {
			return "pkg"
		}
		return name
	}
}

func chooseTotalIndexes(domains []RAPLDomain) []int {
	var psys []int
	var pkgDram []int
	for i, domain := range domains {
		switch {
		case domain.Name == "psys" || strings.HasPrefix(domain.Name, "psys_"):
			psys = append(psys, i)
		case domain.Name == "pkg" || strings.HasPrefix(domain.Name, "pkg_") || domain.Name == "dram" || strings.HasPrefix(domain.Name, "dram_"):
			pkgDram = append(pkgDram, i)
		}
	}
	if len(psys) > 0 {
		return psys
	}
	return pkgDram
}

func readUintFile(path string) (uint64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, errors.New("invalid RAPL energy value in " + path + ": " + err.Error())
	}
	return value, nil
}
