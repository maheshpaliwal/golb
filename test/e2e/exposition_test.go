package e2e

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// exposition is a parsed Prometheus text-format (0.0.4) document.
type exposition struct {
	types   map[string]string // metric family -> counter | gauge | histogram
	help    map[string]string
	samples []sample
}

type sample struct {
	name   string
	labels map[string]string
	value  float64
}

var (
	metricName = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelName  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// parseExposition parses the text format strictly enough to catch what a
// Prometheus scrape would reject: bad names, samples without a TYPE line,
// duplicate series, unparsable values, and histograms whose buckets are not
// cumulative or do not end in +Inf matching _count.
func parseExposition(text string) (*exposition, error) {
	exp := &exposition{types: make(map[string]string), help: make(map[string]string)}
	seen := make(map[string]bool)
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	line := 0
	for scanner.Scan() {
		line++
		text := scanner.Text()
		if text == "" {
			continue
		}
		if strings.HasPrefix(text, "#") {
			err := exp.comment(text)
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", line, err)
			}
			continue
		}
		s, err := parseSample(text)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w: %q", line, err, text)
		}
		if exp.types[exp.family(s.name)] == "" {
			return nil, fmt.Errorf("line %d: sample %q has no preceding TYPE line", line, s.name)
		}
		key := s.key()
		if seen[key] {
			return nil, fmt.Errorf("line %d: duplicate series %s", line, key)
		}
		seen[key] = true
		exp.samples = append(exp.samples, s)
	}
	err := scanner.Err()
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(text, "\n") {
		return nil, errors.New("document does not end with a newline")
	}
	return exp, exp.checkHistograms()
}

func (e *exposition) comment(text string) error {
	fields := strings.SplitN(text, " ", 4)
	if len(fields) < 3 || (fields[1] != "HELP" && fields[1] != "TYPE") {
		return nil // a plain comment
	}
	name := fields[2]
	if !metricName.MatchString(name) {
		return fmt.Errorf("invalid metric name %q", name)
	}
	if fields[1] == "HELP" {
		if _, dup := e.help[name]; dup {
			return fmt.Errorf("second HELP line for %s", name)
		}
		e.help[name] = strings.Join(fields[3:], " ")
		return nil
	}
	if len(fields) != 4 {
		return fmt.Errorf("TYPE line for %s has no type", name)
	}
	switch fields[3] {
	case "counter", "gauge", "histogram", "summary", "untyped":
	default:
		return fmt.Errorf("unknown type %q for %s", fields[3], name)
	}
	if _, dup := e.types[name]; dup {
		return fmt.Errorf("second TYPE line for %s", name)
	}
	e.types[name] = fields[3]
	return nil
}

// family maps a histogram's _bucket, _sum and _count series to its family.
func (e *exposition) family(name string) string {
	if e.types[name] != "" {
		return name
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		base, ok := strings.CutSuffix(name, suffix)
		if ok && e.types[base] == "histogram" {
			return base
		}
	}
	return name
}

func parseSample(text string) (sample, error) {
	s := sample{labels: make(map[string]string)}
	rest := text
	brace := strings.IndexByte(text, '{')
	if brace < 0 {
		name, value, ok := strings.Cut(text, " ")
		if !ok {
			return s, errors.New("no value")
		}
		s.name, rest = name, value
	} else {
		s.name = text[:brace]
		end := strings.LastIndexByte(text, '}')
		if end < brace {
			return s, errors.New("unterminated label set")
		}
		err := parseLabels(text[brace+1:end], s.labels)
		if err != nil {
			return s, err
		}
		rest = strings.TrimPrefix(text[end+1:], " ")
	}
	if !metricName.MatchString(s.name) {
		return s, fmt.Errorf("invalid metric name %q", s.name)
	}
	value, _, _ := strings.Cut(rest, " ") // an optional timestamp may follow
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return s, fmt.Errorf("value: %w", err)
	}
	s.value = parsed
	return s, nil
}

func parseLabels(text string, into map[string]string) error {
	for text != "" {
		name, rest, ok := strings.Cut(text, "=")
		if !ok || !labelName.MatchString(name) {
			return fmt.Errorf("invalid label name in %q", text)
		}
		if !strings.HasPrefix(rest, `"`) {
			return fmt.Errorf("label %s: value is not quoted", name)
		}
		end := 1
		for end < len(rest) && rest[end] != '"' {
			if rest[end] == '\\' {
				end++
			}
			end++
		}
		if end >= len(rest) {
			return fmt.Errorf("label %s: unterminated value", name)
		}
		value, err := strconv.Unquote(rest[:end+1])
		if err != nil {
			return fmt.Errorf("label %s: %w", name, err)
		}
		if _, dup := into[name]; dup {
			return fmt.Errorf("label %s repeated", name)
		}
		into[name] = value
		text = strings.TrimPrefix(rest[end+1:], ",")
	}
	return nil
}

func (s sample) key() string {
	names := make([]string, 0, len(s.labels))
	for name := range s.labels {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(s.name)
	for _, name := range names {
		fmt.Fprintf(&b, ",%s=%q", name, s.labels[name])
	}
	return b.String()
}

func (s sample) matches(pairs []string) bool {
	for i := 0; i+1 < len(pairs); i += 2 {
		if s.labels[pairs[i]] != pairs[i+1] {
			return false
		}
	}
	return true
}

// sum adds every series of name whose labels include the given pairs.
func (e *exposition) sum(name string, pairs ...string) float64 {
	total := 0.0
	for _, s := range e.samples {
		if s.name == name && s.matches(pairs) {
			total += s.value
		}
	}
	return total
}

func (e *exposition) has(name string, pairs ...string) bool {
	for _, s := range e.samples {
		if s.name == name && s.matches(pairs) {
			return true
		}
	}
	return false
}

func (e *exposition) checkHistograms() error {
	type series struct {
		bounds []float64
		counts map[float64]float64
		count  float64
		hasSum bool
		hasCnt bool
	}
	all := make(map[string]*series)
	at := func(s sample, family string) *series {
		rest := sample{name: family, labels: make(map[string]string)}
		for name, value := range s.labels {
			if name != "le" {
				rest.labels[name] = value
			}
		}
		key := rest.key()
		if all[key] == nil {
			all[key] = &series{counts: make(map[float64]float64)}
		}
		return all[key]
	}
	for _, s := range e.samples {
		family := e.family(s.name)
		if e.types[family] != "histogram" {
			if e.types[family] == "counter" && (s.value < 0 || math.IsNaN(s.value)) {
				return fmt.Errorf("counter %s is %v", s.key(), s.value)
			}
			continue
		}
		h := at(s, family)
		switch s.name {
		case family + "_bucket":
			le, err := strconv.ParseFloat(s.labels["le"], 64)
			if err != nil {
				return fmt.Errorf("%s: le label %q: %w", s.name, s.labels["le"], err)
			}
			h.bounds = append(h.bounds, le)
			h.counts[le] = s.value
		case family + "_sum":
			h.hasSum = true
		case family + "_count":
			h.hasCnt = true
			h.count = s.value
		default:
			return fmt.Errorf("unexpected series %s in histogram %s", s.name, family)
		}
	}
	for key, h := range all {
		if !h.hasSum || !h.hasCnt {
			return fmt.Errorf("histogram %s lacks _sum or _count", key)
		}
		if !sort.Float64sAreSorted(h.bounds) {
			return fmt.Errorf("histogram %s: buckets are not in increasing le order", key)
		}
		if len(h.bounds) == 0 || !math.IsInf(h.bounds[len(h.bounds)-1], 1) {
			return fmt.Errorf("histogram %s has no +Inf bucket", key)
		}
		previous := 0.0
		for _, le := range h.bounds {
			if h.counts[le] < previous {
				return fmt.Errorf("histogram %s: bucket le=%v is below the previous bucket", key, le)
			}
			previous = h.counts[le]
		}
		if previous != h.count {
			return fmt.Errorf("histogram %s: +Inf bucket %v differs from _count %v", key, previous, h.count)
		}
	}
	return nil
}
