package queue

import (
	"regexp"
	"strconv"
	"strings"
)

// perfDataPoint is one label=value[unit];warn;crit;min;max metric parsed
// out of a Nagios/Icinga perf_data string (CLAUDE.md rule 5). Only Label,
// Value and Unit are kept - statusengine_perfdata has no columns for the
// warn/crit/min/max thresholds, and Graphite only wants a bare numeric
// value per path.
type perfDataPoint struct {
	Label string
	Value float64
	Unit  string
}

// valueUnitPattern splits a token's value part (e.g. "0.084000ms", "0%")
// into its leading numeric value and trailing unit suffix.
var valueUnitPattern = regexp.MustCompile(`^([+-]?[0-9]*\.?[0-9]+(?:[eE][+-]?[0-9]+)?)(.*)$`)

// parsePerfData splits a Nagios/Icinga perf_data string, e.g.
// `rta=0.084000ms;100.000000;500.000000;0.000000 pl=0%;20;60;0`, into its
// individual metrics. Labels containing spaces arrive single-quoted (e.g.
// `'response time'=0.5s`); tokens that are malformed or whose value is "U"
// (undefined, per the plugin API) are skipped rather than failing the
// whole batch over one bad metric.
func parsePerfData(raw string) []perfDataPoint {
	var points []perfDataPoint

	for _, tok := range tokenizePerfData(raw) {
		eq := strings.IndexByte(tok, '=')
		if eq < 0 {
			continue
		}

		label := strings.Trim(tok[:eq], "'")
		valueUnit := strings.SplitN(tok[eq+1:], ";", 2)[0]

		m := valueUnitPattern.FindStringSubmatch(valueUnit)
		if m == nil {
			continue
		}
		value, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			continue
		}

		points = append(points, perfDataPoint{Label: label, Value: value, Unit: m[2]})
	}

	return points
}

// tokenizePerfData splits raw on whitespace while keeping single-quoted
// labels (which may themselves contain spaces) intact.
func tokenizePerfData(raw string) []string {
	var tokens []string
	var cur strings.Builder
	inQuotes := false

	for _, r := range raw {
		switch {
		case r == '\'':
			inQuotes = !inQuotes
			cur.WriteRune(r)
		case r == ' ' && !inQuotes:
			if cur.Len() > 0 {
				tokens = append(tokens, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}

	return tokens
}
