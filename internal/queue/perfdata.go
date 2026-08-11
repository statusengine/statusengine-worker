package queue

import (
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

// parsePerfData splits a Nagios/Icinga perf_data string, e.g.
// `rta=0.069000ms;100.000000;500.000000;0.000000 pl=0%;20;60;0`, into its
// individual metrics. This is a behavioural port of the legacy PHP worker's
// PerfdataParser (splitGauges + parseGauge character state machines, see
// https://github.com/statusengine/worker/blob/master/src/PerfdataParser.php
// and its test at
// https://github.com/statusengine/worker/blob/master/tests/StatusengineTest/PerfdataParserTest.php),
// including its less obvious behaviour:
//
//   - Labels may be single- or double-quoted to allow spaces (e.g.
//     'response time'=3.7s or "response time"=3.7s); double quotes are
//     normalized to single quotes. Like the legacy worker, the quote
//     characters are kept as part of Label rather than stripped, so
//     statusengine_perfdata.label matches the legacy worker's output
//     (and shadow-tests as identical via cmd/db_verifier) even for
//     quoted labels.
//   - Both '.' and ',' are accepted as decimal separators in the value
//     (some locales/plugins emit "3,7" instead of "3.7"); every ',' is
//     rewritten to '.' before the number is parsed.
//   - A unit of exactly "%%" (some plugins double-escape '%') collapses
//     to a single "%".
//
// Tokens that are malformed or whose value doesn't parse as a float are
// skipped rather than failing the whole batch over one bad metric.
func parsePerfData(raw string) []perfDataPoint {
	// One point per gauge at most - tokens that don't parse are skipped
	// below - so this capacity is exact for well-formed input and an upper
	// bound otherwise. Worth sizing: this runs for every service check
	// carrying perf_data, the highest-volume path in the worker.
	gauges := splitGauges(raw)
	points := make([]perfDataPoint, 0, len(gauges))

	for _, gauge := range gauges {
		label, rawValue, unit, ok := parseGauge(gauge)
		if !ok {
			continue
		}
		value, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			continue
		}
		points = append(points, perfDataPoint{Label: label, Value: value, Unit: unit})
	}

	return points
}

// splitGauges splits raw on whitespace while keeping single- or
// double-quoted labels (which may themselves contain spaces) intact,
// normalizing a double quote to a single quote as it goes - a port of
// PerfdataParser::splitGauges().
func splitGauges(raw string) []string {
	// Gauges are space-separated, so the number of spaces plus one is a
	// true upper bound. A quoted label containing spaces makes it an
	// overestimate, which costs nothing.
	gauges := make([]string, 0, strings.Count(raw, " ")+1)
	var cur []byte
	inQuotes := false

	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case (c == '\'' || c == '"') && !inQuotes:
			inQuotes = true
			cur = append(cur, '\'')
		case inQuotes && (c == '\'' || c == '"'):
			inQuotes = false
			cur = append(cur, '\'')
		case inQuotes:
			cur = append(cur, c)
		case c == ' ':
			if len(cur) > 0 {
				gauges = append(gauges, string(cur))
				cur = cur[:0]
			}
		default:
			cur = append(cur, c)
		}
	}
	if len(cur) > 0 {
		gauges = append(gauges, string(cur))
	}

	return gauges
}

// parseGauge parses one gauge token, e.g. "rta=0.069000ms;100;500;0", into
// its label, raw numeric value (US-decimal - any ',' already rewritten to
// '.') and unit - a port of PerfdataParser::parseGauge(), keeping only the
// fields statusengine_perfdata/Graphite actually use (see parsePerfData's
// doc comment for why warn/crit/min/max aren't tracked, and for the "%%"
// and quote-preservation behaviour). ok is false if the token has no '='
// or no value characters at all (e.g. Nagios's "U" for an undefined
// value), matching how the legacy worker drops those via its
// is_numeric($gaugeRaw['current']) check.
func parseGauge(gauge string) (label, value, unit string, ok bool) {
	eq := strings.IndexByte(gauge, '=')
	if eq < 0 {
		return "", "", "", false
	}
	label = gauge[:eq]

	var valueBuf, unitBuf []byte
	inUnit := false
	for i := eq + 1; i < len(gauge); i++ {
		c := gauge[i]
		if c == ';' {
			break
		}
		if inUnit {
			unitBuf = append(unitBuf, c)
			continue
		}
		switch {
		case c == ',':
			valueBuf = append(valueBuf, '.')
		case c == '.' || c == '-' || (c >= '0' && c <= '9'):
			valueBuf = append(valueBuf, c)
		default:
			inUnit = true
			unitBuf = append(unitBuf, c)
		}
	}

	if len(valueBuf) == 0 {
		return "", "", "", false
	}

	unit = string(unitBuf)
	if unit == "%%" {
		unit = "%"
	}
	return label, string(valueBuf), unit, true
}
