package queue

import "testing"

func TestParsePerfData(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []perfDataPoint
	}{
		{
			name: "single metric without unit",
			raw:  "users=0;20;50;0",
			want: []perfDataPoint{{Label: "users", Value: 0, Unit: ""}},
		},
		{
			name: "single metric with unit",
			raw:  "swap=0MB;0;0;0;0",
			want: []perfDataPoint{{Label: "swap", Value: 0, Unit: "MB"}},
		},
		{
			name: "multiple metrics separated by whitespace",
			raw:  "rta=0.084000ms;100.000000;500.000000;0.000000 pl=0%;20;60;0",
			want: []perfDataPoint{
				{Label: "rta", Value: 0.084, Unit: "ms"},
				{Label: "pl", Value: 0, Unit: "%"},
			},
		},
		{
			// The legacy worker keeps the quote characters as part of the
			// label rather than stripping them - see perfdata.go's doc
			// comment - so statusengine_perfdata.label matches it exactly.
			name: "single-quoted label containing spaces keeps its quotes",
			raw:  "'response time'=0.5s;1;2;0",
			want: []perfDataPoint{{Label: "'response time'", Value: 0.5, Unit: "s"}},
		},
		{
			name: "double-quoted label containing spaces is normalized to single quotes",
			raw:  `"response time"=0.5s;1;2;0`,
			want: []perfDataPoint{{Label: "'response time'", Value: 0.5, Unit: "s"}},
		},
		{
			name: "comma decimal separator is normalized to a dot",
			raw:  "temp=3,7C;;;",
			want: []perfDataPoint{{Label: "temp", Value: 3.7, Unit: "C"}},
		},
		{
			name: "negative comma decimal separator is normalized to a dot",
			raw:  "temp=-1,5C;;;",
			want: []perfDataPoint{{Label: "temp", Value: -1.5, Unit: "C"}},
		},
		{
			name: "doubled percent unit collapses to a single percent",
			raw:  "pl=0%%;20;60;0",
			want: []perfDataPoint{{Label: "pl", Value: 0, Unit: "%"}},
		},
		{
			name: "negative and signed values",
			raw:  "temp=-3.5C;;;",
			want: []perfDataPoint{{Label: "temp", Value: -3.5, Unit: "C"}},
		},
		{
			name: "undefined value is skipped",
			raw:  "users=0;20;50;0 broken=U;;;",
			want: []perfDataPoint{{Label: "users", Value: 0, Unit: ""}},
		},
		{
			name: "empty string yields no points",
			raw:  "",
			want: nil,
		},
		{
			name: "token without equals sign is skipped",
			raw:  "garbage users=1;;;",
			want: []perfDataPoint{{Label: "users", Value: 1, Unit: ""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parsePerfData(tt.raw)
			if len(got) != len(tt.want) {
				t.Fatalf("parsePerfData(%q) = %+v, want %+v", tt.raw, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("parsePerfData(%q)[%d] = %+v, want %+v", tt.raw, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// benchPerfData is a realistic perf_data string: several metrics, a quoted
// label with a space, an EU decimal separator and a percentage unit - the
// shapes parsePerfData's doc comment calls out.
const benchPerfData = `rta=0.069000ms;100.000000;500.000000;0.000000 pl=0%;20;60;0 ` +
	`'response time'=3,7s;5;10;0 load1=0.42;;;0 load5=0.38;;;0 load15=0.31;;;0`

// BenchmarkParsePerfData tracks the allocation cost of the highest-volume
// path in the worker: every service check carrying perf_data goes through
// here. It exists to keep the pre-sizing in parsePerfData/splitGauges
// honest - both slices used to grow from nil, reallocating and copying
// several times per check.
func BenchmarkParsePerfData(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		points := parsePerfData(benchPerfData)
		if len(points) != 6 {
			b.Fatalf("parsed %d points, want 6", len(points))
		}
	}
}
