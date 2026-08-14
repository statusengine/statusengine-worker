package main

import (
	"fmt"
	"testing"
)

// TestMarkerRoundTrip is the load-bearing property: every hostname this tool
// publishes must be recoverable as the sequence number it stands for. If it
// were not, verify would report perfectly delivered events as missing.
func TestMarkerRoundTrip(t *testing.T) {
	const runID = "r1"

	for _, seq := range []int{0, 1, 99, 100, 12345, 999999} {
		hostname := hostnameFor(runID, seq)
		got, ok := parseSeq(hostname, runID)
		if !ok {
			t.Fatalf("parseSeq(%q) reported no match", hostname)
		}
		if got != seq {
			t.Fatalf("parseSeq(%q) = %d, want %d", hostname, got, seq)
		}
	}
}

// TestParseSeqRejectsForeignRows guards the other direction: a row that
// belongs to a different run - or to a real host that happens to look
// similar - must never be counted as a delivered event of this run.
func TestParseSeqRejectsForeignRows(t *testing.T) {
	const runID = "r1"

	foreign := []string{
		"localhost",                    // an ordinary host
		"lt-r2-5",                      // another run
		"lt-r1",                        // no sequence number
		"lt-r1-",                       // empty sequence number
		"lt-r1-abc",                    // not a number
		"lt-r10-5",                     // prefix of the run id, not the run id
		"prefix-lt-r1-5",               // marker embedded in a longer name
		hostnamePrefix + runID + "-5x", // trailing junk
	}

	for _, hostname := range foreign {
		if seq, ok := parseSeq(hostname, runID); ok {
			t.Errorf("parseSeq(%q) accepted a foreign row as seq %d", hostname, seq)
		}
	}
}

func TestMissingRanges(t *testing.T) {
	tests := []struct {
		name  string
		seen  []int
		count int
		want  []int
	}{
		{"nothing missing", []int{0, 1, 2}, 3, nil},
		{"nothing arrived", nil, 3, []int{0, 1, 2}},
		{"hole in the middle", []int{0, 3}, 4, []int{1, 2}},
		{"tail missing", []int{0, 1}, 4, []int{2, 3}},
		// A row beyond the expected count must not mask a genuine gap.
		{"stray high sequence", []int{0, 2, 99}, 3, []int{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := make(map[int]struct{}, len(tt.seen))
			for _, seq := range tt.seen {
				seen[seq] = struct{}{}
			}

			got := missingRanges(seen, tt.count)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("missingRanges = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestFormatRanges covers the compression, whose whole purpose is to make a
// whole-batch loss legible: 100 consecutive missing events must read as one
// range, not as a hundred numbers.
func TestFormatRanges(t *testing.T) {
	whole := make([]int, 0, 100)
	for i := 200; i < 300; i++ {
		whole = append(whole, i)
	}

	tests := []struct {
		name string
		nums []int
		want string
	}{
		{"empty", nil, "none"},
		{"single", []int{7}, "7"},
		{"one run", []int{1, 2, 3}, "1-3"},
		{"mixed", []int{1, 2, 3, 7, 9, 10}, "1-3, 7, 9-10"},
		{"unsorted input", []int{9, 1, 10, 2, 3, 7}, "1-3, 7, 9-10"},
		{"a whole batch", whole, "200-299"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatRanges(tt.nums); got != tt.want {
				t.Fatalf("formatRanges = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestFormatRangesTruncates keeps a pathological result readable: thousands
// of separate single-event gaps must not print thousands of groups.
func TestFormatRangesTruncates(t *testing.T) {
	var scattered []int
	for i := 0; i < 200; i++ {
		scattered = append(scattered, i*2) // every other number, so no two merge
	}

	got := formatRanges(scattered)
	if len(got) > 300 {
		t.Fatalf("output not truncated, got %d chars", len(got))
	}
	if want := "more ranges)"; got[len(got)-len(want):] != want {
		t.Fatalf("expected a truncation notice, got %q", got)
	}
}

// TestHostnameLikeMatchesOnlyThisRun documents what cleanup deletes. The
// pattern is interpolated into a LIKE, so a run id that widened it would
// delete another run's rows - or, with a bare %, the entire table.
func TestHostnameLikeMatchesOnlyThisRun(t *testing.T) {
	got := hostnameLike("r1")
	if want := "lt-r1-%"; got != want {
		t.Fatalf("hostnameLike = %q, want %q", got, want)
	}
	if hostnameLike("r1") == hostnameLike("r2") {
		t.Fatal("two runs share a delete pattern")
	}
}
