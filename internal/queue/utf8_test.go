package queue

import (
	"bytes"
	"encoding/json"
	"testing"
	"unicode/utf8"
)

func TestRepairUTF8(t *testing.T) {
	tests := []struct {
		name    string
		in      []byte
		want    string
		changed bool
	}{
		{
			name: "plain ascii is untouched",
			in:   []byte(`{"output":"OK - 1 of 1 hosts up"}`),
			want: `{"output":"OK - 1 of 1 hosts up"}`,
		},
		{
			name: "valid utf-8 umlauts are untouched",
			in:   []byte(`{"output":"Datenträger C: schön"}`),
			want: `{"output":"Datenträger C: schön"}`,
		},
		{
			name:    "cp1252 lowercase u umlaut",
			in:      []byte("{\"output\":\"\xFCberlast\"}"),
			want:    `{"output":"überlast"}`,
			changed: true,
		},
		{
			name:    "cp1252 lowercase a umlaut",
			in:      []byte("{\"output\":\"Datentr\xE4ger C:\"}"),
			want:    `{"output":"Datenträger C:"}`,
			changed: true,
		},
		{
			name:    "cp1252 uppercase A umlaut and sharp s",
			in:      []byte("{\"output\":\"\xC4nderung gr\xF6\xDFer\"}"),
			want:    `{"output":"Änderung größer"}`,
			changed: true,
		},
		{
			// 0x80 is a control character in ISO-8859-1 but the euro
			// sign in CP1252 - the range where the two disagree, and
			// exactly what Windows puts there.
			name:    "cp1252 euro sign in the 0x80-0x9F range",
			in:      []byte("{\"output\":\"Kosten: 5\x80\"}"),
			want:    `{"output":"Kosten: 5€"}`,
			changed: true,
		},
		{
			// The case the legacy worker's whole-document
			// mb_detect_encoding + iconv gets wrong: one bulk payload
			// carrying output from a UTF-8 host and a Windows host.
			// Converting the whole document from CP1252 would turn the
			// correct 'ü' (0xC3 0xBC) into 'Ã¼'. Repairing per byte
			// leaves it alone.
			name:    "mixed valid utf-8 and cp1252 in one payload",
			in:      []byte("[{\"output\":\"Datenträger\"},{\"output\":\"Datentr\xE4ger\"}]"),
			want:    `[{"output":"Datenträger"},{"output":"Datenträger"}]`,
			changed: true,
		},
		{
			name: "empty payload",
			in:   []byte{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed := repairUTF8(tt.in)

			if changed != tt.changed {
				t.Errorf("changed = %v, want %v", changed, tt.changed)
			}
			if string(got) != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
			if !utf8.Valid(got) {
				t.Error("result is still not valid UTF-8")
			}
		})
	}
}

// TestRepairUTF8ValidPayloadIsNotCopied pins the fast path. Every single
// message the worker handles goes through repairUTF8, and virtually all
// of them are already valid, so that case has to cost nothing beyond the
// scan - not a copy of the payload.
func TestRepairUTF8ValidPayloadIsNotCopied(t *testing.T) {
	in := []byte(`{"output":"Datenträger C: 42% belegt"}`)

	got, changed := repairUTF8(in)
	if changed {
		t.Fatal("a valid payload was reported as repaired")
	}
	if &got[0] != &in[0] {
		t.Error("a valid payload was copied instead of returned as-is")
	}
}

// TestRepairUTF8KeepsJSONParseable is the point of the whole exercise:
// a CP1252 byte in plugin output must survive as the character it was
// meant to be, all the way through the real decoder.
func TestRepairUTF8KeepsJSONParseable(t *testing.T) {
	// Shaped like a real statusngin_servicechecks bulk payload (see
	// .claude/specs/statusngin_servicechecks.json), with the output field
	// carrying what a Windows disk check would emit.
	raw := []byte("{\"messages\":[{\"timestamp_usec\":603182,\"servicecheck\":{" +
		"\"host_name\":\"win-srv-01\",\"service_description\":\"Disk C:\"," +
		"\"output\":\"CRITICAL - Datentr\xE4ger C: zu 95% belegt\",\"state\":2}}]}")

	repaired, changed := repairUTF8(raw)
	if !changed {
		t.Fatal("the payload should have needed a repair")
	}

	events, err := decodeServiceCheck(repaired)
	if err != nil {
		t.Fatalf("decodeServiceCheck: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("decoded %d events, want 1", len(events))
	}
	if want := "CRITICAL - Datenträger C: zu 95% belegt"; events[0].Output != want {
		t.Fatalf("output = %q, want %q", events[0].Output, want)
	}
}

// TestGoJSONSilentlyCorruptsInvalidUTF8 documents *why* the repair
// exists, since the behaviour is easy to assume wrong: unlike PHP's
// json_decode, which fails with JSON_ERROR_UTF8 and made the legacy
// worker drop the message, encoding/json accepts the payload and
// substitutes U+FFFD. Without repairUTF8 the corruption is silent - no
// error, nothing to alert on, and it reaches MySQL as valid UTF-8.
//
// If this test ever fails because Go started rejecting invalid UTF-8,
// that is good news and repairUTF8's rationale should be revisited.
func TestGoJSONSilentlyCorruptsInvalidUTF8(t *testing.T) {
	raw := []byte("{\"output\":\"Datentr\xE4ger\"}")

	var got struct {
		Output string `json:"output"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("encoding/json rejected invalid UTF-8 - the premise changed: %v", err)
	}

	if !bytes.ContainsRune([]byte(got.Output), utf8.RuneError) {
		t.Fatalf("expected U+FFFD in %q", got.Output)
	}
	if got.Output == "Datenträger" {
		t.Fatal("encoding/json repaired the byte on its own - repairUTF8 would be redundant")
	}
}

func BenchmarkRepairUTF8(b *testing.B) {
	valid := []byte(`[{"host_name":"localhost","service_description":"Ping",` +
		`"output":"OK - rta 0.417ms, lost 0%","state":0}]`)
	broken := []byte("[{\"host_name\":\"win-srv-01\",\"service_description\":\"Disk C:\"," +
		"\"output\":\"CRITICAL - Datentr\xE4ger C: zu 95% belegt\",\"state\":2}]")

	// The one that matters: every message pays this, so it has to report
	// zero allocations.
	b.Run("valid", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, changed := repairUTF8(valid); changed {
				b.Fatal("valid payload reported as repaired")
			}
		}
	})

	b.Run("needs repair", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, changed := repairUTF8(broken); !changed {
				b.Fatal("broken payload reported as clean")
			}
		}
	})
}
