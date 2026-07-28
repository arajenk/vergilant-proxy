package quota

import "testing"

// The policy in one table, since every service now reads it from here.
func TestTheLadder(t *testing.T) {
	const limit = 10
	cases := []struct {
		name           string
		used           int
		capMonths      int
		refuse, record bool
	}{
		{"well under the cap", 3, 0, false, true},
		{"exactly at the cap, first offence", 10, 0, false, true},
		{"inside the grace window", 19, 0, false, true},
		{"at the grace multiple", 20, 0, false, false},
		{"past it, still forwarded", 30, 0, false, false},
		{"at the abuse ceiling", 40, 0, true, false},
		{"well past the ceiling", 400, 0, true, false},

		// A repeat offender loses the grace row and nothing else. Note the
		// ceiling row: still refused at 4x, not sooner.
		{"repeat offender under the cap", 9, 2, false, true},
		{"repeat offender at the cap", 10, 2, false, false},
		{"repeat offender below the ceiling", 39, 2, false, false},
		{"repeat offender at the ceiling", 40, 2, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := For(limit, c.used, c.capMonths)
			if got.Refuse != c.refuse || got.Record != c.record {
				t.Errorf("For(%d, %d, %d) = %+v, want refuse=%v record=%v",
					limit, c.used, c.capMonths, got, c.refuse, c.record)
			}
		})
	}
}

// Uncapped is every paid project. No ladder applies at any volume.
func TestUncappedIsNeverRefusedOrSilenced(t *testing.T) {
	for _, used := range []int{0, 1, 1_000_000} {
		got := For(0, used, 5)
		if got.Refuse || !got.Record {
			t.Errorf("For(0, %d, 5) = %+v, want forwarded and recorded", used, got)
		}
	}
	if got := RecordingStopsAt(0, 0); got != 0 {
		t.Errorf("RecordingStopsAt(0, 0) = %d, want 0 - there is no threshold to report", got)
	}
}

func TestRecordingStopsAtTightensForARepeatOffender(t *testing.T) {
	if got, want := RecordingStopsAt(100, 0), 200; got != want {
		t.Errorf("first offender = %d, want %d", got, want)
	}
	if got, want := RecordingStopsAt(100, RepeatOffenderMonths), 100; got != want {
		t.Errorf("repeat offender = %d, want %d", got, want)
	}
}
