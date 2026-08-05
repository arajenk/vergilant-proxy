// Package quota decides what happens once a project goes over its monthly
// limit.
//
// Going over costs you the monitoring, not your traffic. We stop recording, so
// the dashboard goes quiet, but the calls keep going through.
package quota

const (
	// How far over the limit we keep recording for a first offender. Alerts stay
	// live through the start of a spike, which is when a runaway agent shows up.
	GraceMultiple = 2

	// Where forwarding actually stops. Abuse protection so the free tier can't be
	// an unlimited proxy forever, not a way to push upgrades: a repeat offender
	// gets the same ceiling as everyone else.
	CeilingMultiple = 4

	// Months in a row over the cap before an account loses its grace window. Two
	// means a full month of warning emails went unanswered.
	RepeatOffenderMonths = 2

	// Default cap for a project with no override of its own. Set per deployment
	// with MONTHLY_REQUEST_LIMIT. The free plan, the dashboard meter and the
	// warning emails all have to agree on this number.
	DefaultFreeMonthlyLimit = 5000
)

// State is what to do with one request.
type State struct {
	// Send a 429 and don't forward. Only happens at the abuse ceiling.
	Refuse bool
	// Save the metadata row. Goes false past the grace window, where the call
	// still goes through but we don't keep a receipt.
	Record bool
}

// For decides what to do with one request. A limit of 0 or less means uncapped,
// which is every paid project since 0 is what the billing webhook writes, and
// none of the ladder applies.
func For(limit, used, capMonths int) State {
	if limit <= 0 {
		return State{Record: true}
	}
	if used >= limit*CeilingMultiple {
		return State{Refuse: true}
	}
	return State{Record: used < RecordingStopsAt(limit, capMonths)}
}

// RecordingStopsAt is the usage where metadata stops being written. Exported so
// the dashboard and the warning emails can quote it. 0 means uncapped.
func RecordingStopsAt(limit, capMonths int) int {
	if limit <= 0 {
		return 0
	}
	if capMonths >= RepeatOffenderMonths {
		return limit
	}
	return limit * GraceMultiple
}
