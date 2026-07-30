// Package quota is the monthly request policy: what happens to a request once a
// project has used more than its plan allows.
//
// It lives in the proxy module because that is where the policy is enforced, and
// because the proxy is the public mirror, so the limits are readable. The
// dependency only runs one way: the private services import this package, never
// the reverse.
package quota

// Past the limit the cap withdraws the product rather than the traffic: nothing
// is recorded, so the dashboard goes dark and alerts stop, but calls keep
// flowing. Refusing at the limit instead would break the caller's production app
// to make a pricing point.
const (
	// How far past the limit a first offender is still recorded. Alerts keep
	// evaluating through the start of a spike, which is when a runaway agent is
	// most likely.
	GraceMultiple = 2

	// Where forwarding stops. Abuse protection, so a free tier cannot be an
	// unlimited proxy forever. Never a monetisation lever: a repeat offender gets
	// the same ceiling as everyone else.
	CeilingMultiple = 4

	// Consecutive finished months over cap that cost an account its grace window.
	// Two means a whole month of notices went unanswered.
	RepeatOffenderMonths = 2

	// The cap for a project storing no override. Overridable per deployment via
	// MONTHLY_REQUEST_LIMIT, but the free plan, the dashboard meter and the
	// notice emails all have to agree on this number.
	DefaultFreeMonthlyLimit = 10000
)

// State is the decision for one request.
type State struct {
	// Answer 429 without forwarding. Only true at the abuse ceiling.
	Refuse bool
	// Write the metadata row. False past the grace window, where the call still
	// happens but no receipt is kept.
	Record bool
}

// For decides what to do with one request. limit <= 0 means uncapped, which is
// every paid project since 0 is what the billing webhook writes, and no part of
// the ladder applies.
func For(limit, used, capMonths int) State {
	if limit <= 0 {
		return State{Record: true}
	}
	if used >= limit*CeilingMultiple {
		return State{Refuse: true}
	}
	return State{Record: used < RecordingStopsAt(limit, capMonths)}
}

// RecordingStopsAt is the usage at which metadata stops being written. Exported
// because the dashboard and the notice emails have to quote it without deciding
// about a request. Returns 0 for an uncapped project, which has no threshold.
func RecordingStopsAt(limit, capMonths int) int {
	if limit <= 0 {
		return 0
	}
	if capMonths >= RepeatOffenderMonths {
		return limit
	}
	return limit * GraceMultiple
}
