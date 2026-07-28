// Package quota is the monthly request policy: what happens to a request once
// a project has used more than its plan allows.
//
// It lives in the proxy module rather than the private one for two reasons.
// The proxy is where the policy is actually enforced, so this is its home. And
// the proxy is mirrored to a public repo, so anyone reading it can see exactly
// what the limits do - which is the whole point of the core being public. A
// package in the private module could not be imported here without dragging
// that module into the mirror, so the dependency runs this way round: the
// private services import the public policy, never the reverse.
//
// Before this package the numbers were copied into three modules and kept in
// step by a test that scraped the proxy's source for them. This replaces that
// with the compiler.
package quota

// The ladder.
//
// The cap used to be a wall: at the limit, refuse the request and stop
// forwarding. That breaks the caller's production app to make a pricing point,
// and someone whose site goes down deletes the tool rather than upgrading it.
// So the cap withdraws the product instead: past the grace window nothing is
// recorded, which takes the dashboard dark and stops alerts, while the calls
// themselves keep flowing.
const (
	// GraceMultiple is how far past the limit a first offender is still
	// recorded. Alerts keep evaluating through the start of a spike, which is
	// exactly when a runaway agent is most likely and the worst possible moment
	// to go blind.
	GraceMultiple = 2

	// CeilingMultiple is where forwarding finally stops. This is abuse
	// protection so a free tier cannot be an unlimited proxy forever, and it is
	// never a monetisation lever - a repeat offender is forwarded to exactly
	// the same ceiling as everyone else.
	CeilingMultiple = 4

	// RepeatOffenderMonths is how many consecutive finished months over the cap
	// cost an account its grace window. Two means a whole month of notices has
	// already gone unanswered.
	RepeatOffenderMonths = 2

	// DefaultFreeMonthlyLimit is the cap a project gets when it stores no
	// override. Overridable per deployment via MONTHLY_REQUEST_LIMIT, but this
	// is the number the free plan, the dashboard meter and the notice emails
	// all have to agree on.
	DefaultFreeMonthlyLimit = 10000
)

// State is the decision for one request.
type State struct {
	// Refuse means answer 429 without forwarding. Only ever true at the abuse
	// ceiling.
	Refuse bool
	// Record means write the metadata row. False past the grace window: the
	// call still happens, we simply stop keeping the receipt.
	Record bool
}

// For decides what to do with one request.
//
// limit <= 0 means uncapped - which is every paid project, since 0 is what the
// billing webhook writes - and no part of the ladder applies.
func For(limit, used, capMonths int) State {
	if limit <= 0 {
		return State{Record: true}
	}
	if used >= limit*CeilingMultiple {
		return State{Refuse: true}
	}
	return State{Record: used < RecordingStopsAt(limit, capMonths)}
}

// RecordingStopsAt is the usage at which metadata stops being written.
//
// Exported separately because the dashboard and the notice emails both have to
// quote this number, and neither of them is deciding about a request. Returns 0
// for an uncapped project, which has no threshold to report.
func RecordingStopsAt(limit, capMonths int) int {
	if limit <= 0 {
		return 0
	}
	if capMonths >= RepeatOffenderMonths {
		return limit
	}
	return limit * GraceMultiple
}
