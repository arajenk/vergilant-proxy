package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These run against a real Postgres, because a mock would accept an INSERT the
// database rejects, and saveRequest's column list and value list can disagree
// without anything failing loudly.
//
// Point TEST_DATABASE_URL at a database with schema.sql applied. Tests fail
// rather than skip when none is reachable; LLM_MONITOR_SKIP_DB_TESTS=1 opts out.
// The schema is applied for this harness, not by it: this module cannot reach
// the migration runner the hosted service uses.

const defaultTestDatabaseURL = "postgres://127.0.0.1:5544/llm_monitor_test"

var testDBUnavailable string

func TestMain(m *testing.M) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = defaultTestDatabaseURL
	}

	ctx := context.Background()
	p, err := pgxpool.New(ctx, url)
	if err == nil {
		err = p.Ping(ctx)
	}
	switch {
	case err != nil:
		testDBUnavailable = fmt.Sprintf("could not reach %s: %v", url, err)
	default:
		// Probing the newest column catches a stale database as well as a
		// missing one.
		var exists bool
		if qerr := p.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'users' AND column_name = 'consecutive_cap_months')`,
		).Scan(&exists); qerr != nil || !exists {
			testDBUnavailable = "schema is missing or out of date; re-apply it"
		} else {
			pool = p
		}
	}

	os.Exit(m.Run())
}

func requireDB(t *testing.T) {
	t.Helper()
	if testDBUnavailable == "" {
		return
	}
	if os.Getenv("LLM_MONITOR_SKIP_DB_TESTS") != "" {
		t.Skip("LLM_MONITOR_SKIP_DB_TESTS set:", testDBUnavailable)
	}
	t.Fatal("no test database: ", testDBUnavailable,
		" (point TEST_DATABASE_URL at a database with the schema applied)")
}

// Clears what these tests write, plus the caches that outlive a request, so one
// test's warm key cache cannot make the next one pass.
func resetDB(t *testing.T) {
	t.Helper()
	requireDB(t)
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE requests, projects, users RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	keys = newKeyCache()
	rl = newLimiter(refillPerSecond, burstSize)
}

// A project with no owner, which reads a cap history of zero and gets the full
// grace window. Tests needing a history call seedOwner.
func seedProject(t *testing.T, key string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO projects (key, name) VALUES ($1, 'p')`, key); err != nil {
		t.Fatal(err)
	}
}

// Inserts an owner carrying a cap history and returns its id. Two inserts
// because this module runs against two different users tables: its own
// schema.sql has only id and the cap counter, while the hosted service's table
// has NOT NULL columns the minimal insert cannot satisfy.
func seedOwner(t *testing.T, key string, capMonths int) int64 {
	t.Helper()
	ctx := context.Background()
	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO users (consecutive_cap_months) VALUES ($1) RETURNING id`, capMonths).Scan(&id)
	if err != nil {
		err = pool.QueryRow(ctx,
			`INSERT INTO users (email, password_hash, consecutive_cap_months)
			 VALUES ($1, 'x', $2) RETURNING id`,
			fmt.Sprintf("proxy-%s@example.com", key), capMonths).Scan(&id)
	}
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Points a provider at a local server for one test, so nothing calls a real API.
func stubUpstream(t *testing.T, provider string, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	original := providers[provider]
	providers[provider] = srv.URL
	t.Cleanup(func() {
		providers[provider] = original
		srv.Close()
	})
	return srv
}

const anthropicReply = `{"model":"claude-sonnet-5","content":[{"type":"text","text":"pong"}],` +
	`"usage":{"input_tokens":16,"output_tokens":4}}`

func post(t *testing.T, path, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("X-Monitor-Key", key)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// Every column the INSERT names has to receive its value. A mismatch between the
// column list and the value list is the failure this file exists to catch.
func TestSaveRequestPersistsEveryColumn(t *testing.T) {
	resetDB(t)
	seedProject(t, "lm_save")

	validate, upstream, firstToken := int64(7), int64(910), int64(120)
	msg := "upstream is down"
	rec := requestRecord{
		ProjectKey:       "lm_save",
		Timestamp:        time.Now().UTC().Truncate(time.Second),
		Provider:         "anthropic",
		Model:            "claude-sonnet-5",
		Status:           503,
		LatencyMs:        917,
		ValidateMs:       &validate,
		UpstreamMs:       &upstream,
		FirstTokenMs:     &firstToken,
		InputTokens:      16,
		OutputTokens:     4,
		EstimatedCostUSD: 0.000072,
		Error:            &msg,
	}
	if err := saveRequest(context.Background(), pool, rec); err != nil {
		t.Fatal(err)
	}

	var got requestRecord
	if err := pool.QueryRow(context.Background(), `
		SELECT project_key, provider, model, status, latency_ms, validate_ms,
		       upstream_ms, first_token_ms, input_tokens, output_tokens,
		       estimated_cost_usd::float8, error
		FROM requests WHERE project_key = 'lm_save'`,
	).Scan(&got.ProjectKey, &got.Provider, &got.Model, &got.Status, &got.LatencyMs,
		&got.ValidateMs, &got.UpstreamMs, &got.FirstTokenMs, &got.InputTokens,
		&got.OutputTokens, &got.EstimatedCostUSD, &got.Error); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name      string
		got, want any
	}{
		{"project_key", got.ProjectKey, rec.ProjectKey},
		{"provider", got.Provider, rec.Provider},
		{"model", got.Model, rec.Model},
		{"status", got.Status, rec.Status},
		{"latency_ms", got.LatencyMs, rec.LatencyMs},
		{"validate_ms", *got.ValidateMs, validate},
		{"upstream_ms", *got.UpstreamMs, upstream},
		{"first_token_ms", *got.FirstTokenMs, firstToken},
		{"input_tokens", got.InputTokens, rec.InputTokens},
		{"output_tokens", got.OutputTokens, rec.OutputTokens},
		{"estimated_cost_usd", got.EstimatedCostUSD, rec.EstimatedCostUSD},
		{"error", *got.Error, msg},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// A request that reached neither stage has no timing to report, so the columns
// stay null rather than claiming the proxy added nothing.
func TestFailuresBeforeUpstreamLeaveTimingsNull(t *testing.T) {
	resetDB(t)
	seedProject(t, "lm_fail")

	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages", strings.NewReader("{}"))
	saveFailure(req, "lm_fail", "anthropic", time.Now(), "", http.StatusBadRequest, "failed to read request body", true)

	var validate, upstream *int64
	if err := pool.QueryRow(context.Background(),
		`SELECT validate_ms, upstream_ms FROM requests WHERE project_key = 'lm_fail'`,
	).Scan(&validate, &upstream); err != nil {
		t.Fatal(err)
	}
	if validate != nil || upstream != nil {
		t.Errorf("timings = %v/%v, want null on a request that never reached upstream", validate, upstream)
	}
}

func TestUnknownKeyIsRejectedAndNotRecorded(t *testing.T) {
	resetDB(t)

	rec := post(t, "/anthropic/v1/messages", "lm_nope", `{"model":"claude-sonnet-5"}`)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
	// An unknown key belongs to no project, and recording it would let a stranger
	// fill someone's table.
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM requests`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("recorded %d rows for an unknown key, want 0", count)
	}
}

func TestUnknownProviderIs404(t *testing.T) {
	resetDB(t)
	seedProject(t, "lm_prov")

	rec := post(t, "/gemini/v1/messages", "lm_prov", "{}")

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// No resetDB or seedProject: robots.txt is served before any key or database
// lookup, since a crawler never carries a key.
func TestRobotsTxtDisallowsEverything(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", robotsHandler)
	mux.HandleFunc("/", handler)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/robots.txt", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/plain; charset=utf-8", got)
	}
	if got := rec.Body.String(); got != "User-agent: *\nDisallow: /\n" {
		t.Errorf("body = %q, want a total disallow", got)
	}
}

func TestProxiesUpstreamAndRecordsMetadata(t *testing.T) {
	resetDB(t)
	seedProject(t, "lm_ok")

	var gotPath, gotAuth, gotMonitorKey string
	stubUpstream(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("x-api-key")
		gotMonitorKey = r.Header.Get("X-Monitor-Key")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(anthropicReply))
	})

	req := httptest.NewRequest(http.MethodPost, "/anthropic/v1/messages",
		strings.NewReader(`{"model":"claude-sonnet-5","messages":[{"role":"user","content":"ping"}]}`))
	req.Header.Set("X-Monitor-Key", "lm_ok")
	req.Header.Set("x-api-key", "sk-ant-customer-key")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rec.Code, rec.Body.String())
	}
	// The provider segment is stripped, and the caller's auth header is untouched.
	if gotPath != "/v1/messages" {
		t.Errorf("forwarded path = %q, want /v1/messages", gotPath)
	}
	if gotAuth != "sk-ant-customer-key" {
		t.Errorf("upstream auth = %q, want the caller's key forwarded unchanged", gotAuth)
	}
	if gotMonitorKey != "" {
		t.Errorf("X-Monitor-Key = %q, want it stripped rather than leaked upstream", gotMonitorKey)
	}
	// Byte-identical: the caller gets what the provider sent.
	if body, _ := io.ReadAll(rec.Body); string(body) != anthropicReply {
		t.Errorf("response body was rewritten:\n got %s\nwant %s", body, anthropicReply)
	}

	var model string
	var status, in, out int
	var cost float64
	var validate, upstream *int64
	if err := pool.QueryRow(context.Background(), `
		SELECT model, status, input_tokens, output_tokens,
		       estimated_cost_usd::float8, validate_ms, upstream_ms
		FROM requests WHERE project_key = 'lm_ok'`,
	).Scan(&model, &status, &in, &out, &cost, &validate, &upstream); err != nil {
		t.Fatal("no row recorded for a successful proxied request: ", err)
	}
	if model != "claude-sonnet-5" || status != 200 || in != 16 || out != 4 {
		t.Errorf("recorded %s %d in=%d out=%d, want claude-sonnet-5 200 in=16 out=4",
			model, status, in, out)
	}
	if cost <= 0 {
		t.Errorf("cost = %v, want a real figure from the price map", cost)
	}
	if validate == nil || upstream == nil {
		t.Error("validate_ms/upstream_ms are null on a request that reached upstream")
	}
}

// The service must never store request or response content. There is no column
// for it; this pins that rather than trusting it.
func TestNothingFromTheBodyIsEverStored(t *testing.T) {
	resetDB(t)
	seedProject(t, "lm_priv")

	const secret = "SENSITIVE-PROMPT-TEXT-9f3a"
	stubUpstream(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Write([]byte(`{"model":"claude-sonnet-5","content":[{"type":"text","text":"` + secret + `-reply"}],` +
			`"usage":{"input_tokens":5,"output_tokens":5}}`))
	})

	post(t, "/anthropic/v1/messages", "lm_priv",
		fmt.Sprintf(`{"model":"claude-sonnet-5","messages":[{"role":"user","content":%q}]}`, secret))

	// Every column of every row as text, so a leaked prompt or reply shows up.
	var dump string
	if err := pool.QueryRow(context.Background(),
		`SELECT coalesce(string_agg(r::text, ' '), '') FROM requests r`).Scan(&dump); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(dump, secret) {
		t.Errorf("request content reached the database: %s", dump)
	}
	if !strings.Contains(dump, "claude-sonnet-5") {
		t.Error("nothing was recorded at all, so this test proved nothing")
	}
}

func TestKeyCacheKeepsTheLookupOffRepeatRequests(t *testing.T) {
	resetDB(t)
	seedProject(t, "lm_cache")
	stubUpstream(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(anthropicReply))
	})

	body := `{"model":"claude-sonnet-5"}`
	post(t, "/anthropic/v1/messages", "lm_cache", body)
	if _, _, _, ok := keys.get("lm_cache"); !ok {
		t.Fatal("a successful request did not populate the key cache")
	}

	// Deleting mid-flight proves the second request came from cache: otherwise the
	// lookup fails and it 401s.
	if _, err := pool.Exec(context.Background(),
		`DELETE FROM projects WHERE key = 'lm_cache'`); err != nil {
		t.Fatal(err)
	}
	if rec := post(t, "/anthropic/v1/messages", "lm_cache", body); rec.Code != http.StatusOK {
		t.Errorf("second request status = %d, want 200 from the cached key", rec.Code)
	}
}

func TestSplitProviderPath(t *testing.T) {
	cases := []struct {
		path, provider, rest string
		ok                   bool
	}{
		{"/anthropic/v1/messages", "anthropic", "/v1/messages", true},
		{"/openai/v1/chat/completions", "openai", "/v1/chat/completions", true},
		{"/gemini/v1/messages", "", "", false},
		{"/anthropic", "", "", false},
		{"/", "", "", false},
	}
	for _, c := range cases {
		provider, rest, ok := splitProviderPath(c.path)
		if provider != c.provider || rest != c.rest || ok != c.ok {
			t.Errorf("splitProviderPath(%q) = %q, %q, %v; want %q, %q, %v",
				c.path, provider, rest, ok, c.provider, c.rest, c.ok)
		}
	}
}

func TestEstimatedCostUsesThePriceMap(t *testing.T) {
	// claude-sonnet-5 is $2/M in, $10/M out (db.go).
	got := estimatedCost("claude-sonnet-5", 1_000_000, 1_000_000)
	if got != 12 {
		t.Errorf("estimatedCost = %v, want 12", got)
	}
	// An unpriced model reports zero rather than guessing.
	if got := estimatedCost("some-model-nobody-priced", 1_000_000, 1_000_000); got != 0 {
		t.Errorf("estimatedCost for an unknown model = %v, want 0", got)
	}
}

// $0 for an unpriced model is deliberate, but it must not be silent: cost_spike
// then compares 0 against 0, never fires, and looks identical to everything
// working. Logged once per model, since one line per call is how a warning
// becomes invisible.
func TestUnpricedModelIsLoggedOnce(t *testing.T) {
	var buf bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(restore)

	// Unique per run, so the once-only latch cannot leak in from another test.
	const model = "claude-opus-5-deliberately-absent-from-the-map"
	estimatedCost(model, 1_000_000, 1_000_000)
	estimatedCost(model, 500_000, 500_000)

	if n := strings.Count(buf.String(), model); n != 1 {
		t.Errorf("unpriced model named %d times in the log, want exactly 1:\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), "unpriced") {
		t.Errorf("log never says the model is unpriced, so the reason is lost:\n%s", buf.String())
	}
}

// The map is hand-maintained, and a miss is a $0 cost with a cost_spike rule
// that cannot fire, so the likeliest models have to be in it. Verified against
// platform.claude.com/docs/en/about-claude/pricing on 2026-07-28. On failure,
// check the live page rather than guessing.
func TestCurrentClaudeModelsArePriced(t *testing.T) {
	for _, want := range []struct {
		model         string
		input, output float64
	}{
		{"claude-opus-5", 5, 25},
		{"claude-fable-5", 10, 50},
		{"claude-haiku-4-5", 1, 5},
	} {
		got, ok := priceMap[want.model]
		if !ok {
			t.Errorf("%s is missing from priceMap, so its cost records as $0", want.model)
			continue
		}
		if got.InputPerMillion != want.input || got.OutputPerMillion != want.output {
			t.Errorf("%s priced at $%v/$%v per million, want $%v/$%v",
				want.model, got.InputPerMillion, got.OutputPerMillion, want.input, want.output)
		}
	}
}

// Logging on every lookup rather than only on a miss would drown the warning
// above in ordinary traffic.
func TestPricedModelLogsNothing(t *testing.T) {
	var buf bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(restore)

	estimatedCost("claude-sonnet-5", 1_000_000, 1_000_000)

	if buf.Len() != 0 {
		t.Errorf("a priced model logged %q, want silence", buf.String())
	}
}

func TestJSONMarshalOfLogEntryCarriesNoBody(t *testing.T) {
	// logEntry is the only per-request thing reaching stdout, so adding a body
	// field to it fails here.
	b, err := json.Marshal(logEntry{Model: "claude-sonnet-5"})
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"prompt", "messages", "content", "body", "response"} {
		if strings.Contains(strings.ToLower(string(b)), banned) {
			t.Errorf("logEntry carries a %q field; this log is metadata only", banned)
		}
	}
}

// The startup check only helps while requiredColumns covers what saveRequest
// writes, and it silently stopped covering it once validate_ms and upstream_ms
// were added. Read the INSERT and compare, so the list cannot drift again.
func TestRequiredColumnsCoverTheInsert(t *testing.T) {
	src, err := os.ReadFile("db.go")
	if err != nil {
		t.Fatal(err)
	}
	open := strings.Index(string(src), "INSERT INTO requests")
	if open == -1 {
		t.Fatal("could not find the requests INSERT in db.go")
	}
	rest := string(src)[open:]
	start := strings.Index(rest, "(")
	end := strings.Index(rest, ")")
	if start == -1 || end == -1 || end < start {
		t.Fatal("could not read the INSERT's column list")
	}

	for _, raw := range strings.Split(rest[start+1:end], ",") {
		column := strings.TrimSpace(strings.Trim(strings.TrimSpace(raw), "\n\t"))
		if column == "" {
			continue
		}
		if !slices.Contains(requiredColumns["requests"], column) {
			t.Errorf("saveRequest writes %q but requiredColumns does not list it; a "+
				"database without it would pass checkSchema and then fail every insert", column)
		}
	}
}

// --- serving stale when the database is unreachable -----------------------
//
// The proxy sits in the caller's request path, so an unreachable database must
// not 500 a key that has been validated before. The metadata is lost either way;
// these cover whether the caller's traffic dies with it.

// Swaps the package pool for one pointing at a closed port, so every query fails
// the way an outage makes them fail.
func breakPool(t *testing.T) {
	t.Helper()
	original := pool
	dead, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/nonexistent")
	if err != nil {
		t.Fatalf("building dead pool: %v", err)
	}
	pool = dead
	t.Cleanup(func() {
		pool = original
		dead.Close()
	})
}

// Plants an entry that has expired but is still inside keyCacheMaxStale, the
// state a key is in partway through an outage. Written directly, since letting a
// real TTL elapse would mean a 45-second test.
func staleEntry(t *testing.T, key string, age time.Duration) {
	t.Helper()
	keys.mu.Lock()
	defer keys.mu.Unlock()
	keys.entries[key] = cachedStatus{monthCount: 5, limit: 10000, expires: time.Now().Add(-age)}
}

func TestADatabaseOutageDoesNotBreakAKnownCustomersTraffic(t *testing.T) {
	resetDB(t)
	seedProject(t, "stale-known")
	stubUpstream(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, anthropicReply)
	})
	// Expired a minute ago, well inside the 15 minute stale window.
	staleEntry(t, "stale-known", time.Minute)
	breakPool(t)

	rec := post(t, "/anthropic/v1/messages", "stale-known", `{"model":"claude-sonnet-5"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: a database outage must not fail a validated key's call; body %s",
			rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "pong") {
		t.Error("expected the upstream reply to reach the caller")
	}
}

// The line between serving stale and being an open relay. An unknown key has no
// entry to fall back on, so a database outage must not get it forwarded.
func TestADatabaseOutageStillRefusesAnUnknownKey(t *testing.T) {
	resetDB(t)
	forwarded := false
	stubUpstream(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		forwarded = true
		io.WriteString(w, anthropicReply)
	})
	breakPool(t)

	rec := post(t, "/anthropic/v1/messages", "never-seen-before", `{"model":"claude-sonnet-5"}`)

	if rec.Code == http.StatusOK {
		t.Error("an unvalidated key was served during an outage; that is an open relay")
	}
	if forwarded {
		t.Error("an unvalidated key was forwarded upstream during an outage")
	}
}

// Bounded: past keyCacheMaxStale the revocation lag and quota overshoot would
// grow without limit, so it fails closed again.
func TestStaleServingStopsAtTheMaxStaleBound(t *testing.T) {
	resetDB(t)
	seedProject(t, "stale-expired")
	forwarded := false
	stubUpstream(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		forwarded = true
		io.WriteString(w, anthropicReply)
	})
	staleEntry(t, "stale-expired", keyCacheMaxStale+time.Minute)
	breakPool(t)

	rec := post(t, "/anthropic/v1/messages", "stale-expired", `{"model":"claude-sonnet-5"}`)

	if rec.Code == http.StatusOK {
		t.Error("served an entry older than keyCacheMaxStale")
	}
	if forwarded {
		t.Error("forwarded upstream on an entry past the stale bound")
	}
}

// The fallback is reachable only through the error branch, so normal traffic
// still gets a real lookup.
func TestStaleFallbackDoesNotChangeTheHealthyPath(t *testing.T) {
	resetDB(t)
	seedProject(t, "stale-healthy")
	stubUpstream(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, anthropicReply)
	})
	// Expired, but the database is up, so this refreshes from the real table
	// instead of being served stale.
	staleEntry(t, "stale-healthy", time.Minute)

	rec := post(t, "/anthropic/v1/messages", "stale-healthy", `{"model":"claude-sonnet-5"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rec.Code, rec.Body.String())
	}
	monthCount, _, _, ok := keys.get("stale-healthy")
	if !ok {
		t.Fatal("expected a fresh cache entry after a successful lookup")
	}
	if monthCount == 5 {
		t.Error("the planted stale count survived; the healthy path did not re-read the database")
	}
}

func TestGetStaleRespectsItsBounds(t *testing.T) {
	c := newKeyCache()
	c.put("k", 1, 10, 0)

	// Fresh entries are returned by get, and by getStale too.
	if _, _, _, ok := c.getStale("k"); !ok {
		t.Error("a fresh entry should also be servable as stale")
	}

	c.mu.Lock()
	c.entries["k"] = cachedStatus{monthCount: 1, limit: 10, expires: time.Now().Add(-time.Minute)}
	c.mu.Unlock()
	if _, _, _, ok := c.get("k"); ok {
		t.Error("get must still refuse an expired entry")
	}
	if _, _, _, ok := c.getStale("k"); !ok {
		t.Error("getStale should serve an entry one minute past expiry")
	}

	c.mu.Lock()
	c.entries["k"] = cachedStatus{expires: time.Now().Add(-keyCacheMaxStale - time.Minute)}
	c.mu.Unlock()
	if _, _, _, ok := c.getStale("k"); ok {
		t.Error("getStale should refuse an entry past keyCacheMaxStale")
	}

	if _, _, _, ok := c.getStale("never-cached"); ok {
		t.Error("getStale invented an entry for a key it has never seen")
	}
}

// --- the quota ladder ------------------------------------------------------
//
//	  under 1x limit  forward + record        (normal)
//	  1x .. 2x        forward + record        (grace: alerts stay live)
//	  2x .. 4x        forward, stop recording (product withdrawn, app fine)
//	  4x and over     429                     (abuse ceiling only)
//
// A repeat offender, two consecutive months over cap, skips the grace row and
// loses recording at 1x. Forwarding is unchanged for them.

// Owner and cap set explicitly, so a test can work with a limit of 2 instead of
// seeding 10,000 rows.
func seedProjectWithLimit(t *testing.T, key string, limit int, consecutiveCapMonths int) {
	t.Helper()
	userID := seedOwner(t, key, consecutiveCapMonths)
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO projects (key, name, user_id, monthly_request_limit) VALUES ($1, 'p', $2, $3)`,
		key, userID, limit); err != nil {
		t.Fatal(err)
	}
}

// Inserts n recorded requests in the current month, which is what projectStatus
// counts.
func fillUsage(t *testing.T, key string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := pool.Exec(context.Background(), `
			INSERT INTO requests
			  (project_key, timestamp, provider, model, status, latency_ms,
			   input_tokens, output_tokens, estimated_cost_usd)
			VALUES ($1, now(), 'anthropic', 'claude-sonnet-5', 200, 1, 0, 0, 0)`, key); err != nil {
			t.Fatal(err)
		}
	}
}

func countRows(t *testing.T, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM requests WHERE project_key = $1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Stubs anthropic and reports whether it was reached.
func okUpstream(t *testing.T) *bool {
	t.Helper()
	reached := false
	stubUpstream(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, anthropicReply)
	})
	return &reached
}

// Inside the grace window the product still works, since this is when a runaway
// agent is most likely and alerts have to keep evaluating.
func TestInsideTheGraceWindowItStillRecords(t *testing.T) {
	resetDB(t)
	seedProjectWithLimit(t, "q-grace", 2, 0)
	fillUsage(t, "q-grace", 3) // over 1x limit, under 2x
	reached := okUpstream(t)

	rec := post(t, "/anthropic/v1/messages", "q-grace", `{"model":"claude-sonnet-5"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; over cap must not break the caller", rec.Code)
	}
	if !*reached {
		t.Error("request was not forwarded upstream")
	}
	if got := countRows(t, "q-grace"); got != 4 {
		t.Errorf("rows = %d, want 4: inside the grace window the request should still be recorded", got)
	}
}

// Past the grace multiple there is no row, so the dashboard goes dark and alerts
// stop, but the call still goes through.
func TestPastTheGraceWindowItForwardsWithoutRecording(t *testing.T) {
	resetDB(t)
	seedProjectWithLimit(t, "q-nolog", 2, 0)
	fillUsage(t, "q-nolog", 4) // at 2x limit
	reached := okUpstream(t)

	rec := post(t, "/anthropic/v1/messages", "q-nolog", `{"model":"claude-sonnet-5"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; the caller's app must keep working", rec.Code)
	}
	if !*reached {
		t.Error("request was not forwarded upstream")
	}
	if got := countRows(t, "q-nolog"); got != 4 {
		t.Errorf("rows = %d, want 4: past the grace window nothing new should be recorded", got)
	}
}

// The only wall left, and it is abuse protection rather than pricing.
func TestAtTheAbuseCeilingItRefuses(t *testing.T) {
	resetDB(t)
	seedProjectWithLimit(t, "q-ceiling", 2, 0)
	fillUsage(t, "q-ceiling", 8) // 4x limit (quota.CeilingMultiple)
	reached := okUpstream(t)

	rec := post(t, "/anthropic/v1/messages", "q-ceiling", `{"model":"claude-sonnet-5"}`)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 at the abuse ceiling", rec.Code)
	}
	if *reached {
		t.Error("forwarded upstream past the abuse ceiling")
	}
}

// A second consecutive month over cap stops recording at the cap itself.
// Forwarding is unchanged.
func TestARepeatOffenderLosesTheGraceWindow(t *testing.T) {
	resetDB(t)
	seedProjectWithLimit(t, "q-repeat", 2, 2)
	fillUsage(t, "q-repeat", 2) // exactly at 1x limit
	reached := okUpstream(t)

	rec := post(t, "/anthropic/v1/messages", "q-repeat", `{"model":"claude-sonnet-5"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; a repeat offender still must not be broken", rec.Code)
	}
	if !*reached {
		t.Error("request was not forwarded upstream")
	}
	if got := countRows(t, "q-repeat"); got != 2 {
		t.Errorf("rows = %d, want 2: a repeat offender loses recording at the cap", got)
	}
}

// A first offender at exactly 1x still gets the grace window.
func TestAFirstOffenderKeepsRecordingAtTheCap(t *testing.T) {
	resetDB(t)
	seedProjectWithLimit(t, "q-first", 2, 0)
	fillUsage(t, "q-first", 2)
	okUpstream(t)

	post(t, "/anthropic/v1/messages", "q-first", `{"model":"claude-sonnet-5"}`)

	if got := countRows(t, "q-first"); got != 3 {
		t.Errorf("rows = %d, want 3: a first offender still records inside the grace window", got)
	}
}

// Pro is uncapped: limit 0 means no ladder at all, at any volume.
func TestAnUncappedProjectIsNeverThrottledOrSilenced(t *testing.T) {
	resetDB(t)
	seedProjectWithLimit(t, "q-pro", 0, 5) // uncapped, and a history that would matter if capped
	fillUsage(t, "q-pro", 50)
	reached := okUpstream(t)

	rec := post(t, "/anthropic/v1/messages", "q-pro", `{"model":"claude-sonnet-5"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for an uncapped project", rec.Code)
	}
	if !*reached {
		t.Error("an uncapped project was not forwarded")
	}
	if got := countRows(t, "q-pro"); got != 51 {
		t.Errorf("rows = %d, want 51: an uncapped project is always recorded", got)
	}
}

// schema.sql and requiredColumns are two hand-written lists of the same thing,
// so they drift. They already did once: projectStatus started joining users while
// schema.sql had no users table, which broke only the people who cloned this
// module and followed its own setup instructions.
func TestTheShippedSchemaSatisfiesTheStartupCheck(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("reading schema.sql: %v", err)
	}
	schema := string(src)

	for table, columns := range requiredColumns {
		if !strings.Contains(schema, "CREATE TABLE "+table+" (") {
			t.Errorf("schema.sql defines no %q table, but the proxy refuses to start without it", table)
			continue
		}
		for _, col := range columns {
			if !strings.Contains(schema, col) {
				t.Errorf("schema.sql is missing %s.%s, which checkSchema requires", table, col)
			}
		}
	}
}
