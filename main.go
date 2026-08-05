package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/arajenk/vergilant-proxy/quota"
	"github.com/joho/godotenv"
)

var pool *pgxpool.Pool

// Burst guardrail. The monthly cap is enforced in Postgres; see ratelimit.go.
var rl = newLimiter(refillPerSecond, burstSize)

var keys = newKeyCache()

// Generous by default: long context and base64 images. Override with
// MAX_REQUEST_BYTES.
var maxRequestBytes int64 = 25 << 20 // 25 MiB

// Timeouts belong on the Transport, not the Client. Client.Timeout would cap
// the whole request including the body read, truncating long-lived SSE streams.
var httpClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		MaxIdleConns:          100,
		// Default per-host is 2, which makes concurrent traffic re-handshake
		// constantly.
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		// Setting DialContext disables HTTP/2 unless re-enabled here.
		ForceAttemptHTTP2: true,
	},
}

type requestBody struct {
	Model string `json:"model"`
}

// Carries both providers' names for the same two numbers. Only one pair is
// ever populated.
type responseBody struct {
	Usage struct {
		InputTokens      int `json:"input_tokens"`
		OutputTokens     int `json:"output_tokens"`
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

func (b responseBody) tokens() (input, output int) {
	if b.Usage.InputTokens != 0 || b.Usage.OutputTokens != 0 {
		return b.Usage.InputTokens, b.Usage.OutputTokens
	}
	return b.Usage.PromptTokens, b.Usage.CompletionTokens
}

type logEntry struct {
	Timestamp    string `json:"timestamp"`
	Method       string `json:"method"`
	Path         string `json:"path"`
	Model        string `json:"model,omitempty"`
	Status       int    `json:"status"`
	LatencyMs    int64  `json:"latency_ms"`
	ValidateMs   int64  `json:"validate_ms"`
	UpstreamMs   int64  `json:"upstream_ms"`
	Cached       bool   `json:"cached"`
	InputTokens  int    `json:"input_tokens,omitempty"`
	OutputTokens int    `json:"output_tokens,omitempty"`
	Streamed     bool   `json:"streamed,omitempty"`
	FirstTokenMs int64  `json:"first_token_ms,omitempty"`
}

// The only per-request log. Metadata only, never bodies.
func logRequest(e logEntry) {
	slog.Info("request",
		"method", e.Method,
		"path", e.Path,
		"model", e.Model,
		"status", e.Status,
		"latency_ms", e.LatencyMs,
		"validate_ms", e.ValidateMs,
		"upstream_ms", e.UpstreamMs,
		"cached", e.Cached,
		"input_tokens", e.InputTokens,
		"output_tokens", e.OutputTokens,
		"streamed", e.Streamed,
		"first_token_ms", e.FirstTokenMs,
	)
}

func recordRequest(ctx context.Context, rec requestRecord, record bool) {
	if !record {
		return
	}
	// Detached from the request context on purpose. net/http cancels that context
	// when the caller's connection closes, and this write happens after the
	// response has already gone out - so a client that hangs up the moment it has
	// its answer would take the metadata row with it. The streaming path is worse:
	// a dropped connection is exactly what ends its read loop, so the disconnect
	// and the lost row arrive together, and an agent killed mid-stream is the case
	// most worth recording. Bounded so a stuck write cannot outlive shutdown.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := saveRequest(ctx, pool, rec); err != nil {
		slog.Error("failed to save request record", "err", err)
	}
}

// Crawlers reach this host via the vergilant.dev domain property. Nothing here
// is indexable: every path either 404s or needs a customer's API key.
func robotsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "User-agent: *\nDisallow: /\n")
}

func handler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	projectKey := r.Header.Get("X-Monitor-Key")

	// Before the key check, so a bad path 404s without a DB round-trip.
	provider, forwardPath, ok := splitProviderPath(r.URL.Path)
	if !ok {
		http.Error(w, "unknown provider; use /anthropic/... or /openai/...", http.StatusNotFound)
		return
	}

	// A hit skips the cross-region DB round-trip; a miss falls through, so a new
	// key works immediately. Rejected requests are not recorded: an unknown key
	// belongs to no project.
	validateStart := time.Now()
	monthCount, limit, capMonths, cached := keys.get(projectKey)
	if !cached {
		projectLimit, cnt, months, err := projectStatus(r.Context(), pool, projectKey)
		// Must precede the generic error below, or an unknown key 500s.
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "unknown project key", http.StatusUnauthorized)
			return
		}
		if err != nil {
			// The database is unreachable rather than saying "no such key". For a
			// key validated before, serve the last known-good answer instead of
			// failing the caller's production traffic. The metadata is lost either
			// way, since a database that cannot be read cannot be written. Not
			// fail-open: an unknown key has no entry and still 500s below.
			staleCount, staleLimit, staleMonths, stale := keys.getStale(projectKey)
			if !stale {
				slog.Error("failed to validate project key", "err", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			// Warn, not Error: the request is served, but quota enforcement is
			// running blind.
			slog.Warn("database unreachable, serving stale key status",
				"err", err, "max_stale", keyCacheMaxStale)
			monthCount, limit, capMonths = staleCount, staleLimit, staleMonths
			// No keys.put: re-caching would reset the expiry and let a long
			// outage renew itself past keyCacheMaxStale forever.
		} else {
			// NULL means no override, so use the configured default. Resolved
			// here so everything downstream deals in a plain number.
			limit = monthlyLimit
			if projectLimit != nil {
				limit = *projectLimit
			}
			keys.put(projectKey, cnt, limit, months)
			monthCount, capMonths = cnt, months
		}
	}
	validateMs := time.Since(validateStart).Milliseconds()

	// Soft quota, so bounded overshoot is fine: concurrent requests race, and a
	// cached count can be up to keyCacheTTL stale.
	q := quota.For(limit, monthCount, capMonths)
	if q.Refuse {
		http.Error(w, "monthly request limit exceeded by too much; contact support", http.StatusTooManyRequests)
		return
	}

	if !rl.allow(projectKey) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many requests, slow down", http.StatusTooManyRequests)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	reqBytes, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		saveFailure(r, projectKey, provider, start, "", http.StatusBadRequest, "failed to read request body", q.Record)
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	r.Body.Close()

	var reqParsed requestBody
	_ = json.Unmarshal(reqBytes, &reqParsed) // best-effort; bad JSON leaves Model empty

	// r.URL.Path drops the query string, so it has to be carried over separately
	// or parameterised endpoints (pagination, list filters) silently reach the
	// provider asking a different question than the caller did.
	upstreamURL := providers[provider] + forwardPath
	if r.URL.RawQuery != "" {
		upstreamURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequest(r.Method, upstreamURL, bytes.NewReader(reqBytes))
	if err != nil {
		saveFailure(r, projectKey, provider, start, reqParsed.Model, http.StatusInternalServerError, "failed to build upstream request", q.Record)
		http.Error(w, "failed to build upstream request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header = r.Header.Clone()
	// Cloning forwards the caller's provider key untouched, which is the point.
	// This header is ours and has no business in a provider's logs.
	proxyReq.Header.Del("X-Monitor-Key")
	// Plaintext, so the streaming path can parse SSE text. Go only
	// auto-decompresses gzip when it set Accept-Encoding itself, so a
	// client-supplied value would leave resp.Body as raw gzip bytes.
	proxyReq.Header.Set("Accept-Encoding", "identity")

	upstreamStart := time.Now()
	resp, err := httpClient.Do(proxyReq)
	if err != nil {
		saveFailure(r, projectKey, provider, start, reqParsed.Model, http.StatusBadGateway, "upstream request failed", q.Record)
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	upstreamMs := time.Since(upstreamStart).Milliseconds()

	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		streamResponse(w, r, resp, start, reqParsed, projectKey, provider, validateMs, upstreamMs, cached, q.Record)
		return
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		saveFailure(r, projectKey, provider, start, reqParsed.Model, http.StatusBadGateway, "failed to read upstream response", q.Record)
		http.Error(w, "failed to read upstream response", http.StatusBadGateway)
		return
	}

	var respParsed responseBody
	_ = json.Unmarshal(respBytes, &respParsed) // best-effort; an unparseable body leaves zero tokens
	inputTokens, outputTokens := respParsed.tokens()

	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	w.Write(respBytes)
	// Flush now, or net/http holds a small body until the handler returns and the
	// metadata write below lands on the client's critical path.
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	entry := logEntry{
		Timestamp:    start.UTC().Format(time.RFC3339),
		Method:       r.Method,
		Path:         r.URL.Path,
		Model:        reqParsed.Model,
		Status:       resp.StatusCode,
		LatencyMs:    time.Since(start).Milliseconds(),
		ValidateMs:   validateMs,
		UpstreamMs:   upstreamMs,
		Cached:       cached,
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
	}
	logRequest(entry)

	rec := requestRecord{
		ProjectKey:       projectKey,
		Timestamp:        start.UTC(),
		Provider:         provider,
		Model:            reqParsed.Model,
		Status:           resp.StatusCode,
		LatencyMs:        entry.LatencyMs,
		ValidateMs:       &validateMs,
		UpstreamMs:       &upstreamMs,
		InputTokens:      inputTokens,
		OutputTokens:     outputTokens,
		EstimatedCostUSD: estimatedCost(reqParsed.Model, inputTokens, outputTokens),
	}
	recordRequest(r.Context(), rec, q.Record)
}

func saveFailure(r *http.Request, projectKey, provider string, start time.Time, model string, status int, errMsg string, record bool) {
	rec := requestRecord{
		ProjectKey: projectKey,
		Timestamp:  start.UTC(),
		Provider:   provider,
		Model:      model,
		Status:     status,
		LatencyMs:  time.Since(start).Milliseconds(),
		Error:      &errMsg,
	}
	recordRequest(r.Context(), rec, record)
}

// streamResponse relays an SSE response line by line, flushing after each write
// so tokens arrive as they are produced. It reads the same bytes to recover
// token counts and first-token latency, which are spread across events rather
// than sitting in one JSON object.
func streamResponse(w http.ResponseWriter, r *http.Request, resp *http.Response, start time.Time, reqParsed requestBody, projectKey, provider string, validateMs, upstreamMs int64, cached, record bool) {
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)

	reader := bufio.NewReader(resp.Body)
	var currentEvent string
	var inputTokens, outputTokens int
	var firstTokenAt time.Time

	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			w.Write(line)
			if flusher != nil {
				flusher.Flush()
			}

			text := strings.TrimSpace(string(line))
			switch provider {
			case "anthropic":
				parseAnthropicSSELine(text, &currentEvent, &inputTokens, &outputTokens, &firstTokenAt)
			case "openai":
				parseOpenAISSELine(text, &inputTokens, &outputTokens, &firstTokenAt)
			}
		}
		if err != nil {
			break // EOF or dropped connection; headers are already sent
		}
	}

	entry := logEntry{
		Timestamp:    start.UTC().Format(time.RFC3339),
		Method:       r.Method,
		Path:         r.URL.Path,
		Model:        reqParsed.Model,
		Status:       resp.StatusCode,
		LatencyMs:    time.Since(start).Milliseconds(),
		ValidateMs:   validateMs,
		UpstreamMs:   upstreamMs,
		Cached:       cached,
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		Streamed:     true,
	}
	if !firstTokenAt.IsZero() {
		entry.FirstTokenMs = firstTokenAt.Sub(start).Milliseconds()
	}
	logRequest(entry)

	rec := requestRecord{
		ProjectKey:       projectKey,
		Timestamp:        start.UTC(),
		Provider:         provider,
		Model:            reqParsed.Model,
		Status:           resp.StatusCode,
		LatencyMs:        entry.LatencyMs,
		ValidateMs:       &validateMs,
		UpstreamMs:       &upstreamMs,
		InputTokens:      inputTokens,
		OutputTokens:     outputTokens,
		EstimatedCostUSD: estimatedCost(reqParsed.Model, inputTokens, outputTokens),
	}
	if entry.FirstTokenMs > 0 {
		rec.FirstTokenMs = &entry.FirstTokenMs
	}
	recordRequest(r.Context(), rec, record)
}

func requireEnv(names ...string) {
	var missing []string
	for _, name := range names {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slog.Error("missing required environment variable(s); set them in the environment or in a .env file", "missing", strings.Join(missing, ", "))
		os.Exit(1)
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// A missing .env is fine in production, where the platform sets the
	// variables. requireEnv below is what enforces them.
	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file found; relying on the process environment")
	}
	requireEnv("DATABASE_URL")

	if v := os.Getenv("MAX_REQUEST_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			slog.Error("invalid MAX_REQUEST_BYTES; must be a positive integer", "value", v)
			os.Exit(1)
		}
		maxRequestBytes = n
	}

	// 0 disables the cap.
	if v := os.Getenv("MONTHLY_REQUEST_LIMIT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			slog.Error("invalid MONTHLY_REQUEST_LIMIT; must be a non-negative integer", "value", v)
			os.Exit(1)
		}
		monthlyLimit = n
	}

	var err error
	pool, err = connectDB(context.Background())
	if err != nil {
		slog.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Fail at startup with one clear message, rather than 500ing every request
	// because the database predates a column this build expects.
	if err := checkSchema(context.Background(), pool); err != nil {
		slog.Error("refusing to start", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", robotsHandler)
	mux.HandleFunc("/", handler)
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // bound slow-header (slowloris) clients
	}

	// ErrServerClosed is the normal Shutdown path; anything else means the
	// listener failed and is fatal.
	go func() {
		slog.Info("proxy listening on :8080, forwarding /anthropic and /openai")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// Fly sends SIGTERM on deploy. Shutdown stops accepting connections and
	// drains in-flight requests, including long-lived streams.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	slog.Info("shutdown signal received; draining in-flight requests")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "err", err)
	}
}
