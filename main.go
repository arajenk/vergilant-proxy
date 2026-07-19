package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

var pool *pgxpool.Pool

type requestBody struct {
	Model string `json:"model"`
}

// Usage carries both providers' field names for the same two numbers —
// Anthropic and OpenAI never populate the same pair, so tokens() below just
// returns whichever pair is non-zero.
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
	InputTokens  int    `json:"input_tokens,omitempty"`
	OutputTokens int    `json:"output_tokens,omitempty"`
	Streamed     bool   `json:"streamed,omitempty"`
	FirstTokenMs int64  `json:"first_token_ms,omitempty"`
}

func handler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	projectKey := r.Header.Get("X-Monitor-Key")

	// Checked before the project key so a garbage path 404s without a DB
	// round-trip.
	provider, forwardPath, ok := splitProviderPath(r.URL.Path)
	if !ok {
		http.Error(w, "unknown provider; use /anthropic/... or /openai/...", http.StatusNotFound)
		return
	}

	// Rejected requests are not saved: an unknown key can't be attributed to
	// a project, and storing arbitrary client-supplied strings would pollute
	// the requests table.
	known, err := projectKeyExists(r.Context(), pool, projectKey)
	if err != nil {
		log.Println("failed to validate project key:", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !known {
		http.Error(w, "unknown project key", http.StatusUnauthorized)
		return
	}

	reqBytes, err := io.ReadAll(r.Body)
	if err != nil {
		saveFailure(r, projectKey, provider, start, "", http.StatusBadRequest, "failed to read request body")
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	r.Body.Close()

	var reqParsed requestBody
	json.Unmarshal(reqBytes, &reqParsed) // best-effort; missing/bad JSON just leaves Model empty

	proxyReq, err := http.NewRequest(r.Method, providers[provider]+forwardPath, bytes.NewReader(reqBytes))
	if err != nil {
		saveFailure(r, projectKey, provider, start, reqParsed.Model, http.StatusInternalServerError, "failed to build upstream request")
		http.Error(w, "failed to build upstream request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header = r.Header.Clone()
	// Force plaintext so the streaming path can parse SSE text directly;
	// otherwise http.DefaultClient only auto-decompresses gzip when it set
	// Accept-Encoding itself, and a client-supplied value would leave
	// resp.Body as raw gzip bytes.
	proxyReq.Header.Set("Accept-Encoding", "identity")

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		saveFailure(r, projectKey, provider, start, reqParsed.Model, http.StatusBadGateway, "upstream request failed")
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		streamResponse(w, r, resp, start, reqParsed, projectKey, provider)
		return
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		saveFailure(r, projectKey, provider, start, reqParsed.Model, http.StatusBadGateway, "failed to read upstream response")
		http.Error(w, "failed to read upstream response", http.StatusBadGateway)
		return
	}

	var respParsed responseBody
	json.Unmarshal(respBytes, &respParsed) // best-effort; e.g. streamed SSE bodies won't parse, leaving zero token counts
	inputTokens, outputTokens := respParsed.tokens()

	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	w.Write(respBytes)

	entry := logEntry{
		Timestamp:    start.UTC().Format(time.RFC3339),
		Method:       r.Method,
		Path:         r.URL.Path,
		Model:        reqParsed.Model,
		Status:       resp.StatusCode,
		LatencyMs:    time.Since(start).Milliseconds(),
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
	}
	logLine, _ := json.Marshal(entry)
	log.Println(string(logLine))

	rec := requestRecord{
		ProjectKey:       projectKey,
		Timestamp:        start.UTC(),
		Provider:         provider,
		Model:            reqParsed.Model,
		Status:           resp.StatusCode,
		LatencyMs:        entry.LatencyMs,
		InputTokens:      inputTokens,
		OutputTokens:     outputTokens,
		EstimatedCostUSD: estimatedCost(reqParsed.Model, inputTokens, outputTokens),
	}
	if err := saveRequest(r.Context(), pool, rec); err != nil {
		log.Println("failed to save request record:", err)
	}
}

// saveFailure persists a request record for a proxy-level failure (bad
// request body, unreachable upstream, etc.) that never reaches the normal
// success logging above — these previously went unrecorded entirely.
func saveFailure(r *http.Request, projectKey, provider string, start time.Time, model string, status int, errMsg string) {
	rec := requestRecord{
		ProjectKey: projectKey,
		Timestamp:  start.UTC(),
		Provider:   provider,
		Model:      model,
		Status:     status,
		LatencyMs:  time.Since(start).Milliseconds(),
		Error:      &errMsg,
	}
	if err := saveRequest(r.Context(), pool, rec); err != nil {
		log.Println("failed to save request record:", err)
	}
}

// streamResponse passes an SSE (text/event-stream) response through to the
// client one line at a time, flushing after every write so the client sees
// tokens as they arrive instead of waiting for the whole response. Alongside
// that passthrough it watches the same bytes to recover token counts and
// first-token latency, since streaming responses spread that information
// across multiple events instead of one JSON object like non-streaming does.
// The two providers' SSE framing differs enough (event-typed vs. bare data
// chunks) that parsing a line is dispatched to a per-provider function
// rather than one parser branching throughout.
func streamResponse(w http.ResponseWriter, r *http.Request, resp *http.Response, start time.Time, reqParsed requestBody, projectKey, provider string) {
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
			w.Write(line) // passthrough first, unmodified
			if flusher != nil {
				flusher.Flush()
			}

			// Parse a local copy only; never touches the bytes already sent.
			text := strings.TrimSpace(string(line))
			switch provider {
			case "anthropic":
				parseAnthropicSSELine(text, &currentEvent, &inputTokens, &outputTokens, &firstTokenAt)
			case "openai":
				parseOpenAISSELine(text, &inputTokens, &outputTokens, &firstTokenAt)
			}
		}
		if err != nil {
			break // EOF, or upstream connection dropped; headers are already sent, nothing more to do
		}
	}

	entry := logEntry{
		Timestamp:    start.UTC().Format(time.RFC3339),
		Method:       r.Method,
		Path:         r.URL.Path,
		Model:        reqParsed.Model,
		Status:       resp.StatusCode,
		LatencyMs:    time.Since(start).Milliseconds(),
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		Streamed:     true,
	}
	if !firstTokenAt.IsZero() {
		entry.FirstTokenMs = firstTokenAt.Sub(start).Milliseconds()
	}
	logLine, _ := json.Marshal(entry)
	log.Println(string(logLine))

	rec := requestRecord{
		ProjectKey:       projectKey,
		Timestamp:        start.UTC(),
		Provider:         provider,
		Model:            reqParsed.Model,
		Status:           resp.StatusCode,
		LatencyMs:        entry.LatencyMs,
		InputTokens:      inputTokens,
		OutputTokens:     outputTokens,
		EstimatedCostUSD: estimatedCost(reqParsed.Model, inputTokens, outputTokens),
	}
	if entry.FirstTokenMs > 0 {
		rec.FirstTokenMs = &entry.FirstTokenMs
	}
	if err := saveRequest(r.Context(), pool, rec); err != nil {
		log.Println("failed to save request record:", err)
	}
}

// requireEnv fails startup if any of the given environment variables are
// unset, so a misconfigured deployment is caught immediately instead of
// surfacing later as a confusing failure. ANTHROPIC_API_KEY and
// OPENAI_API_KEY are validated here but not otherwise used by the proxy:
// each client request still carries its own provider auth header, which is
// forwarded upstream unchanged.
func requireEnv(names ...string) {
	var missing []string
	for _, name := range names {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		log.Fatalf("missing required environment variable(s): %s — set them in the environment or in a .env file", strings.Join(missing, ", "))
	}
}

func main() {
	// A missing .env is fine in production, where the platform sets
	// variables directly; requireEnv below is what actually enforces them.
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found; relying on the process environment")
	}
	requireEnv("ANTHROPIC_API_KEY", "OPENAI_API_KEY")

	var err error
	pool, err = connectDB(context.Background())
	if err != nil {
		log.Fatal("failed to connect to database: ", err)
	}
	defer pool.Close()

	http.HandleFunc("/", handler)
	log.Println("proxy listening on :8080, forwarding /anthropic and /openai")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
