package main

import (
	"encoding/json"
	"strings"
	"time"
)

// providers maps the proxy path prefix a client uses (e.g. /openai/...) to
// the upstream API it forwards to. Adding a provider means adding one entry
// here, its price-map rows in db.go, and — if it streams — an SSE line
// parser alongside the two below.
var providers = map[string]string{
	"anthropic": "https://api.anthropic.com",
	"openai":    "https://api.openai.com",
}

// splitProviderPath reads the leading path segment as a provider name and
// returns the remaining path to forward upstream: "/openai/v1/chat/completions"
// becomes ("openai", "/v1/chat/completions", true). ok is false if the path
// has no second segment or the first segment isn't a known provider.
func splitProviderPath(path string) (provider, rest string, ok bool) {
	trimmed := strings.TrimPrefix(path, "/")
	provider, rest, found := strings.Cut(trimmed, "/")
	if !found {
		return "", "", false
	}
	if _, known := providers[provider]; !known {
		return "", "", false
	}
	return provider, "/" + rest, true
}

// parseAnthropicSSELine updates the running token counts and first-token
// time from one line of an Anthropic Messages API SSE stream. Anthropic
// pairs each event with an "event:" line naming it and a "data:" line
// carrying its JSON payload, so currentEvent tracks which event the next
// data line belongs to.
func parseAnthropicSSELine(text string, currentEvent *string, inputTokens, outputTokens *int, firstTokenAt *time.Time) {
	switch {
	case strings.HasPrefix(text, "event:"):
		*currentEvent = strings.TrimSpace(strings.TrimPrefix(text, "event:"))
	case strings.HasPrefix(text, "data:"):
		data := strings.TrimSpace(strings.TrimPrefix(text, "data:"))
		switch *currentEvent {
		case "message_start":
			var msg struct {
				Message struct {
					Usage struct {
						InputTokens  int `json:"input_tokens"`
						OutputTokens int `json:"output_tokens"`
					} `json:"usage"`
				} `json:"message"`
			}
			if json.Unmarshal([]byte(data), &msg) == nil {
				*inputTokens = msg.Message.Usage.InputTokens
				*outputTokens = msg.Message.Usage.OutputTokens
			}
		case "message_delta":
			var delta struct {
				Usage struct {
					OutputTokens int `json:"output_tokens"`
				} `json:"usage"`
			}
			if json.Unmarshal([]byte(data), &delta) == nil {
				*outputTokens = delta.Usage.OutputTokens
			}
		case "content_block_delta":
			if firstTokenAt.IsZero() {
				*firstTokenAt = time.Now()
			}
		}
	case text == "":
		*currentEvent = "" // blank line = SSE event boundary
	}
}

// parseOpenAISSELine updates the running token counts and first-token time
// from one line of an OpenAI chat-completions SSE stream. Unlike Anthropic,
// OpenAI has no "event:" line, every line is a bare "data: {...}" chunk,
// terminated by a final "data: [DONE]". Usage only shows up in the last
// chunk, and only if the request set stream_options.include_usage, so
// requests that skip it just show zero tokens for a streamed response.
func parseOpenAISSELine(text string, inputTokens, outputTokens *int, firstTokenAt *time.Time) {
	if !strings.HasPrefix(text, "data:") {
		return
	}
	data := strings.TrimSpace(strings.TrimPrefix(text, "data:"))
	if data == "" || data == "[DONE]" {
		return
	}
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal([]byte(data), &chunk) != nil {
		return
	}
	if chunk.Usage.PromptTokens != 0 || chunk.Usage.CompletionTokens != 0 {
		*inputTokens = chunk.Usage.PromptTokens
		*outputTokens = chunk.Usage.CompletionTokens
	}
	if firstTokenAt.IsZero() {
		for _, c := range chunk.Choices {
			if c.Delta.Content != "" {
				*firstTokenAt = time.Now()
				break
			}
		}
	}
}
