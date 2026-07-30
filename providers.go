package main

import (
	"encoding/json"
	"strings"
	"time"
)

// A new provider needs an entry here, price rows in db.go, and an SSE line
// parser below if it streams.
var providers = map[string]string{
	"anthropic": "https://api.anthropic.com",
	"openai":    "https://api.openai.com",
}

// splitProviderPath turns "/openai/v1/chat/completions" into
// ("openai", "/v1/chat/completions", true). ok is false for an unknown provider
// or a path with no second segment.
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

// Anthropic names each event on an "event:" line and puts its payload on the
// following "data:" line, so currentEvent tracks which event that data belongs
// to.
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

// OpenAI has no "event:" line: every line is a bare "data: {...}" chunk, ending
// with "data: [DONE]". Usage appears only in the last chunk, and only if the
// request set stream_options.include_usage, so requests without it report zero
// tokens.
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
