# vergilant-proxy

The proxy that runs Vergilant. It sits in front of the Anthropic and OpenAI
APIs, forwards your requests upstream untouched, and records what happened:
model, status, latency, token counts, estimated cost.

It does not record your prompts or the model's replies.

```sh
# before
curl https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" ...

# after
curl https://your-proxy/anthropic/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "X-Monitor-Key: your-project-key" ...
```

Same request, same response, same streaming behaviour. The path gains a
`/anthropic` or `/openai` prefix and the request gains one header.

## Why this is open source

Vergilant's pitch is that we see your metadata and never your data. That is a
claim about code you can't see, made by a company that would benefit from
shading it. So here is the code.

Be precise about what the claim is, because the precision is the point:

- Request and response bodies **pass through this proxy in memory**. They have
  to — it's a proxy. They are gone when the response finishes.
- They are **never written to the database and never written to the logs**.

The claim is *never stored, never logged*. It is **not** "never touches our
servers" — anyone telling you that about a proxy is confused or lying.

### Where to check

You don't need to read all of it. Three places decide whether the claim holds:

- **`logRequest` in `main.go`** — the only function in the program that logs
  anything per-request. It takes a `logEntry` struct with fixed fields and
  passes them to `slog` one by one. There is no `body` field on the struct, and
  no code path that logs `reqBytes` or `respBytes`.
- **`saveRequest` in `db.go`** — the only `INSERT`. Its column list is written
  out literally, eleven columns, none of which hold content.
- **`schema.sql`** — the table those columns go into. There is nowhere to put a
  body even if some future line of code tried.

The bodies exist as `reqBytes` and `respBytes` in `handler`. Grep for them: they
are read, parsed for `model` and `usage` token counts, forwarded, and dropped.
That's the whole lifecycle, and it's about thirty lines.

Streaming works the same way. `streamResponse` writes each SSE line straight
through to your client and *then* parses a local copy to pull token counts and
time-to-first-token. It never buffers the stream to disk or anywhere else.

### It doesn't hold your provider keys either

The proxy has no Anthropic or OpenAI credentials of its own. Your `x-api-key` /
`Authorization` header rides along on each request and is forwarded upstream
unchanged. There is no environment variable for a provider key, because nothing
reads one.

## Running it

You need Postgres and Go 1.26+.

```sh
createdb vergilant
psql vergilant -f schema.sql
psql vergilant -c "INSERT INTO projects (key, name) VALUES ('dev-key', 'local')"

DATABASE_URL="postgres://localhost/vergilant" go run .
```

It listens on `:8080`. Send it a request with `X-Monitor-Key: dev-key` and a
row shows up in `requests`.

### Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `DATABASE_URL` | yes | — | Postgres connection string. |
| `MONTHLY_REQUEST_LIMIT` | no | `10000` | Requests per project per calendar month. `0` disables the cap. |
| `MAX_REQUEST_BYTES` | no | `26214400` (25 MiB) | Largest request body accepted. |

A `.env` file in the working directory is loaded if present.

There is also an in-memory per-project token bucket (30 burst, 10/sec sustained)
as an abuse guardrail. It's in `ratelimit.go` as constants; edit them if your
traffic shape is different.

### Adding a provider

Add an entry to `providers` in `providers.go`, add the model's prices to
`priceMap` in `db.go`, and — if it streams — write an SSE line parser next to
the two that are there. Nothing else is provider-aware.

Note that `priceMap` is maintained by hand and models not listed in it record a
cost of zero rather than a wrong guess.

## Relationship to hosted Vergilant

This is the real proxy, not a reduced sample. It is mirrored out of the private
monorepo that also holds the dashboard and the alerting engine, so what you read
here is what serves production traffic.

Hosted Vergilant is this plus a dashboard over those `requests` rows and an
alert engine that pings you on Discord when error rates spike, spend jumps, or
your traffic goes silent. If you'd rather run the proxy yourself and query the
table directly, that works and is a supported thing to do.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Short version: issues and PRs welcome,
but this repo is a mirror, so merges land upstream first and flow back here.

## License

MIT. See [LICENSE](LICENSE).
