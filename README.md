# vergilant-proxy

This is the proxy that runs Vergilant. It sits in front of the Anthropic and
OpenAI APIs, forwards your requests upstream untouched, and logs what
happened: model, status, latency, token counts, estimated cost.

It does not log your prompts or the model's replies. That's the whole point.

```sh
# before
curl https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" ...

# after
curl https://your-proxy/anthropic/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "X-Monitor-Key: your-project-key" ...
```

Same request, same response, same streaming behavior. You just add a
`/anthropic` or `/openai` prefix to the path and one extra header.

## Why open source

Vergilant's whole pitch is "we see your metadata, never your data." That's an
easy thing to say and a hard thing to prove, especially from a company that'd
benefit from fudging it a little. So instead of asking you to take our word
for it, here's the code.

I want to be precise about the claim, because the precision is the point.
Request and response bodies pass through this proxy in memory. They have to,
it's a proxy. They're gone as soon as the response finishes. They're never
written to the database, never written to the logs.

The claim is "never stored, never logged." It is not "never touches our
servers." Anyone telling you that about a proxy is either confused or lying to
you.

### Where to check, if you don't believe me

You don't need to read the whole thing. Three spots decide whether this claim
actually holds:

- `logRequest` in `main.go`, the only place in the program that logs anything
  per-request. It takes a `logEntry` struct with a fixed set of fields and
  hands them to `slog` one at a time. There's no `body` field on that struct,
  and nothing logs `reqBytes` or `respBytes`.
- `saveRequest` in `db.go`, the only `INSERT` in the codebase. The column list
  is spelled out by hand, eleven columns, none of them hold content.
- `schema.sql`, the table those columns land in. There's nowhere to even put a
  body if some future change tried to sneak one in.

The bodies do exist, briefly, as `reqBytes` and `respBytes` in `handler`. Grep
for them if you want: they get read, parsed for `model` and token usage,
forwarded upstream, and dropped. That's the entire lifecycle and it's about
thirty lines of code.

Streaming works the same way. `streamResponse` writes each SSE line straight
through to your client, then parses its own local copy afterward to pull token
counts and time-to-first-token. Nothing gets buffered to disk or anywhere
else.

### It doesn't hold your provider keys either

The proxy has no Anthropic or OpenAI credentials of its own. Your `x-api-key`
/ `Authorization` header just rides along on the request and gets forwarded
upstream unchanged. There's no env var for a provider key because nothing ever
reads one.

## Running it

You'll need Postgres and Go 1.26+.

```sh
createdb vergilant
psql vergilant -f schema.sql
psql vergilant -c "INSERT INTO projects (key, name) VALUES ('dev-key', 'local')"

DATABASE_URL="postgres://localhost/vergilant" go run .
```

It listens on `:8080`. Send it a request with `X-Monitor-Key: dev-key` and
you'll see a row show up in `requests`.

### Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `DATABASE_URL` | yes | | Postgres connection string. |
| `MONTHLY_REQUEST_LIMIT` | no | `10000` | Requests per project per calendar month. Set to `0` to disable the cap. |
| `MAX_REQUEST_BYTES` | no | `26214400` (25 MiB) | Largest request body it'll accept. |

Drop a `.env` file in the working directory and it'll get picked up
automatically.

`MONTHLY_REQUEST_LIMIT` sets the cap for every project at once. To give one
project a different ceiling, set its `monthly_request_limit` column, which
overrides the env value for that project only:

```sh
# this one project gets 100k; everything else stays on the env default
psql vergilant -c "UPDATE projects SET monthly_request_limit = 100000 WHERE key = 'busy-app'"

# and 0 means no monthly cap at all, for that project
psql vergilant -c "UPDATE projects SET monthly_request_limit = 0 WHERE key = 'busy-app'"
```

`NULL`, the default, means "use the env value". Changes take up to 45 seconds
to take effect — see the key cache note in `keycache.go`.

### If you upgrade and forget to re-apply the schema

On startup the proxy checks that every column it reads or writes actually
exists, and refuses to run if any are missing:

```
ERROR refusing to start
      database is missing projects.monthly_request_limit; apply schema.sql
```

That's one message at boot instead of a 500 on every request. The list it
checks is `requiredColumns` in `schema_check.go` — short, and worth a glance if
you've customized the schema.

There's also an in-memory per-project rate limiter (30 burst, 10/sec
sustained) as a basic abuse guardrail. It lives in `ratelimit.go` as plain
constants, so just edit them if your traffic looks different.

### Adding a provider

Add an entry to `providers` in `providers.go`, add the model's prices to
`priceMap` in `db.go`, and if it streams, write an SSE line parser next to the
two already there. That's it, nothing else in the codebase cares which
provider you're talking to.

One note: `priceMap` is maintained by hand. Models that aren't listed in it
record a cost of `0` instead of a guess, on purpose. A wrong number is worse
than a missing one.

## How this relates to hosted Vergilant

This isn't a stripped-down demo version, it's the real thing. It's mirrored
out of the private monorepo that also has the dashboard and alerting engine,
so what you're reading here is what actually serves production traffic.

Hosted Vergilant is this proxy plus a dashboard over the `requests` table and
an alert engine that pings you on Discord when error rates spike, spend
jumps, or your traffic goes quiet. If you'd rather self-host and just query
the table yourself, that's a perfectly normal thing to do and it's supported.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Short version: issues and PRs are
welcome, but this repo is a mirror, so changes land upstream first and flow
back here.

## License

MIT. See [LICENSE](LICENSE).
