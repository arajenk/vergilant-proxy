# Contributing

Issues and PRs are welcome.

**This repo is a mirror.** I work on the proxy inside a private monorepo next
to the dashboard and alert engine, and copy it out to here. Worth knowing
before you sink time into a change: merges land upstream first and get pushed
back out after, so an accepted PR can show up here as a commit that isn't
literally yours. You still get credit. Nothing gets quietly rewritten, the
history you see is the real history.

If a PR sits for a bit, that's just the sync step, not me ignoring it.

## What this project is for

It's a metadata-recording proxy, and I want to keep it small. Changes that fit:

- Bug fixes.
- New providers (see "Adding a provider" in the README).
- Updated model prices in `priceMap`. These are hand-maintained and drift.
- Clearer docs, especially anywhere the privacy claim reads vaguer than the
  code actually is.

Changes that probably don't fit: request/response caching, routing or
fallback logic, prompt management, evaluation features. Those are different
products, not this one.

## The one hard rule

**Never log or persist request or response bodies.** Not behind a flag, not
at debug level, not "temporarily." The whole reason this repo is public is
so people can verify that by reading it, and one debug line that dumps a
body makes the claim false for everyone. A PR that adds one won't get
merged, no exceptions.

Metadata is fine: model, status, latency, token counts, cost.

## Style

Plain, boring Go that reads top to bottom. Standard library unless there's a
real reason not to. The only dependencies are the Postgres driver and a `.env`
loader. Comments should explain *why*, not repeat what the line already says.

Run `gofmt` and `go vet ./...` before opening a PR.
