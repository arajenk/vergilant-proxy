# Contributing

Issues and pull requests are welcome.

**This repo is a mirror.** The proxy is developed inside a private monorepo
alongside the dashboard and alert engine, and is published here with
`git subtree`. That has one consequence worth knowing before you spend time on
a change: merges are applied upstream first and then pushed back out, so an
accepted PR may land here as a commit that isn't literally yours. You'll be
credited. Nothing is silently rewritten — the history you see is the real
history.

If a PR sits for a bit, that's the sync step, not disinterest.

## What this project is for

It's a metadata-recording proxy, deliberately small. Changes that fit:

- Bug fixes.
- New providers (see "Adding a provider" in the README).
- Updated model prices in `priceMap` — these are hand-maintained and drift.
- Clearer docs, especially anywhere the privacy claim reads as vaguer than the
  code actually is.

Changes that probably don't fit: request/response caching, routing or fallback
logic, prompt management, evaluation features. Those are different products.

## The one hard rule

**Never log or persist request or response bodies.** Not behind a flag, not at
debug level, not "temporarily." The entire reason this repo is public is so that
people can verify this by reading it, and a single debug line that dumps a body
makes the claim false for everyone. A PR that adds one won't be merged.

Metadata — model, status, latency, token counts, cost — is fine.

## Style

Plain, boring Go that reads top to bottom. Standard library unless there's a
real reason not to; the only dependencies are the Postgres driver and a `.env`
loader. Comments explain *why* a thing is the way it is, not what the line does.

Run `gofmt` and `go vet ./...` before opening a PR.
