# Contributing

## Scope
- Keep changes minimal and production-safe.
- Do not commit secrets or local runtime artifacts.
- Prefer small, reviewable PRs.

## Local Setup
1. `cp .env.example .env`
2. Fill required credentials in `.env`.
3. `cargo build --release`

## Required Checks (Before Push)
Run in this order:
- `cargo fmt --all -- --check`
- `cargo check --all-targets`
- `cargo test --all-targets --quiet`
- `./scripts/security_audit.sh`

## Strategy Surface Changes
If strategy logic, risk gates, entry/exit behavior, sizing, checkpoint schedule, or strategy defaults change, update `strategy-changelog.md` in the same PR.

## Docs Freshness Rule
If runtime behavior or env surface changes, update related docs in the same PR:
- `README.md`
- `docs/*.md`
- env template comments (`.env.example`, `.env.full.example`) when relevant.
