# Standing session rules

## Efficiency
- Minimize test loops: batch related edits first, run one targeted check while iterating, and the full suite once at the end — not after every small change.
- Batch writes, reads, and tooling: chain related shell steps into one command, fire independent tool calls in parallel, and keep `gh`/API endpoint calls to a minimum.
- Answer N questions with one command: chain independent `rg`/`git`/`ls` steps instead of spending N turns.
- Cache and cite long output: tee builds, tests, clippy, and fetches to a file under `$env:TEMP\opencode`, then filter what reaches the transcript; keep the file path and the command so any dropped detail is one call away.
- Fetch collections once: get the whole `gh api` list and filter with `ConvertFrom-Json`; avoid one request per item and `--jq` expressions with `\(`/`|` in PowerShell.
- No sleep-polling: check CI once at a natural boundary or use `gh pr checks --watch`.
- Filter at the source: end long commands with `Select-String`/`Select-Object -Last N` so full logs never enter the transcript.
- Never truncate a native command's output with `Select-Object -First N` while it still needs to write: closing the pipe kills the process mid-write (it silently skipped a Cargo.lock write). Redirect to a file first (`Out-File`), then filter the file.
- Trim redundancy, not evidence: drop repeated reads, success logs, and dead ends; keep exact identifiers, error text, decisions, and their reasons.
- Leave a trail: when context is summarized or dropped, record the command, file path, or `file:line` that restores it; re-read a file before editing it when the exact text is not in context.

## Versioning
- Patch bump by default. Minor bump only for breaking changes (or on explicit user request).
- Ask the user before cutting a minor release tag — core and satellites alike.

## Release
- Create and push the release tag before publishing (crates.io, npm, or any other registry). Never publish before the tag exists.
- Land the version bump on main via PR BEFORE tagging: `v*` tags are permanently locked once pushed (ruleset forbids delete/force-push — v0.20.0 is pinned to a commit whose Cargo.lock differs from main). The bump commit must include Cargo.lock, or every `--locked` consumer fails.

## Rust
- Run `cargo clippy --all-targets` before push; zero warnings. CI does not gate on clippy, so this is the only check.
- Clear all compiler and clippy warnings before `cargo publish`.
