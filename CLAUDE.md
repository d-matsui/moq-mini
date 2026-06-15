# CLAUDE.md

This file provides guidance for AI coding agents when working with code in this repository.

## Project Overview

MoQ (Media over QUIC) is a next-generation live media delivery protocol providing real-time latency at massive scale. This is a project to implement MoQT (MoQ Transport) from scratch in order to understand the protocol.

## Common Development Commands

```bash
# Code quality and testing
just check        # Run all tests and linting
just fix          # Auto-fix linting issues
just build        # Build all packages
```

## Architecture

TBD

Key architectural rule: The CDN/relay does not know anything about media. Anything in the `moq` layer should be generic, using rules on the wire on how to deliver content.

## Project Structure

TBD

## Dependencies

- When adding new dependencies, always use the **newest stable version** available.
- **Prefer a maintained third-party crate over hand-rolling non-core functionality** (standard container/codec parsers, compression, serialization, etc.). Reserve bespoke code for the wire/protocol layers where we need full control or no suitable crate exists.

## Development Tips

1. The project uses `just` as the task runner. Check `justfile` for all available commands.
2. For Rust development, the workspace is configured in the root `Cargo.toml`.
3. For JS/TS development, bun workspaces are used with configuration in the root `package.json`.
4. Consult `docs/reference/` for protocol work. **Read these local files instead of `WebFetch`/`WebSearch`.**
   - Wire format/terminology: `docs/reference/draft-ietf-moq-transport-18.txt` (plus the other drafts there).
   - Clean-design model: `docs/reference/moq/`, a gitignored symlink to Luke's [moq.dev](https://moq.dev) Rust implementation.

## Writing Style

- **No em dashes (—)** in code, comments, doc comments, commit messages, or any prose. Use a period and start a new sentence, or use a comma/parenthesis if the clauses are tightly bound.

## Rust Conventions

- **Error handling**: Use `thiserror` with `#[from]` for library crates, `anyhow` for binaries. Always add `#[non_exhaustive]` to public `thiserror` enums.
- Use `anyhow::Context` (`.context("msg")`) instead of `.map_err(|_| anyhow::anyhow!("msg"))` for error conversion
- **Config flags + TOML merge**: For any `#[arg]` field on a TOML-loadable config, use `Option<T>` (not bare `bool` / `String` / etc.). The TOML→CLI merge clobbers bare fields with their `Default` when the flag is absent, silently overwriting TOML values.
- **Prefer `if let` / `let ... else` over an unwrapping `match`**: a `match` whose only job is to unwrap (`Ok(v) => v` / `Some(v) => v`) reads cleaner as `if let Some(v) = x { ... }` or `let Some(v) = x else { ... };`. Matching on an `Option`/`Result` just to bind the inner value is the tell. Keep `match` when both arms do real work or you need the `Err` / `None` payload.
- **`poll_*` plumbing**: a `Poll::Pending => Poll::Pending` arm usually means `ready!(...)` will collapse the match. It composes with `?`: `let v = ready!(inner.poll_next(cx))?;` in a `fn -> Poll<Result<...>>` both unwraps the `Poll` and converts the error.

## Comment Conventions

- Keep things brief and avoid comments if the code is self-explanatory. Reserve comments for the non-obvious WHY: a hidden constraint, a subtle invariant, a workaround for a specific bug, behavior that would surprise a reader. This is about *implementation* comments inside function bodies and on private items.
- **Public API symbols are the exception: document every exported symbol.** Each `pub` Rust item (and exported JS/TS symbol) gets a doc comment (`///` / `/** */`), even when it looks self-explanatory, plus a module-level doc (`//!` / `@module`) on each entrypoint. Say what a reader needs (units, ownership, lifecycle, what it wraps), not throat-clearing.
- Write the way you'd say it out loud, not the way a doc generator would. One short line is almost always enough. Skip throat-clearing like "This function is responsible for...".
- Comments must reflect the **current** state of the code, not its history. Don't write "X no longer does Y" or "this used to cascade". Describe what the code does today, or delete the comment. Migration context belongs in commit messages and PR descriptions, where it ages with the change rather than rotting in the source.

## AI Attribution

LLM-authored prose visible to humans (PR descriptions, PR comments, review replies) should end with `(Written by Claude)` or similar. Do **not** tag code comments or doc comments: source markers rot. Commit attribution lives in the `Co-Authored-By` trailer, not the commit body.

## Refactor As You Go

A function with 4+ args, or a call site passing the same 3+ values into multiple functions, is a struct waiting to happen. Make the change in the same PR rather than leaving a TODO. Same for repeated tuples returned across modules.

## Public API Scrutiny

Before exposing a new public type, function, or field, stop and ask: how will consumers actually call this, and what are we likely to add later? Default to the smallest surface that does the job. Prefer one insulated high-level entry point (plain config in, plain result out) over exposing every building block. Then future-proof what you do expose so additions don't force a breaking change:

- **Config structs consumers construct**: add `#[non_exhaustive]` and a `Default` or constructor. New optional fields then stay additive (callers build via `default()`/`new()` + field set, not struct literals). Prefer adding a field to an existing `#[non_exhaustive]` config over adding a function parameter.
- **Public enums that may gain variants**: add `#[non_exhaustive]` so external `match`es keep compiling.
- **Name by role, not by today's only implementation** (e.g. `Session`, not `WebTransportSession`), so a second one slots in without a rename. And don't bundle generic options under a specific-case name (e.g. a setting shared by origin and relay doesn't belong in a `RelayConfig`).
- **Namespace with modules; keep type names short.** Split a growing crate into role modules (e.g. `encode`, `decode`) and let each own short, unprefixed names. The module already supplies the prefix, so e.g. `encode::Encoder` beats `MessageEncoder` and `encode::Config` beats `EncodeConfig`. And don't nest a module whose name echoes its main type: `encode::encoder::Encoder` stutters; re-export it flat (`pub use encoder::Encoder`) so it reads `encode::Encoder`, and keep `mod encoder` private.
- **Don't leak a third-party type** (`quinn`, `web_transport`, etc.) in a signature unless the crate is explicitly a thin wrapper. If you must, re-export the dependency and document that a major bump is a breaking change; keep the recommended high-level path free of it.

## Tooling

- **TypeScript**: Always use `bun` for all package management and script execution (not npm, yarn, or pnpm)
- **Common**: Use `just` for common development tasks
- **Rust**: Use `cargo` for Rust-specific operations
- **Formatting/Linting**: Biome for JS/TS formatting and linting
- **Builds**: Nix flake for reproducible builds (optional)
- **Local-first**: When work can live in a `just` recipe (invoked via `nix develop --command`) or as logic in a GitHub Actions workflow step, prefer the recipe. The same code then runs reproducibly on a developer machine and in CI, and is debuggable locally without pushing commits. Workflow YAML should mostly delegate to `just`; reach for plugins (`dorny/paths-filter`, custom actions, etc.) only when a recipe genuinely can't express the logic.
- **CI**: Prefer building release artifacts inside Nix (`nix build .#pkg`) over relying on runner-provided toolchains and `apt`/`brew` packages. Pinning the build environment in `flake.lock` makes artifacts deterministic and decouples them from drift in GitHub Actions runner images. Reach for the runner-native toolchain only when Nix doesn't fit (e.g. Windows runners).
- **JS async patterns**: Prefer wrappers that auto-clean-up over raw `setInterval`, `setTimeout`, `addEventListener`, which leak timers/listeners if cleanup is forgotten.

## Testing Approach

- Run `just check` to execute all tests and linting.
- Run `just fix` to automatically fix formating and easy things.
- Rust tests are integrated within source files
- Async tests that sleep should call `tokio::time::pause()` at the start to simulate time instantly

## Workflow

When making changes to the codebase:

1. Branch off `main`; don't commit directly to it
2. Make your code changes
3. Run `just fix` to auto-format and fix linting issues
4. Run `just check` to verify everything passes
5. Update any affected docs in the same PR
6. Add tests where they're easy to write
7. Commit and push changes

## PR Title and Description Maintenance

When pushing additional commits to an existing PR, check whether the title and description still describe the change accurately. They often go stale during review iterations: a flag gets renamed, an API gets reshaped, an extra fix lands, etc. The PR description is what shows up in the squash-merge commit, so a stale title/body means a misleading entry in `git log` forever.

Update them with `gh pr edit <num> --title "..." --body "..."` whenever the scope shifts. Specifically watch for:

- Flags, file names, or public APIs renamed in later commits but still referenced by their old name in the PR body.
- Bullet points in the "Summary" section that describe behavior the latest commits have changed or removed.
- The test-plan checklist getting out of date as new tests are added.

When you edit a PR description you authored, keep the `(Written by Claude)` marker so reviewers still know the body wasn't human-authored.
