# ADR 0015 — Verifier Plugin Architecture

**Status:** Accepted
**Date:** 2026-03-09

## Context

The verifier chain is the core value-add of the stophammer primary node. As the
network grows, operators will need to add, remove, or reorder verifiers — for
example to add a music classifier, tighten or loosen medium enforcement, or
experiment with new signal sources.

The original implementation hardcoded the verifier list in `main.rs` and
colocated all verifier structs in `verify.rs`. Adding a new verifier required
modifying two files and redeploying all nodes simultaneously, with no way to
vary the chain between deployments.

Several plugin approaches were evaluated:

| Approach | Isolation | Authoring | Runtime cost | Verdict |
|---|---|---|---|---|
| WASM (wasmtime/extism) | Sandbox | Needs wasm toolchain | Low | Over-specified; DB access across WASM boundary is complex |
| Native .so / .dylib | None | Rust cdylib | None | Unstable ABI; unsafe; one Rust version update away from segfault |
| Subprocess (stdin/stdout JSON) | OS process | Any language | Per-ingest fork or IPC | Fork overhead unacceptable at crawl scale |
| Embedded scripting (JS/Lua) | Script sandbox | JS or Lua | V8 / Lua runtime | Large dependency for focused use case |
| Config-driven built-ins | n/a | Rust (recompile) | None | Simplest; covers all current needs |

## Decision

Adopt the **config-driven built-in** model:

1. **One file per verifier** — each verifier lives in `src/verifiers/<name>.rs`
   as a self-contained Rust module. No verifier logic lives in `verify.rs`.

2. **`verify.rs` owns only the trait, chain, and registry** — the `Verifier`
   trait, `VerifierChain`, `IngestContext`, `VerifyResult`, `ChainSpec`, and
   `build_chain` are the only things in `verify.rs`.

3. **`VERIFIER_CHAIN` controls the chain at runtime** — a comma-separated
   ordered list of verifier names. No code change is needed to reorder or
   disable verifiers. The default covers all built-ins in their recommended
   order and is used when the variable is absent.

4. **Adding a verifier is a four-step procedure** documented in the
   `verify.rs` module doc:
   - Write `src/verifiers/<name>.rs`
   - Declare it in `src/verifiers/mod.rs`
   - Add a `match` arm in `build_chain`
   - Set `VERIFIER_CHAIN` to include the name

5. **Community nodes run an empty chain** — community nodes do not accept
   ingest requests; the verifier chain is not constructed for them. They trust
   the primary to have validated events before signing them.

## Consequences

- A recompile is required to add a new verifier. This is acceptable: verifiers
  need access to `IngestContext` which holds a live `rusqlite::Connection` — a
  hard boundary that subprocess/WASM approaches cannot easily cross.
- The chain is fully observable at startup: unknown names in `VERIFIER_CHAIN`
  log a warning and are skipped, so misconfiguration is immediately visible.
- The module structure makes each verifier independently testable. Future
  verifiers (music classifier, iTunes subcategory, tombstone detector) each
  become a single file addition.
- If a future requirement genuinely needs runtime-extensible plugins (e.g.
  community-authored verifiers without recompiling the primary), the subprocess
  hook approach remains viable as a single additional verifier slot in the
  registry. This ADR does not foreclose that path.
