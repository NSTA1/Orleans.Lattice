---
applyTo: "src/lattice.api.mcp/**,src/lattice.api.mcp.telemetry/**,src/lattice.explorer/**,src/lattice.explorer.entra/**,src/lattice.replication/**,src/lattice.replication.grpc/**,src/lattice.membership/**,src/lattice.membership.entra/**,src/lattice.membership.entra.graph/**,src/lattice.api.auth/**,src/lattice.api.auth.grpc/**"
---

# Security Boundaries and Invariants

These are load-bearing security invariants for the auth, membership, replication,
telemetry, MCP, and Explorer surfaces. They were established by the v8 security
hardening epic (#1270, sub-issues #1264-#1269). Do not regress them, and apply the
cross-cutting principles below to any new code on these surfaces. When you touch a
seam named here, re-read the invariant before changing it.

## Cross-cutting principles

1. **Fail closed.** Every security gate denies on ambiguity, parse failure, missing
   context, or a null collaborator - it never falls through to allow. An
   unparseable matcher, a null `HttpContext`, a null authorizer, or an
   unresolvable principal is a denial, not a pass. New gates must have an explicit
   deny/allow decision on every branch, with deny as the default arm.
2. **Never trust peer- or wire-supplied classification.** Anything a remote peer or
   client can set (a replication batch's declared merge-mode, a tree id, a metric
   name, a requested tool) is re-resolved or re-validated locally against
   authoritative local state before it is honoured. The wire value is an assertion
   to check, never a fact to act on.
3. **Enforce at the single narrowest seam every path funnels through.** Put the
   check where all transports and callers converge (the applier, not each transport
   handler; a shared lock-step gate consulted at every enforcement point), so one
   well-tested rejection covers all current and future entry points. Do not scatter
   partial checks per transport.
4. **Isolate security context per request / per circuit.** Credential, auth-session,
   and connection state is scoped to the request or Blazor circuit, never a process
   singleton, and no singleton or hosted service may capture that scoped graph (a
   captive dependency silently re-globalises it). Adding a service on these surfaces
   requires auditing its lifetime and everything it captures.
5. **No dead security config.** A configuration flag or option that claims to
   enforce must be wired to real enforcement and covered by a test that proves the
   enforcement fires (and that it is skipped when the flag is off). Do not add a
   security knob that does nothing.
6. **Security hot paths keep the allocation bar.** Fail-closed and steady-state
   security paths allocate nothing avoidable: static/cached header value sets, a
   cached denied `Task`, span-based matching over substring allocation. Allocate
   only on the cold reject/diagnostic path, and comment any intentional allocation.

## Surface-specific invariants

### MCP tool authorization (`src/lattice.api.mcp`)
- The tool authorizer is consulted at **both** enforcement points - `tools/list`
  advertisement (session configurator) and `tools/call` invocation (credential
  stamping tool) - through one shared **lock-step** gate. A tool that is hidden at
  advertisement must also be unreachable at invocation, and vice versa.
- The gate is **fail-closed**: a null `HttpContext` or null authorizer denies. The
  default `DenyAll` authorizer means tools are denied until a host explicitly opts
  in a permissive authorizer. This is secure-by-default; do not add an implicit
  allow fallback.
- The `lattice_capabilities` meta-tool is the only ungated advertisement; do not
  widen the ungated set.

### Telemetry metric-name allow-list (`src/lattice.api.mcp.telemetry`)
- The PromQL `__name__` / metric-name allow-list fails closed: an unparseable,
  ambiguous, or non-exact-match `__name__` matcher is treated as **not** on the
  allow-list (deny), never as a bypass. Label-matcher parsing must not offer a path
  that evades the allow-list.
- Match `__name__` label names via span comparison; only allocate a substring on the
  actual matched-name path, never for every in-brace label.

### Replication receiver enrollment gate (`src/lattice.replication`)
- The receiver gate lives at the **applier seam** (`ReplicationApplier.ApplyAsync` /
  `ApplyOriginRunAsync`), not any per-transport push handler, so every transport is
  covered by one rejection.
- A tree **not locally enrolled** for replication is **dropped** (no dead-letter - a
  non-enrolled tree id is peer-controlled and parking it would let a peer spawn
  unbounded dead-letter activations).
- A tree that **is** enrolled but whose **wire merge-mode disagrees** with the
  locally-resolved mode is **dead-lettered** (bounded, safe to park).
- The local merge-mode is **always re-resolved locally**; the wire header's mode is
  never trusted. The mode is a per-batch header field, so classify once per run, not
  per entry.

### Identity-directory validation (`src/lattice.membership`, `src/lattice.api.auth`)
- Administrative membership-reference create paths (`UpsertGroupAsync`,
  `AddMemberAsync`) validate the supplied principal id against the identity
  directory when `LatticeIdentityDirectoryOptions.ValidationRequired` is set **and**
  a real provider is active (`DirectoryAvailable`, i.e. not `NullIdentityDirectory`).
- Rejection is fail-closed via `LatticeDirectoryValidationException` (an
  `ArgumentException`, so the gRPC layer maps it to `InvalidArgument` with no
  transport edit): an unresolvable id or a kind mismatch (User vs Group) is denied
  before any system-origin write.
- Ordering is fixed: authorize, then validate, then the system-origin write. Do not
  reorder validation after the write.

### Explorer web head (`src/lattice.explorer`)
- Per-user auth, connection, and credential services are **scoped** (per Blazor
  circuit), never singleton - a process-global auth session leaks one operator's
  credential to every circuit. When adding an Explorer service, confirm no singleton
  or hosted service captures the scoped auth/connection graph.
- The web head emits security response headers (content-security-policy,
  x-content-type-options, x-frame-options / frame-ancestors, referrer-policy, and
  the rest of the hardening set) via middleware on the Explorer branch, using
  static header values (no per-response allocation). Applies to both the standalone
  and the mountable/co-hosted host.

## Release-status note for security fixes

When labelling or writing changelog/PR prose for a change on these surfaces, judge
"breaking" by the affected package's **release status**, not by the change's
surface area. A behavioural or API change in a package that has **never shipped a
release tag** (currently `lattice.api.mcp`, `lattice.api.mcp.telemetry`, and the
`lattice.explorer` app, which is not a published package) cannot break an existing
consumer and is **not** breaking - it is an `enhancement`/`security` change. Reserve
the `breaking` label for a behavioural or API change to an **already-released**
package (verify with `git tag | Select-String <package>`) that alters previously
shipped behaviour. An opt-in change on a released package (guarded by a default-off
flag, like `ValidationRequired`) is additive, not breaking.
