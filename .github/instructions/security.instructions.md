---
applyTo: "src/lattice.api.mcp/**,src/lattice.api.mcp.apps/**,src/lattice.api.mcp.repocontext/**,src/lattice.api.mcp.repocontext.replication/**,src/lattice.api.mcp.telemetry/**,src/lattice.api.mcp.telemetry.azure/**,src/lattice.explorer/**,src/lattice.explorer.entra/**,src/lattice.explorer.entra.web/**,src/lattice.replication/**,src/lattice.replication.grpc/**,src/lattice.membership/**,src/lattice.membership.entra/**,src/lattice.membership.entra.graph/**,src/lattice.membership.oidc/**,src/lattice.api.auth/**,src/lattice.api.auth.grpc/**,src/lattice.auth/**,src/lattice.api.replication/**,src/lattice.api.replication.grpc/**,src/lattice.api.telemetry/**,src/lattice.api.telemetry.grpc/**,src/lattice.apps/**,src/lattice.api.apps/**,src/lattice.api.apps.grpc/**"
---

# Security Boundaries and Invariants

These are load-bearing security invariants for the auth, membership, replication,
telemetry, MCP, Explorer, and installable-app control surfaces. They were established by the v8 security
hardening epic (#1270, sub-issues #1264-#1269). Do not regress them, and apply the
cross-cutting principles below to any new code on these surfaces. When you touch a
seam named here, re-read the invariant before changing it.

## Which packages this file governs

The `applyTo` list in the front matter names **one entry per package directory**,
because the glob matches path segments: `src/lattice.api.mcp/**` descends into
`src/lattice.api.mcp/`, and does **not** match the sibling directory
`src/lattice.api.mcp.repocontext/`. The dots read as nesting to a human and as an
ordinary character to the matcher, so every member of a governed family has to be
listed by name.

**The list is therefore not self-maintaining.** A package added to a governed
family tomorrow is ungoverned the moment it is created, and the failure is silent:
nothing is red, the instructions simply never attach. `SecurityInstructionsCoverageTests`
(in `test/lattice/Hygiene/`) is what makes that loud - it enumerates `src/`, and
fails the build for any directory whose name extends a listed package name by a dot
suffix without itself being listed. When you add such a package, add it here; the
gate will tell you if you forget.

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
  default `DenyAllMcpAuthorizer` means tools are denied until a host explicitly opts
  in a permissive authorizer. This is secure-by-default; do not add an implicit
  allow fallback.
- The `lattice_capabilities` meta-tool is the only ungated advertisement; do not
  widen the ungated set.
- Discovery must not advertise a capability the caller does not hold. The two
  scopeless operations, which name no tree (`LatticeOperation.Telemetry` and
  `LatticeOperation.AppInstall`), are carried only from an Allow rule written at
  cluster-wide scope - a tree-kind scope on `LatticeScope.ClusterWideTreeId`, as
  `LatticeScope.ClusterWide()` writes it - never from a rule scoped to one tree, and
  never from a key- or prefix-kind rule on the cluster-wide tree id, none of which
  can confer a scopeless capability at the gate (#3645, #3863, #4082). Inside a
  group the caller may use, the reported operations also set a per-tool minimum,
  applied unconditionally: a tool is withheld when the caller holds none of the
  operations it requires, and an access set that carries no granted operation
  reaches no tool at all, because missing evidence is a denial (#4082). So a caller
  holding only a read grant is offered neither a mutating data tool (#3863) nor a
  mutating repository-context tool.
- An asserted active tenant is validated, never trusted. The region catalog
  re-resolves the caller-supplied assertion (the `lattice-active-tenant` header by
  default) through `ITenantContextResolver` - the validating seam the data plane
  uses - and honours it only when the resolved tenant matches; a refused or
  unresolvable assertion degrades to the current region alone and is never
  echoed back (#3645).
- Client-error text is untrusted. Every client-error message a tool call raises -
  a missing, malformed or unknown argument, refused content, an unknown record -
  is sanitized once, at the credential-stamping tool every call funnels through,
  before it is echoed to the caller or written to the log: control characters and
  the Unicode line and paragraph separators are replaced and the text is truncated
  to a fixed length, so caller content can neither forge a log record nor choose
  its size (#4056). Unknown argument names are further reduced to a safe character
  set, truncated, and capped in number (#3972). An authorization denial is never
  marked as a client error; it surfaces as a denial.

### Telemetry metric-name allow-list (`src/lattice.api.telemetry`, consumed by `src/lattice.api.mcp.telemetry`)
- The PromQL `__name__` / metric-name allow-list fails closed: an unparseable,
  ambiguous, or non-exact-match `__name__` matcher is treated as **not** on the
  allow-list (deny), never as a bypass. Label-matcher parsing must not offer a path
  that evades the allow-list.
- A `*`-wildcard allow-list entry admits a name only by whole-name match: it is
  anchored with `\z` rather than `$` (which also matches before a trailing newline)
  and compiled without `Singleline`, so a name carrying a newline is refused in any
  position, and it compiles with `RegexOptions.NonBacktracking` because the names
  it tests are caller-supplied (#3929).
- Match `__name__` label names via span comparison; only allocate a substring on the
  actual matched-name path, never for every in-brace label.
- The allow-list (`TelemetryMetricAccessPolicy`) and the PromQL matcher parsing
  (`PromQlMetricExtractor`) live in the telemetry facade and are applied through
  `TelemetryQueryAuthorizer`. The MCP telemetry tools and the facade's query catalogue
  both authorize through that one authorizer, so harden it there, not in a binding.
- The allow-list narrows; it never authorizes. Its default `ReadAll` posture admits
  every metric name, so every MCP telemetry tool first checks the cluster-wide
  `LatticeOperation.Telemetry` capability through
  `TelemetryAccessAuthorizer.AuthorizeClusterTelemetryAsync` - the seam the
  transport-neutral `ILatticeTelemetry` facade also consults - before any backend
  call or range-guardrail evaluation, and applies the allow-list only after it
  (#3645). Discovery decides only which tools are offered; a tool gated only there
  is not gated at all, because a tool name is guessable.

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
- Inbound per-peer contact (`ReplicationPeerStats`, the inbound gauges and the
  peer-status report) is recorded only for a run the gate **admitted**, through the
  same rule (`ReplicationInboundAdmission`) the gate uses, on every receive path
  including the dead-letter decorator's single-entry and per-entry branches (#4021).
  The inbound rows are capped, because an admitted run's origin id is still the
  peer's own claim; a new pair beyond the cap is not recorded.

### Replication origin binding (`src/lattice.replication`, `src/lattice.replication.grpc`)
- A body-declared origin cluster id is never an authorization input on its own.
  The gRPC receiver refuses a data-plane call that consumes one (push,
  content-manifest exchange, peer high-water-mark read), and every saga control
  call, unless the transport-stamped origin header is present and names the same
  cluster; an absent header is refused rather than tolerated (#3893).
- `LatticeReplicationSecurityOptions.BindCredentialToOriginCluster` defaults to
  `true` (#4082). With it on, the receiver's gRPC authentication interceptor also
  requires the presented credential to equal the secret this cluster would itself
  use to call the claimed origin
  (`ILatticeReplicationSecretSource.GetOutboundSecretAsync`), so the claimed origin
  is authenticated rather than self-asserted - matching the flat accepted-secret set
  proves only that the caller holds some accepted secret. A missing origin, an
  origin with no configured secret, and a mismatch are one indistinguishable
  `PermissionDenied`, compared in constant time. An estate running an asymmetric
  per-peer secret scheme must set it to `false`, and must then not rely on a claimed
  origin for any security decision.

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
- A sign-in is bound to the endpoint it was minted for. Repointing the console at a
  different endpoint signs out and clears the stored credential rather than
  re-applying it, and an endpoint that is not recognisably the same counts as
  different. The clear must not rest on deleting the credential cookie, which a
  Blazor circuit cannot do once its response has started: the cookie credential
  store revokes the presented value and refuses a revoked value on read (#3800).
  That store is a singleton holding no credential in memory - it reads each
  browser's own encrypted cookie from the ambient request - and its revocation set
  must stay process-wide, because a per-circuit set would forget the revocation on
  the next launch. Because that set is also bounded and process-local - a restart,
  a second web head, or an eviction loses it - the store additionally stamps the
  endpoint a credential was minted for inside the protected cookie payload and
  refuses on read a credential whose stamp is not recognisably the endpoint now
  configured, failing closed when no endpoint can be resolved; and only a value the
  store itself minted is admitted to the bounded revocation set, so fabricated
  logout posts cannot evict a genuine revocation (#3972).
- The web head emits security response headers (content-security-policy,
  x-content-type-options, x-frame-options / frame-ancestors, referrer-policy, and
  the rest of the hardening set) via middleware on the Explorer branch, using
  static header values (no per-response allocation). Applies to both the standalone
  and the mountable/co-hosted host.

### Installable apps (`src/lattice.apps`, `src/lattice.api.apps`, `src/lattice.api.apps.grpc`, `src/lattice.api.mcp.apps`)
- `LatticeOperation.AppInstall` is a scopeless, cluster-wide capability. It is
  deliberately excluded from `LatticeAuthOperations.All`, it is never inherited from a
  permissive data-plane default, and - apart from the root-of-trust
  `LatticeAuthOptions.BootstrapAdministrators`, who are allowed every operation - it is
  granted only by an Allow rule written over `LatticeScope.ClusterWide()`, so a
  whole-data-plane grant never confers the authority to install, upgrade, or uninstall
  an app. The app role compiler never emits it (or `LatticeOperation.Telemetry`) as a
  role operation; a manifest role that asks for one is reported as an operations excess
  whatever the install ceiling allows.
- Every verb of the app control facade (`ILatticeAppsControl`), the read verbs
  included, authorizes `AppInstall` over the cluster-wide scope through the shared
  access gate before it touches registry, source, or activation state, so a denied
  caller learns nothing about which apps exist. The catalogue facade
  (`ILatticeAppCatalog`) and the role re-binding facade (`ILatticeAppRoleBindings`)
  apply the same gate, and the control and catalogue facades' capability probes run
  it too, reporting the outcome as a flag instead of throwing. The registry and
  activation-status reads beneath them are ungated in-process surfaces, so the gate
  belongs at the facade; do not add a verb that reaches them first.
- The per-user workspace facade (`ILatticeAppWorkspace`) is gated per caller instead:
  every verb requires the caller to match at least one app-owned compiled rule of an
  enabled install in the active tenant, fails closed on a missing membership context
  or an unresolved tenant, and answers a caller without a grant exactly as it
  answers for an app that does not exist. Consent, capability ceilings, approved
  exception scopes and role-to-group bindings stay behind `AppInstall`.
- Rule ids in the app-owned namespace (`LatticeAppRuleIds.Prefix`, `app:`) are written
  only from inside a system-origin scope, by the app activation path persisting the
  rules the role compiler produced. The authorization
  policy store rejects any other write or delete of such an id with
  `LatticeAppOwnedRuleException` before it issues any read or write, so a rejected
  delete does not disclose whether the rule exists.
- App MCP tools are gated per tool on the declared role's compiled grants, and the
  same evaluation runs when a tool is advertised and again when it is invoked, against
  the current registry snapshot - the lock-step rule of the MCP surface above. Only an
  unfiltered allow holds a role's operation on its scope; a key-filtered decision
  fails closed, on a prefix scope exactly as on a tree scope (#3863).
- Alias changes are bounded by tree ownership. The tree registry consults the core
  `ITreeOwnershipGuard` seam on every alias assignment - system-origin maintenance
  included - after its namespace and target-control checks: a denial throws
  `LatticeTreeOwnershipDeniedException`, a default decision denies, and a guard
  failure propagates without writing the alias. The apps package backs the seam
  with its tree ownership ledger (the reserved `sys-app-trees` tree), so for every
  caller an alias is allowed only when the logical tree and the tree its physical
  target derives from (or the target itself) have the same owner - the same
  install, or no app at all; a host without the apps package gets an allow-all
  guard. Do not add an alias write that bypasses the guard.
- The gRPC binding is default-deny: `DenyAppsApiAuthorizer` is registered unless the
  host supplies its own `ILatticeAppsApiAuthorizer`.
- An install is pinned to what the operator reviewed. Every `AppDescriptor` carries
  the `ManifestDigest` of the manifest and provenance it describes, and an install or
  upgrade whose `AppInstallRequest.ExpectedManifestDigest` differs from the digest of
  the manifest re-resolved at commit is refused before anything is recorded (#4021).
  A review-then-install client must send the digest; the Explorer does.

## Release-status note for security fixes

When labelling or writing changelog/PR prose for a change on these surfaces, judge
"breaking" by whether the change alters **previously shipped behaviour**, not by the
change's surface area. Most packages in the family have shipped a release tag, but not
all: `lattice.api.mcp.repocontext` and `lattice.api.mcp.repocontext.replication` are
still unreleased, as are the installable-app packages (`lattice.apps`,
`lattice.api.apps`, `lattice.api.apps.grpc`, and `lattice.api.mcp.apps`) and the
`Orleans.Lattice.Explorer.AppKit` package built from
`src/lattice.explorer/` (see
`PACKAGES.md`), and `lattice.membership.oidc`, `lattice.api.telemetry`, and
`lattice.api.telemetry.grpc` first shipped at 9.5.0. So verify a package's shipped
versions with
`git tag | Select-String <package>` and reserve the `breaking` label for a behavioural
or API change that alters behaviour a released version already exposed. An opt-in
change (guarded by a default-off flag, like `ValidationRequired`) is additive, not
breaking, and an additive hardening change (a new response header, a new fail-closed
gate that no prior version promised to leave open) is an `enhancement`/`security`
change, not breaking.
