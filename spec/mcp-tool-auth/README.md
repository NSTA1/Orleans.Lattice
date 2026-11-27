# MCP tool authorization lock-step

This module checks the design of MCP tool authorization (issue #6052, epic
#6046): what a session's `tools/list` advertises, what its `tools/call` serves,
and how the two stay in lock-step through the shared
`McpToolAuthorizationGate` when the policy, the app registry or the caller's
request context change between the two calls. It is not a machine-checked
refinement proof of the C# implementation; the refinement note states which
production seams its detectors reach.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `McpToolAuthorization` | 8 | 0 | 7 | 23 | 12 | 32,268 |

## McpToolAuthorization

**Authorization is the F1 contract.** The module declares the abstract
operator of `spec/auth-decision/` (contract version 1) per policy snapshot,
`AllowedUnder(p, caller, op, scope)`, because the policy here changes between
advertisement and invocation; for each snapshot it is a contract `Allowed`.
The cfg binds it to `ContractAllowedUnder`, the contract's minimal grant-set
definition with one grant set per snapshot. Nothing here depends on how a
snapshot reaches its verdict, so the module holds for any definition
satisfying C1-C6 in every snapshot. The scopeless `Telemetry` follows C3 as
amended for #5582: a platform grant over one tenant's capability scope is a
contract allow, but the MCP telemetry tools stay cluster-wide only.

**Actors and actions.** Four callers: an operator, a data reader, a caller
holding `Telemetry` only over tenant A's capability scope, and an anonymous
caller. A caller opens a session (`OpenSession`) with an asserted active
tenant and a permission resolver that does or does not carry operation
detail, then calls the meta-tool (`CallMeta`), the region-discovery tool
(`CallRegions`), a data or telemetry group tool (`CallGroupTool`) or a tenant
app's tool (`CallAppTool`). Between calls the environment changes the policy
snapshot (`ChangePolicy`) or disables and re-enables the app's install
(`ToggleApp`). The registered authorizer is the default deny-all one, absent,
or a permissive host authorizer whose verdict follows the policy; each call
may arrive with or without a request context and with a malformed argument.

**Properties.**

- `LockStep`: under the snapshot it was built from, a session advertises
  only tools the design entitles its caller to, and all of them when the
  resolver carried operation detail; a call is served only when its tool was
  advertised and the design entitles the call under the snapshot current when
  it is made. Advertisement is never a capability.
- `OnlyMetaToolUngated`: every advertised tool but `lattice_capabilities`
  was admitted by the authorizer, and every served call but it passed the gate.
- `PerToolMinimum`: a group tool is advertised only when the resolver's
  carried operations meet its minimum, so an access set carrying no operation
  reaches no group tool, and a withheld tool is unreachable at `tools/call`.
- `ScopelessToolsClusterWide`: the MCP telemetry tools are advertised,
  carried and served only on cluster-wide `Telemetry`; a `Telemetry` grant
  over a tenant's capability scope never reaches one.
- `FailClosed`: an anonymous session is offered nothing; the default
  deny-all authorizer and an absent authorizer leave only the meta-tool; and
  no call without a request context is served past the meta-tool.
- `ActiveTenantValidated`: a tenant-scoped decision uses the asserted tenant
  only when `ITenantContextResolver` re-resolves it to itself; a refused
  assertion degrades the region list to the current region and is never
  echoed back.
- `DenialNotClientError`: a call the gate or the tool's own check refuses is
  answered as a denial, never as a client error.

**Bounds.** Two policy snapshots, two tenants, four callers, one session at a
time, three asserted-tenant headers, two data scopes and the cluster-wide
telemetry scope, one installed app and three authorizer postures. Every
property is a safety property over the open session or the last call, and a
policy change between a session's advertisement and its calls is reachable
in both directions, so each counterexample the mutations target is a few
steps long. There are no liveness properties.

## Refinement and mutations

- [Refinement note](Refinement.md), including the deliberate abstraction gaps.
- [Mutations](mutations/): every property and every behavioural action of
  `Next` has a falsifying mutation. `ChangePolicy` and `ToggleApp` are
  environment steps with no protocol behaviour of their own.
