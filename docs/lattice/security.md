# Security: identity, authorization, and enforcement

Orleans.Lattice ships an **opt-in** security layer that turns an anonymous,
allow-all key-value store into an authenticated, authorized one. It is composed
of several small add-on packages that layer cleanly on top of the core library.
This page is the map: it explains what each capability does and links to the
package documentation that covers it in depth. Nothing here is enabled unless
the host registers it, and a cluster that registers none of it keeps the core
read/write path byte-for-byte unchanged (see
[Zero cost when absent](#zero-cost-when-absent)).

## The pipeline at a glance

A gated operation flows through four stages. Each stage is a separate package so
a deployment adopts only what it needs:

1. **Identity** turns the credential a caller presents into a stable *subject*
   (a subject id plus the transitive closure of the groups it belongs to).
2. **Authorization** compiles durable rules into a decision: given a subject, an
   operation, and a tree key or range, allow or deny.
3. **Enforcement** consults that decision on the core data path for every
   user-originated operation, fail-closed: a denied write throws and a denied
   read reports absent.
4. **External surfaces** project the same gated data and control planes to
   non-.NET callers, operators, and the Explorer - all authorizing through the
   very same gate, never a bespoke bypass.

## Identity and membership

[`Orleans.Lattice.Membership`](../lattice.membership/README.md) owns a durable,
introspectable directory of users and groups (with nested, group-in-group
membership) and a credential-to-subject resolution pipeline. Authentication is
pluggable: a credential is mapped to a principal by one or more scheme-selected
`ILatticeCredentialAuthenticator`s. The package ships an anonymous authenticator
and a built-in **JWT authenticator** registered per trusted issuer; a host can
register its own. Resolution is cached with a configurable TTL.

Two optional companions integrate a corporate identity provider:

- [`Orleans.Lattice.Membership.Entra`](../lattice.membership.entra/README.md) -
  a Microsoft Entra ID (Azure AD) credential authenticator. Its
  [Azure CLI setup guide](../lattice.membership.entra/entra-setup.md) provisions
  an app registration and shows the host wiring end to end.
- [`Orleans.Lattice.Membership.Entra.Graph`](../lattice.membership.entra.graph/README.md) -
  a Microsoft Graph-backed resolver for subjects whose group claims overflow the
  token.

Membership on its own adds identity resolution and the directory; it enforces
nothing until the authorization package is also registered.

## Authorization: policy and decisions

[`Orleans.Lattice.Auth`](../lattice.auth/README.md) adds the durable policy
store, the decision engine, and the enforcing access gate. Rules grant or deny a
set of operations to a subject selector (a user or a group) at a scope (a whole
tree, a key prefix, or a single key). When several rules match, the engine
resolves them deterministically: most-specific scope wins, deny overrides allow
within a tier, a user rule outranks a group rule at equal scope, and with no
matching rule the configured default effect applies. The recommended and default
posture is **default-deny**. A small set of **bootstrap administrators** forms
the root-of-trust that seeds the first rules and performs break-glass operations.

### Consistency: eventual by default, strict on request

Policy propagation is **eventually consistent** by default: a rule change takes
effect once the destination's compiled snapshot rebuilds off the updated policy
tree, which happens continuously in the background. A tree that needs a caller to
observe a policy change before its next operation can opt into a **strict epoch
fence** by naming the tree in the strict-consistency set; strict behaviour is
opt-in and off by default. The trade-offs are covered in the
[authorization README](../lattice.auth/README.md#consistency-modes).

## Enforcement on the data path

The core library exposes an access-gate seam that defaults to an allow-all null
gate. Registering the authorization package replaces it with the enforcing gate,
which every user-originated core operation consults. Enforcement is fail-closed
throughout: writes and deletes throw on denial, point reads of a denied key
report absent, and range scans prune to the authorized subset server-side. A
range delete under partial authorization is **hard-denied** rather than silently
narrowed, so a caller never deletes a subset while believing it deleted a range.
The State API honours the same read-access visibility when membership and
authorization are registered, so a browsing operator only sees the keys the
subject may read.

## External surfaces

Three transport-agnostic facades, each with a code-first gRPC binding, extend the
cluster to callers that do not embed the Orleans client. All of them inherit the
core gate rather than re-implementing authorization:

- [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) and its
  [gRPC binding](../lattice.api.auth.grpc/README.md) - the **control plane**:
  administer membership and policy, and explain decisions. Every operation is
  administrator-gated through the same enforcement primitive the data path uses,
  and `ExplainAsync` produces its verdict from the same gate, so an explanation
  can never disagree with what would actually be enforced.
- [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) - the write-capable
  external **data plane** for non-.NET clients. It routes every call through the
  gated `ILattice` surface, so per-tree and per-key rights are enforced
  automatically; a coarse transport-level authorizer that denies by default sits
  in front as an endpoint on/off switch.
- [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) - the read-only
  state-query surface, which honours the same read visibility.

The [Explorer](../lattice.explorer/connecting-to-an-auth-enabled-state-api.md)
runs an extensible login challenge against an auth-enabled State API endpoint,
and its sign-in mechanisms are a
[provider model](../lattice.explorer/adding-a-custom-auth-method.md) a host can
extend.

## Cross-cluster convergence

The membership directory and the policy store are ordinary `ILattice` trees, so
they replicate through the replication package's
[system-tree enrolment](../lattice.replication/system-tree-replication.md). This
is a **separate, explicit opt-in** from data replication: a rule authored or
revoked in one cluster converges to the others, in both state and enforcement.
Some deployments deliberately keep policy local (per-region security domains,
data-residency boundaries, or blast-radius containment); the trade-offs are
discussed in that document.

## Observability and audit

Every authorization decision, its latency, and the compiled-snapshot epoch and
age are published on a single meter, and an optional durable audit sink records a
decision trail. The full instrument catalogue, the audit-sink seam, and the
reserved subject-resolution-cache counters are documented in
[Authorization observability](../lattice.auth/observability.md).

## Security posture and cost

The [security posture](../lattice.auth/security-posture.md) page is the
authoritative reference for the threat model, the attack surface, the fail-closed
guarantees, the internal-grain trust boundary, TLS expectations, the
security-review findings with their resolutions, and the **measured
per-operation cost** of enforcement.

## Zero cost when absent

Every layer above is opt-in. The core registration installs only the allow-all
null gate, whose decision is a synchronously-completed, allocation-free allow
that never resolves a subject. A cluster that never registers authorization keeps
that null gate, so the data path is byte-for-byte what it was before the security
layer existed.

## Registration order

The layers must be registered in dependency order on the silo: the core lattice
first, then membership, then authorization, then any external facade. Each
add-on validates its prerequisites and fails fast at registration with an
actionable message rather than failing obscurely at silo start. The exact calls
are shown in each package README linked above.
