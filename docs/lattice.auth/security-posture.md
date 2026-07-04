# Security posture

This page describes the security posture of the Orleans.Lattice authorization
layer (`Orleans.Lattice.Auth` and the identity packages it builds on). It
summarises the threat model, enumerates the attack surface, states the
fail-closed guarantees the layer makes, defines the trust boundary for internal
grain calls, sets out the transport-security expectations, and records the
findings from a full security and design review together with their resolutions.

The review that produced this page, and the regression coverage that locks the
fixes in, are tracked in
[issue #1103](https://github.com/NSTA1/Orleans.Lattice/issues/1103).

## Threat model summary

The authorization layer defends a lattice cluster against callers that should
not be able to read or mutate data, or to reconfigure the authorization control
plane. The assets under protection are:

- **Tree data** - the key/value entries in each logical tree, including the
  entries surfaced through views.
- **The authorization control plane** - the membership graph (users, groups,
  memberships) and the policy rule set that together decide who can do what.
- **Existence metadata** - the mere fact that a given tree, view, or key exists
  can be sensitive, so the catalog and read surfaces must not leak it to a
  caller who cannot read the underlying data.

The layer assumes the following trust model:

- **Silos are trusted.** Every silo in the cluster is inside the trust boundary.
  Code running on a silo may establish internal capabilities (system-origin,
  view scopes, the internal-origin marker) because it is already trusted.
- **External clients are untrusted.** A caller reaching the cluster through the
  external data API (the gRPC state/data gateway) or as a plain Orleans client
  is untrusted. Its identity is whatever the registered credential authenticator
  resolves from its presented credential, defaulting to the anonymous subject
  when no valid credential is presented.
- **The Orleans clustering transport is a trust boundary, not an authorization
  boundary.** A party that can already issue arbitrary in-cluster grain calls is
  inside the trust boundary. The defense-in-depth internal-origin assertion (see
  below) hardens against a direct in-cluster grain call that skips the facade,
  but the primary control against external callers is that the external gateway
  can only reach the public facade grain.

## Attack surface

| Surface | Entry point | Enforcement |
|---|---|---|
| Data plane (external) | gRPC state / data API to the `ILattice` facade grain | The facade calls the registered access gate on every operation. |
| Control plane (external) | The admin API (`ILatticeAuthAdmin`) | Every mutating admin call is authorized against the reserved authorization namespace, which is fail-closed (see finding A2). |
| Read catalog (external) | State-API catalog and structure endpoints | Existence of a tree/view/key is hidden from a caller who cannot read the underlying source (see finding A1). |
| Explorer (operator tool) | The Explorer's gRPC client to the state API | Credentials only attach over a transport gRPC can confirm is secure (see finding A3). |
| Internal grain calls | Direct in-cluster calls to the shard / leaf grains | Defense-in-depth internal-origin assertion (see finding A4). |
| Credential smuggling | Reserved `RequestContext` capability keys | The capability-stripping incoming call filter re-derives every internal capability from the real caller identity on each hop (see below). |

## Fail-closed guarantees

The layer is designed to deny by default and to fail closed on every mutating
path:

- **Recommended default effect is deny.** With the recommended configuration an
  operation that matches no allow rule is denied.
- **The control plane is deny-by-default regardless of the data-plane default.**
  Even when the data-plane default effect is configured to allow, an unmatched
  decision in the reserved authorization namespace resolves to deny, so only a
  bootstrap administrator (or an explicitly modelled grant) is ever an
  administrator. See finding A2.
- **Denied mutations leave no partial state.** A denied single-key write,
  delete, range delete, CRDT apply, batch write, atomic multi-key write, or bulk
  load throws before any leg of the operation is applied. The adversarial
  regression suite asserts this fail-closed property for every operation class.
- **Existence is hidden on a read-around.** A caller who cannot read a tree's
  source data receives an empty or not-found result from the read and catalog
  surfaces, and cannot distinguish "exists but I cannot read it" from "does not
  exist". See finding A1.

## Trust boundary for internal grain calls

All access-gate enforcement lives on the `ILattice` facade grain. The physical
shard and leaf grains it delegates to enforce no policy of their own, so a direct
in-cluster grain call to a shard or leaf key would otherwise bypass policy.

Two mechanisms harden this boundary:

1. **The capability-stripping incoming call filter.** A silo-wide incoming grain
   call filter re-derives the internal capability markers from the real caller
   identity on every hop. The reserved `RequestContext` capability keys - the
   system-origin gate-bypass marker, the view read/write scopes, the
   internal-origin marker, and the replication / maintenance origin markers - are
   the signals that cause access-gate enforcement to be skipped without any
   authentication, so an external caller must never be able to assert them. The
   filter strips every such key from a call that arrives from a genuine external
   Orleans client, and stamps a fresh internal-origin marker (re-derived, never
   trusted from the wire) on a call that is silo-sourced or comes from this
   cluster's own in-silo hosted client. A malicious client that manually seeds
   the system-origin or internal-origin marker on its outbound `RequestContext`
   therefore cannot smuggle a forged capability into a grain call.

2. **The shard / leaf internal-origin assertion.** The shard and leaf mutation
   entry points assert that the current turn carries the internal-origin marker
   (established only inside the trust boundary). A direct external client call
   carries no such marker - any forged one having been stripped by the filter -
   and is refused. The assertion is keyed on the presence of the filter (a
   sentinel the authorization layer registers beside it), so it activates exactly
   when the filter that establishes the marker is present. A no-auth cluster, or
   a cluster that registers a custom access gate without the filter, never sets
   the marker and pays nothing.

### In-silo hosted client discriminator

The in-silo hosted client that in-silo infrastructure uses (for example the
co-hosted gRPC gateway and the authorization initializer) is a client from the
Orleans runtime's point of view, but it is inside the trust boundary. It is
distinguished from a genuine external client by the Orleans hosted-client grain
id prefix, so the filter exempts it from stripping and lets it establish the
internal capabilities it legitimately needs, while still stripping forged
capabilities from genuine external clients.

## Transport-security (TLS) expectations

- **Credentials only flow over a transport the runtime can confirm is secure.**
  The Explorer's gRPC client, and the replication transports, only lift the
  gRPC insecure-channel safeguard for an endpoint that is genuinely plaintext
  (an `http` address) and only when the operator has explicitly opted into
  unencrypted transport. For an `https` endpoint the safeguard stays active and
  credentials still attach over the confirmed-secure channel. See finding A3.
- **Production deployments should terminate TLS at or before the cluster's
  external endpoints.** The plaintext opt-in exists for local development and for
  deployments that terminate TLS at a trusted proxy; it should not be enabled on
  an endpoint that is reachable by an untrusted network.

## Enforcement cost

The enforcement path is opt-in: with Membership and Auth not registered,
`LatticeGrain` resolves a null access gate and the enforcement path
short-circuits with no subject resolution and no auth code on the hot path, so
the disabled build is byte-for-byte the pre-feature baseline. The cost below is
what a caller pays only once a default-deny policy and a membership context are
registered.

The numbers come from the microbenchmark harness under
`benchmark/host/Bench.Microbench`, run in a disabled-vs-enabled matched pair.
The enabled config wires a real default-deny `PolicyAccessGate` plus a
fixed-subject membership context into every grain, with a representative
tree/key/prefix allow ruleset for the benchmarked subject. The full report,
methodology, and raw JSON are committed under
`benchmark/host/Bench.Microbench/auth-f162/` (`report.md` plus the matched-pair
`*.json`). The harness ran on a shared machine, so the honest way to read the
result is: **allocation delta is the robust signal; latency delta is dominated
by environment noise.**

**Allocation (the robust signal).** The added allocation splits into two clean
patterns:

- **Flat per-call cost, about +1.5 KB per operation.** Single-key operations
  (write, read, delete, get-or-set, CAS, apply-delta) and range/scan reads
  (key scan, entry scan, predicate scan) evaluate the gate once. They add
  roughly one decision plus subject-resolution allocation regardless of how many
  keys the operation touches or returns. A 4-shard range scan adds the same
  about +1.6 KB whether it returns a handful of keys or a full page.
- **Per-entry cost, about +1.4 KB per key, on batch writes.** Bulk load,
  multi-key `SetMany`, and atomic set-many evaluate the gate for each entry, so
  the added allocation scales with the batch size: a bulk load adds about
  +1.2 MB, a 4-shard `SetMany` about +1.3 MB, and a 64-key atomic set-many about
  +93 KB. This is the dominant enforcement cost for large batch writes and is
  the figure to size capacity against; single operations pay only the flat cost.

**Latency.** On the shared benchmark machine the gate-independent control group
(pure CRDT and version-vector merges, which never run the gate) showed a
run-to-run noise band of about +/-68.5% at the 95th percentile. Most gated
single-key operations landed within about x1.0 to x1.2 of the disabled build,
which is inside or near that band; a few gated operations measured nominally
faster when enabled, which is noise rather than a real speed-up. The one latency
signal that stands clear of the band is the multi-shard batch write: a 4-shard
`SetMany` and a bulk load both measured roughly 2x the disabled latency,
consistent with the per-entry allocation cost above. For precise latency figures
the harness should be re-run on a quiet machine; the wiring is kept in place
(the enabled config is selected by an environment switch documented in
`report.md`) so the run is reproducible.

**Gate-independent paths pay nothing.** The control group's allocation delta was
0 B, confirming that operations the policy does not guard (and, by construction,
the entire disabled build) carry no enforcement overhead.

## Findings and resolutions

The following findings were confirmed by the review and resolved. Each has a
named regression test that fails without the fix and passes with it, plus
adversarial negative-path coverage.

### A1 - View-read must not bypass per-caller read authorization (high)

**Finding.** The State-API view-read paths opened a view-read scope that bypassed
the access gate without first checking that the caller could read the view's
*source* tree, so a caller with no read grant on the source could read the view.
The sibling view-listing path already performed that check; the entry-read paths
omitted it.

**Resolution.** For a view tree the read paths now resolve the view's source tree
id and require that the caller can read the source (and deny anonymous callers)
before opening the view-read scope, mirroring the view-listing path. Existence of
a tree, view, or key is also hidden from a caller who cannot read the underlying
source across the catalog and structure surfaces. Because a source tree that
carries any per-key rule yields an allow-shaped decision for every caller, a
structural "has any grant" probe is used to distinguish a partial/prefix grant
(visible) from no grant at all (hidden), rather than relying on the per-request
decision.

### A2 - Control plane must not inherit a permissive data-plane default (high)

**Finding.** No rule can be scoped at the reserved authorization namespace, so no
rule ever matched the policy tree. Under a permissive data-plane default effect
the unmatched admin request resolved to allow, so any caller (including
anonymous) could rewrite membership and policy.

**Resolution.** The control-plane decision is now independent of the data-plane
default effect. In the gate, an unmatched decision whose target is in the
reserved authorization namespace resolves to deny regardless of the data-plane
default, so only a bootstrap administrator (or an explicitly modelled grant) is
ever an administrator. The bootstrap-administrator path and the recommended
deny-by-default data-plane behaviour are both preserved, and normal data-plane
reads and writes under a permissive default are unaffected.

### A3 - Explorer must not disable the insecure-channel safeguard on TLS (medium)

**Finding.** The Explorer's gRPC client lifted the gRPC safeguard that refuses to
send call credentials over a non-TLS channel for every connection, including TLS
ones, whenever a credential provider was configured.

**Resolution.** The safeguard is now lifted only when the endpoint is genuinely
plaintext (an `http` address) and the operator has opted into unencrypted
transport, mirroring the replication transports. On an `https` address the
safeguard stays active and credentials still attach over the confirmed-secure
channel.

### A4 - Internal shard / leaf grains do no gate enforcement (medium, defense-in-depth)

**Finding.** All gate enforcement lives on the facade; the shard and leaf grains
it delegates to enforce nothing, so a direct in-cluster grain call to a shard or
leaf key would bypass policy. This is bounded by the Orleans clustering trust
boundary - the external gRPC data API funnels through the facade and cannot reach
these grains directly.

**Resolution.** The shard and leaf mutation entry points now assert the
internal-origin marker described above, which is established only inside the trust
boundary and is stripped from any call arriving from an external client. A direct
external call without the marker is refused; a legitimate facade-to-shard-to-leaf
call carries the marker and passes. Replication-apply, structural maintenance,
the atomic-write saga, and bulk-load all run inside the trust boundary and are
unaffected.

## Residual risks and deviations

- **The caller credential is intentionally not stripped.** The capability-
  stripping filter strips the reserved bypass/capability keys but deliberately
  leaves the caller-credential context in place. The credential is an
  authentication input, not a bypass capability: the silo always re-validates it
  through the registered credential authenticator, so a client that forges a
  credential resolves to the anonymous subject rather than escalating. Stripping
  it would break the supported client-side credential-assertion API.
- **The "has any grant" existence probe is deliberately coarse.** The structural
  probe used for existence-hiding answers "does this subject have any resolved
  allow on this tree". It walks the compiled policy tiers and is off the hot read
  path. A tree-wide allow paired with an equal-scope tree-wide deny that wins is
  an acceptable corner where the probe may report visible; this affects only
  existence-hiding, never the per-request read decision, which remains fully
  enforced.
- **The internal-grain assertion is defense-in-depth, not the primary control.**
  The primary control against external callers is that the external gateway can
  only reach the facade grain. The internal-origin assertion hardens against a
  direct in-cluster grain call that skips the facade; it does not replace network
  and clustering controls on who may join the cluster or issue in-cluster calls.
