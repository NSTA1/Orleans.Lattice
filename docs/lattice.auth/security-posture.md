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
| Control plane (external) | The admin API (`ILatticeAuthAdmin`) | Every auth-admin call, reads included, is authorized against the reserved authorization namespace, which is fail-closed (see finding A2). |
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
Orleans runtime's point of view, but it is inside the trust boundary. The
Orleans hosted-client grain id prefix (`hosted-`) alone is not a trust signal:
the caller's source id is client-supplied and unvalidated, so an external client
can announce an arbitrary `hosted-*` id. Recognition therefore requires the
`hosted-` prefix **plus** validation that the silo address embedded in the id is
either this silo's own address or a currently active member of the local
cluster. Only then does the filter exempt the call from stripping and let it
establish the internal capabilities it legitimately needs; a forged `hosted-*`
id whose embedded address is not a live cluster silo is treated as an external
client and has its forged capabilities stripped.

## Bootstrap administrators (break-glass root of trust)

`LatticeAuthOptions.BootstrapAdministrators` is a set of **subject ids** that the
gate treats as a break-glass root of trust. A request whose resolved subject id
is in the set is short-circuited to allow *before* the decision engine is
consulted, so it works even against a cold or empty policy snapshot. It exists
for one reason: to stop a deployment locking every operator out of the
authorization tree itself. Under the recommended deny-by-default posture the
reserved control-plane namespace is forced closed (see finding A2), so with no
rules yet authored nobody could seed the first one. A bootstrap administrator
seeds that first policy and repairs a misconfiguration that would otherwise be
unrecoverable.

Its security properties, and the constraints that keep it safe, are worth stating
plainly:

- **It is only as strong as the authenticator that resolves the subject.** The
  gate matches `request.Subject.SubjectId`, which is the output of the membership
  resolution pipeline, not a string the caller supplies to the gate. Binding a
  bootstrap id to an unforgeable, cryptographically-validated identity (for
  example a Microsoft Entra `oid` from a signed token) means impersonating it
  requires forging a signed token. Binding it to an identity minted by a
  trusted-token authenticator that maps a plaintext token verbatim to a subject
  id (as the shipped samples do for brevity) turns the bootstrap id into an
  unsigned bearer secret - acceptable for an in-process demo, **never** for a
  deployment reachable by anything untrusted.
- **The bypass is cluster-wide god mode, not just policy repair.** A bootstrap
  administrator is allowed every operation on every tree - the data plane as well
  as the control plane - and is exempt from strict epoch fencing. It is a
  break-glass identity, not a day-to-day admin role. Grant ordinary
  administrative rights through explicit rules; reserve the bootstrap set for
  recovery.
- **Keep the set as small as possible.** It is empty by default. Every id in it
  is a full-cluster master key, so the set should hold the smallest possible
  number of break-glass operator identities, be sourced from a strong identity
  provider, and be audited. The option is live-reloadable, so an id can be added
  for a recovery window and removed again afterwards.

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

