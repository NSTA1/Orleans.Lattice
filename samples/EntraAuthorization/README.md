# Entra Authorization

> This sample authenticates a **real Microsoft Entra ID (Azure AD) user** against
> a single silo. For the same authorization layer driven by a self-contained demo
> token (no cloud setup, with a focus on nested-group membership), see the
> [Authorization](../Authorization/README.md) sample. For a multi-cluster
> deployment that converges policy and membership across sites, see the
> [Cross-Cluster Authorization](../CrossClusterAuthorization/README.md) sample.

## What it shows

The Orleans.Lattice authorization layer on **one in-process Orleans silo**, gated
by a genuine Entra identity. Where the [Authorization](../Authorization/README.md)
sample fakes a token, this sample acquires a real Entra access token for the user
who is currently signed in to the Azure CLI, resolves that token to a subject
through the shipped
[Entra authenticator](../../docs/lattice.membership.entra/README.md), and then
writes a value to a tree **as that Entra identity**.

The demo:

1. Acquires an Entra token for the signed-in `az` user (`AzureCliCredential`).
2. Starts a single silo: `AddLatticeMembership` + `AddEntraCredentialAuthenticator`
   + a default-deny `AddLatticeAuth` gate.
3. Resolves the token to a subject and prints the caller's Entra object id
   (`oid`).
4. Proves the gate is **fail-closed**: with no rule authored, the Entra user's
   write is denied.
5. As a bootstrap administrator, authors an allow rule for that exact `oid`.
6. Writes a value to the tree as the Entra user and reads it back.

## Prerequisites - run the Azure setup first

This sample **cannot run** until you have created an Entra app registration and
signed in with the Azure CLI. Follow the
[Entra ID setup guide](../../docs/lattice.membership.entra/entra-setup.md), which
walks the whole thing with `az` commands. In short:

1. `az login`
2. Create the app registration, expose an `access_as_user` scope, and
   pre-authorize the Azure CLI (all in the guide).
3. Export the ids the guide prints:

   ```powershell
   $env:LATTICE_ENTRA_TENANT_ID = "<tenant-guid>"
   $env:LATTICE_ENTRA_CLIENT_ID = "<app-client-id>"
   ```

If either variable is unset, or a token cannot be acquired, the sample prints the
setup steps and exits with a non-zero code rather than silently doing nothing.

## Run it

```
dotnet run --project samples/EntraAuthorization
```

## Expected output

With the setup complete and `az login` active (ids abbreviated):

```
Acquiring an Entra token for scope 'api://.../.default' via the Azure CLI... done.
Silo starting... ready.

== Resolve the signed-in Entra identity ==
  Resolved subject (oid): 8b1e...c4a2
  Groups in token:        (none)

== Fail-closed: write before any rule is authored ==
  write greeting/8b1e...c4a2 -> DENIED   (default-deny)

== Author an allow rule for this oid (as the bootstrap admin) ==
  Allowed Read|RangeRead|Write on tree 'entra-demo' for user '8b1e...c4a2'.

== Write a value to the tree AS THE ENTRA USER ==
  write greeting/8b1e...c4a2 -> allowed
  read  greeting/8b1e...c4a2 -> 'Hello from Entra oid 8b1e...c4a2 at 2025-... UTC'

[OK] wrote and read back a value under the signed-in Entra identity.
```

## When to use

- A single-cluster deployment that authenticates users with Microsoft Entra ID
  and enforces per-tree, per-prefix, or per-key authorization with a default-deny
  posture.
- As a template for wiring `AddEntraCredentialAuthenticator` into your own host
  and authoring rules against Entra object ids (or, with the optional groups
  claim, against Entra security groups).

## When not to use

- You just want to understand the authorization model without any cloud setup -
  use the [Authorization](../Authorization/README.md) sample, which runs entirely
  in-process.
- Multi-cluster deployments that must converge a single policy across regions -
  see [Cross-Cluster Authorization](../CrossClusterAuthorization/README.md).

## Notes on this sample

- The rule targets the caller's Entra `oid` directly, so no groups claim is
  required. The [setup guide](../../docs/lattice.membership.entra/entra-setup.md)
  has an optional step to add a groups claim if you want to author group rules
  instead.
- A tiny bootstrap-token authenticator (`SetupAuthenticator`) is used only to seed
  the first rule as a bootstrap administrator. It handles its own scheme only, so
  it never claims the real Entra bearer tokens, and only this sample process can
  stamp its credential.
- **Do not copy the `SetupAuthenticator` pattern into a real host.** It maps a
  plaintext token verbatim to the bootstrap-administrator subject id, which makes
  that id an unsigned bearer secret - fine for a one-shot, in-process demo,
  insecure anywhere untrusted can reach. A production deployment instead sets
  `BootstrapAdministrators` to a real, unforgeable identity (an Entra `oid`) and
  seeds rules by authenticating *as that identity* through the Entra
  authenticator - no trusted-token shortcut. Remember that a bootstrap
  administrator is cluster-wide god mode (every tree, every operation, exempt from
  strict fencing), so keep the set tiny and treat it as break-glass only. See the
  [security posture](../../docs/lattice.auth/security-posture.md#bootstrap-administrators-break-glass-root-of-trust)
  for the full trust model.
- **Why not just use the signed-in user's own oid as the bootstrap admin?** You
  could - and that is the more secure shape: the `oid` is already in the token
  before the host starts, so putting it in `BootstrapAdministrators` would let the
  same signed Entra token authenticate the seeding identity and remove the
  `SetupAuthenticator` entirely. This demo deliberately keeps a *separate* seeding
  identity because a bootstrap administrator bypasses the gate completely: if the
  Entra user were the bootstrap admin, it would be god mode from the first call,
  and the fail-closed default-deny step and the rule-driven enforcement acting on
  that same user could never be demonstrated. In a real host that does not need to
  demonstrate denial, prefer the own-`oid` approach.
- Enforcement reads a compiled policy snapshot that rebuilds off the policy-tree
  change feed, so the sample polls briefly after authoring the rule before the
  write succeeds.

## Feature docs

- [docs/lattice.membership.entra/entra-setup.md](../../docs/lattice.membership.entra/entra-setup.md) - the Azure CLI setup.
- [docs/lattice.membership.entra/README.md](../../docs/lattice.membership.entra/README.md) - the Entra authenticator.
- [docs/lattice/security.md](../../docs/lattice/security.md) - the security layer overview.
- [docs/lattice.auth/README.md](../../docs/lattice.auth/README.md) - policy, decisions, and enforcement.
