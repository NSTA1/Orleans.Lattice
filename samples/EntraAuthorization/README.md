# Entra Authorization

> This sample authenticates a **real Microsoft Entra ID (Azure AD) user** against
> a single silo. For the same authorization layer driven by a self-contained demo
> token (no cloud setup, with a focus on nested-group membership), see the
> [Authorization](../Authorization/README.md) sample. For a multi-cluster
> deployment that converges policy and membership across sites, see the
> [Cross-Cluster Authorization](../CrossClusterAuthorization/README.md) sample.

## What it shows

The Orleans.Lattice authorization layer on **one in-process Orleans silo**, gated
by a genuine Entra identity where **the signed-in user is the tree's owner**.
Where the [Authorization](../Authorization/README.md) sample fakes a token, this
sample acquires a real Entra access token for the user who is currently signed in
to the Azure CLI, makes that user's Entra object id (`oid`) the sole bootstrap
administrator, and then reads and writes a value to a tree **as that Entra
identity** - while an anonymous request is denied.

The demo:

1. Acquires an Entra token for the signed-in `az` user (`AzureCliCredential`) and
   reads its `oid` from the token.
2. Starts a single silo: `AddLatticeMembership` + `AddEntraCredentialAuthenticator`
   + a default-deny `AddLatticeAuth` gate whose **only bootstrap administrator is
   the caller's own `oid`** - no trusted-token authenticator.
3. Resolves the token to a subject and confirms it is the owner.
4. As the Entra user (the owner), writes a value to the tree and reads it back -
   allowed.
5. As an anonymous request (no credential), attempts the same write and read and
   watches the default-deny gate reject them.

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
Signed-in Entra object id (oid): 8b1e...c4a2
  -> this oid is the tree owner (sole bootstrap administrator).

Silo starting... ready.

== Resolve the signed-in Entra identity ==
  Resolved subject (oid): 8b1e...c4a2  (owner)

== As the signed-in Entra user (owner) ==
  write greeting/8b1e...c4a2 -> allowed   (owner: allowed)
  read  greeting/8b1e...c4a2 -> 'Hello from Entra oid 8b1e...c4a2 at 2025-... UTC'

== As an anonymous request (no credential) ==
  write greeting/8b1e...c4a2 -> DENIED   (default-deny)
  read  greeting/8b1e...c4a2 -> (absent)   (soft-denied)

[OK] the owner wrote and read a value; the anonymous request was denied.
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

- **The signed-in user is the owner.** The sample reads the caller's Entra `oid`
  from the token *before* the host is built and puts exactly that `oid` in
  `BootstrapAdministrators`. The same signed Entra token that authenticates the
  caller therefore also authorizes them - there is no trusted-token authenticator
  and no separate seeding identity. This is the recommended production shape: bind
  the bootstrap administrator to a real, unforgeable identity rather than mapping a
  plaintext token to an admin id.
- **A bootstrap administrator is cluster-wide god mode.** It bypasses the gate for
  every tree and every operation and is exempt from strict fencing, so in a real
  deployment keep the set tiny and treat it as break-glass only. See the
  [security posture](../../docs/lattice.auth/security-posture.md#bootstrap-administrators-break-glass-root-of-trust)
  for the full trust model. Beyond the break-glass owner, author ordinary
  [allow rules](../../docs/lattice.auth/README.md) for day-to-day access instead of
  adding more bootstrap administrators.
- **Anonymous is genuinely denied.** The anonymous request stamps no credential, so
  membership resolves it to the well-known anonymous subject, which the default-deny
  gate authorizes for nothing - the fail-closed contrast to the owner.
- Rules can target an Entra `oid` directly (as the owner grant here does implicitly)
  or, with the optional groups claim, an Entra security group. The
  [setup guide](../../docs/lattice.membership.entra/entra-setup.md) has an optional
  step to add the groups claim.

## Feature docs

- [docs/lattice.membership.entra/entra-setup.md](../../docs/lattice.membership.entra/entra-setup.md) - the Azure CLI setup.
- [docs/lattice.membership.entra/README.md](../../docs/lattice.membership.entra/README.md) - the Entra authenticator.
- [docs/lattice/security.md](../../docs/lattice/security.md) - the security layer overview.
- [docs/lattice.auth/README.md](../../docs/lattice.auth/README.md) - policy, decisions, and enforcement.
