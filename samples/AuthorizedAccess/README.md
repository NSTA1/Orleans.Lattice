# Authorized Access

## What it shows

The Orleans.Lattice authorization layer end to end: membership (users and
groups), a default-deny policy, and per-tree / per-key / per-prefix rules
enforced on every operation. It stands up **two independent in-process Orleans
clusters** (`site-a` and `site-b`) wired together with Orleans.Lattice
replication over gRPC, and enrols the reserved membership and policy system
trees into that replication (the system-tree replication special case) so an
authorization change on one cluster converges onto the other.

The demo runs in four acts:

1. **Membership.** Create users (`alice`, `bob`, `carol`) and groups
   (`line-operators`, `auditors`), and place `alice` and `bob` in groups.
2. **Rules and enforcement.** Author three rules under a default-deny policy -
   a prefix grant (operators read/write/delete/range the `station/` subtree), a
   key grant (only `alice` may read/write the single `config/threshold` key),
   and a tree grant (auditors read the whole tree). Then exercise write, delete,
   and range operations as each subject and watch the gate allow or deny each
   one. `carol`, who is in no group, is denied everything.
3. **Read visibility.** A point read of a key the caller lacks read permission
   for is soft-denied (returns absent, not an error), and a range read returns
   **only** the keys the caller is authorized to read - `bob` (auditor) sees all
   keys, `alice` sees only her stations and her own config key, `carol` sees
   nothing.
4. **Cross-cluster convergence.** Revoke `alice`'s config-key grant on `site-a`
   **only** and watch the revoke replicate onto `site-b`'s policy tree with no
   direct write to `site-b`.

Every subject's operations run under an ambient credential
(`LatticeCredentialContext.Use`) that flows to the grains on the Orleans request
context; a small custom `ILatticeCredentialAuthenticator` maps a demo token to a
subject id, and the membership directory expands the subject's group
memberships. Administrative seeding runs as the configured bootstrap
administrator so the reserved system trees can be provisioned before any rule
exists.

## Run it

```
dotnet run --project samples/AuthorizedAccess
```

## Expected output

```
Starting two Orleans clusters (site-a, site-b) with the auth stack...
Both clusters ready and peered over gRPC.

== Act 1: create users and groups ==
  alice in line-operators, bob in auditors, carol in no group.

== Act 2: author per-tree / per-key / per-prefix rules ==
  As alice (line-operators):
    write station/1/status  -> allowed
    write config/threshold     -> allowed
    write secret/recipe      -> DENIED   (no rule -> deny)
  As bob (auditors, read-only):
    read  secret/recipe      -> 'caramel'
    write station/2/status  -> DENIED   (read-only -> deny)
    delete station/2/status -> DENIED   (read-only -> deny)
  As alice (line-operators):
    delete station/3/status -> allowed   (prefix grant allows delete)

== Act 3: read visibility (point + range) ==
  Point read of the secret recipe:
    bob   -> 'caramel'  (auditor)
    carol -> (hidden)  (low-privilege: soft-denied)
  Range read of the whole tree returns only authorized keys:
    bob   sees 4 keys (auditor: all)
    alice sees 2 keys (stations + own config key)
    carol sees 0 keys (nothing)

== Act 4: a revoke on site-a converges to site-b ==
  Before: alice writing config/threshold on site-b -> allowed
  Revoked 'alice-config' on site-a only. Waiting for site-b to converge...
  site-b policy tree caught up: True (after 1s)
  site-b live gate already denies alice: False

[OK] the revoke authored on site-a converged onto site-b via system-tree replication.
```

## Convergence semantics

The policy tree is one of the reserved system trees enrolled into replication,
so the revoke propagates to `site-b`'s policy tree within the shipping window
(about a second on loopback). That authoritative state convergence is what the
sample asserts on.

Each site keeps a compiled read-through snapshot of its policy tree that its
gate consults, and refreshes that snapshot when it observes the policy change.
The `site-b live gate already denies alice` line reports whether `site-b`'s gate
has already rebuilt its snapshot from the converged tree at the moment we
poll; it may still read `False` immediately after convergence because the
snapshot refresh is decoupled from the tree write. The revoke is durably
converged either way - the rule is gone from `site-b`.

## When to use

- Multi-cluster deployments that must apply a single authorization policy
  uniformly across regions, where a grant or revoke authored in one region must
  become effective in the others without a central policy service.
- Any deployment that needs per-tree, per-key, or per-prefix authorization with
  a default-deny posture and group-based subjects.

## When not to use

- Single-cluster deployments that do not replicate - you still get the full
  authorization layer from `AddLatticeMembership` + `AddLatticeAuth`, without the
  replication and gRPC wiring this sample adds for the cross-cluster act.
- Deployments that do not need authorization at all. The layer is opt-in; a host
  that never calls `AddLatticeAuth` pays nothing on the data path.

## Notes on this sample

- Uses plaintext HTTP/2 (h2c) on loopback with the process-wide
  `Http2UnencryptedSupport` switch and receiver authentication turned off,
  because this is a loopback demo with no secret material. Production must use
  `https://` peer endpoints and leave receiver authentication on.
- The gRPC ports default to `17001` / `17002`; change them in `Program.cs` if
  those are taken on your machine.
- The bootstrap administrator (`root-admin`) is declared before initialization so
  it can provision the reserved system trees. Production should keep the
  bootstrap set as small as possible and grant everything else through rules.

## Feature docs

- [docs/lattice.membership/README.md](../../docs/lattice.membership/README.md)
- [docs/lattice.auth/README.md](../../docs/lattice.auth/README.md)
- [docs/lattice.auth/security-posture.md](../../docs/lattice.auth/security-posture.md)
