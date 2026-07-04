# Authorization

> This is the single-silo introduction to the Orleans.Lattice authorization
> layer. For a multi-cluster deployment that also converges policy and
> membership across sites, see the
> [Cross-Cluster Authorization](../CrossClusterAuthorization/README.md) sample,
> which builds on the same layer.

## What it shows

The Orleans.Lattice authorization layer on **one in-process Orleans silo**, with
a focus on **group and nested-group membership**: identity (users, groups, and a
group nested inside another group), a default-deny policy, and per-tree /
per-key / per-prefix rules enforced on every operation. There is no replication,
no gRPC, and no web host - just the authorization you get from
`AddLatticeMembership` + `AddLatticeAuth`.

The membership graph is deliberately nested:

```
staff  (top-level group)
  |
  +-- engineering  (a group that is a member of staff)
        |
        +-- alice   (a user)
```

Because group membership is transitive, `alice` belongs to both `engineering`
and `staff` even though she was only ever added to `engineering`. A rule that
grants `staff` read therefore reaches `alice` through the nesting.

The demo runs in four acts:

1. **Nested membership.** Create users (`alice`, `bob`, `carol`) and groups
   (`staff`, `engineering`, `oncall`), make `engineering` a **member of**
   `staff`, and print each subject's transitive groups - `alice` shows up in
   `staff` even though she was only added to `engineering`.
2. **Rules and enforcement.** Author three rules under a default-deny policy - a
   tree grant on the top-level group (`staff` reads the whole tree), a prefix
   grant on the nested group (`engineering` writes/deletes the `svc/` subtree),
   and a key grant on a flat group (`oncall` reads/writes one incident key).
   Then exercise reads, writes, and deletes as each subject and watch the gate
   allow or deny each one. `carol`, who is in no group, is denied everything.
3. **Read visibility.** A range read returns **only** the keys the caller is
   authorized to read - `alice` (staff) sees every key, `bob` (oncall) sees only
   the one incident key it may read, `carol` sees nothing. A point read of an
   unauthorized key is soft-denied (returns absent, not an error).
4. **Runtime grant via nesting.** Add `carol` to `engineering` at runtime. She
   immediately inherits `staff` through the nested edge and gains both the staff
   tree-read and the engineering prefix-write - with no per-user rule authored
   for her.

Every subject's operations run under an ambient credential
(`LatticeCredentialContext.Use`) that flows to the grains on the Orleans request
context; a small custom `ILatticeCredentialAuthenticator` maps a demo token to a
subject id, and the membership directory expands the subject's group memberships
transitively (walking nested groups). Administrative seeding runs as the
configured bootstrap administrator so the policy and directory can be
provisioned before any rule exists.

## Run it

```
dotnet run --project samples/Authorization
```

## Expected output

```
Silo starting... ready.

== Act 1: build a nested membership graph ==
  alice's transitive groups: {engineering, staff}  (staff inherited via engineering)
  bob's   transitive groups: {oncall}
  carol's transitive groups: {}

== Act 2: author default-deny rules and enforce them ==
  As alice (engineering -> staff):
    read  audit/log       -> 'seeded'   (staff tree-read)
    write svc/api/status  -> allowed   (engineering prefix-write)
    write incident/current -> DENIED   (not oncall -> deny)
  As bob (oncall):
    write incident/current -> allowed   (oncall key grant)
    write svc/api/status  -> DENIED   (not engineering -> deny)
    delete svc/api/status -> DENIED   (not engineering -> deny)
  As carol (no groups):
    read  audit/log       -> (hidden)   (soft-denied)
    write svc/api/status  -> DENIED   (deny)

== Act 3: read visibility (point + range) ==
  Range read of the whole tree returns only authorized keys:
    alice sees 4 keys (staff: all)
    bob   sees 1 keys (only the incident key it may read)
    carol sees 0 keys (nothing)

== Act 4: grant access by joining a nested group at runtime ==
  Before: carol range read sees 0 keys; write svc/api/status -> DENIED
  Added carol to engineering. Her transitive groups: {engineering, staff}
  After:  carol range read sees 4 keys; write svc/api/status -> allowed

[OK] nested-group membership granted carol read + write with no per-user rule.
```

## When to use

- Single-cluster deployments that need per-tree, per-key, or per-prefix
  authorization with a default-deny posture and group-based subjects.
- Any deployment that wants to grant access by **group**, including nested
  groups (roles composed of roles), rather than authoring a rule per user.

## When not to use

- Multi-cluster deployments that must apply a single authorization policy
  uniformly across regions - see the
  [Cross-Cluster Authorization](../CrossClusterAuthorization/README.md) sample,
  which enrols the membership and policy system trees into cross-cluster
  replication so a grant or revoke authored in one region becomes effective in
  the others.
- Deployments that do not need authorization at all. The layer is opt-in; a host
  that never calls `AddLatticeAuth` pays nothing on the data path.

## Notes on this sample

- The bootstrap administrator (`root-admin`) is declared before initialization so
  it can provision the reserved system trees. Production should keep the
  bootstrap set as small as possible and grant everything else through rules.
- Enforcement reads a compiled policy snapshot that rebuilds off the policy-tree
  change feed, and group membership is resolved from the directory, so the
  sample polls briefly after authoring rules and after the runtime membership
  change before asserting the new outcome.

## Feature docs

- [docs/lattice/security.md](../../docs/lattice/security.md)
- [docs/lattice.membership/README.md](../../docs/lattice.membership/README.md)
- [docs/lattice.auth/README.md](../../docs/lattice.auth/README.md)
