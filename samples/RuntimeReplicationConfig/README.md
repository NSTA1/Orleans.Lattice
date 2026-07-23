# Runtime per-tree replication configuration

This sample drives cross-cluster replication enablement at **runtime** through the
replication control API, instead of declaring replicated trees statically in the
`LatticeReplicationOptions.ReplicatedTrees` options map at boot.

## What it shows

The control facade `ILatticeReplicationControl` writes to a replicated CRDT
system tree, `sys-replication-config`, keyed by target tree id. Every cluster
that enrols that tree converges on the same per-tree decision: whether the tree
is replicated, and under which merge mode. Because the decision itself is a
CRDT, there is no bespoke handshake and no single owning cluster.

The sample hosts one single-silo Orleans cluster and runs a scripted,
non-interactive flow:

1. **Enable** replication for the `orders` tree under an `OrSet` merge mode. The
   merge mode is fixed at enable-time.
2. **Report** the live per-tree config through `GetReplicationConfigAsync`,
   showing `enabled=True mode=OrSet ambiguous=False`.
3. **Reject an in-place mode change.** Re-enabling `orders` under a different
   mode throws `LatticeReplicationModeChangeRejectedException`. The sanctioned
   path to change a tree's merge mode is disable-then-re-enable, which
   re-bootstraps peers.
4. **Disable** the tree. Shipping stops without purging data already replicated
   to peers.
5. **Report** the config again, showing `enabled=False`.

## Running it

```
dotnet run --project samples/RuntimeReplicationConfig
```

Expected output:

```
Silo starting... ready.

Enabling replication for tree 'orders' under OrSet...
  enabled: tree=orders mode=OrSet alreadyEnabled=False bootstrapRequested=False

Replication config (1 tree(s)):
  tree=orders enabled=True mode=OrSet ambiguous=False

Attempting an in-place mode change to LwwRegister (expected to be rejected)...
  rejected as expected: Tree 'orders' is already enabled under merge mode 'OrSet', ...

Disabling replication for tree 'orders'...
  disabled: tree=orders alreadyDisabled=False

Replication config (1 tree(s)):
  tree=orders enabled=False mode=OrSet ambiguous=False

Sample complete. Stopping silo...
Done.
```

## Wiring

The silo opts in with three calls:

- `AddLatticeReplication(...)` enables the replication engine and sets this
  cluster's `ClusterId`.
- `ReplicateLatticeReplicationConfig()` statically anchors the
  `sys-replication-config` tree. This is the one static enrolment the
  runtime-config model requires; every other tree is enabled dynamically
  through the facade.
- `AddLatticeReplicationApi()` binds `ILatticeReplicationControl` over the
  config authority.

## Authorization

This sample registers no auth stack, so the default allow-all access gate
authorizes every facade call. A production deployment registers
`Orleans.Lattice.Auth` and authors through the fail-closed API access gate,
which default-denies anonymous callers. Propagation to peers is **not**
re-consented per cluster: the trust boundary is the existing peer enrolment.

## See also

- `docs/lattice.api.replication/README.md` - the control facade and its DTOs.
- `docs/lattice.api.replication.grpc/README.md` - the cross-process gRPC binding.
- `docs/lattice.replication/runtime-config.md` - the engine-side config tree and
  compiled snapshot.
- `samples/CrossClusterReplication` - the static two-site replication topology.
