# Orleans.Lattice.Api.Replication

Optional, opt-in **replication control facade** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Exposes a single
transport-agnostic admin surface that drives runtime per-tree cross-cluster
replication configuration through the
[`Orleans.Lattice.Replication`](https://www.nuget.org/packages/Orleans.Lattice.Replication)
engine: enable replication for a tree (fixing its wire merge mode), disable it,
and inspect the runtime replicated-tree set - from one place. Sibling packages
project this facade onto a code-first gRPC surface and an MCP tool group.

## Design

The facade mirrors the `Orleans.Lattice.Api.Backup` and `Orleans.Lattice.Api.Schema`
control facades: the facade is the contract, transports bind over it, and it
costs nothing until it is registered.

- **Runtime, not redeploy.** Replication membership becomes a runtime operation
  instead of static host config baked into every silo image.
- **Converges across the estate.** An enable or disable authored on one cluster
  converges over the already-enrolled peer set; the operator flips it once and
  every peer follows.
- **Mode fixed at enable time.** A tree's merge mode is chosen when it is first
  enabled and cannot be changed in place; the sanctioned path to change a mode is
  disable then re-enable, which re-bootstraps.

## Security

Every operation authorizes fail-closed through the same Lattice access gate the
data plane uses, before touching engine state, requiring the dedicated
`LatticeOperation.Replication` capability on the target tree. Anonymous callers
are denied by default.

- **Permission-scoped discovery.** `GetReplicationConfigAsync` reports only the
  trees the caller is authorized to manage, so it never reveals a tree outside
  the caller's grant.
- **Propagation is not re-consented.** The trust boundary is the existing peer
  enrolment, so authorization gates the authoring cluster only.
- **Opt-in and absent by default.** Nothing is registered unless the host calls
  `AddLatticeReplicationApi()`.
- **Must be registered after `AddLatticeReplication(..., enableRuntimeConfig: true)`.**
  The call fails fast with an actionable message otherwise.

## Usage

```csharp
siloBuilder
    .AddLattice(/* ... */)
    .AddLatticeReplication(/* ... */, enableRuntimeConfig: true)
    .AddLatticeReplicationApi();
```

Bind a transport over the facade to drive replication configuration remotely.
