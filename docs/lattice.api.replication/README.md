# Orleans.Lattice.Api.Replication

A transport-agnostic control facade for runtime per-tree cross-cluster replication, layered over [Orleans.Lattice.Replication](../lattice.replication/README.md).

## What is it?

`Orleans.Lattice.Api.Replication` is the **control plane** for turning cross-cluster replication on and off per tree at runtime. The [`Orleans.Lattice.Replication`](../lattice.replication/README.md) package ships the shipping, bootstrap, merge, and anti-entropy engine; this package adds the administrative surface an operator dashboard, a CLI, or an internal admin service needs to enable a tree under a chosen merge mode, disable it again, and inspect the per-tree replicated set - over a single surface with no wire dependency.

It is built the same way as the sibling [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) control facade:

- **A transport-agnostic facade.** A single control surface (`ILatticeReplicationControl`, a public contract in the shared `Orleans.Lattice.Api.Abstractions` package) exposes enable, disable, and permission-scoped config reporting over plain request / response records. It has no wire dependency, so the same surface serves an in-process consumer and a remote one.
- **A code-first gRPC binding** (the sibling [`Orleans.Lattice.Api.Replication.Grpc`](../lattice.api.replication.grpc/README.md) package) that projects this facade onto a remotely callable service and typed client.
- **An MCP tool group** (in [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md)) that exposes the same three operations as agent tools, gated by the same access control.

## How configuration is distributed

Replication configuration is not a bespoke store. It is itself a **replicated CRDT system tree**, `sys-replication-config`, dogfooding the exact pattern the replication engine already uses for its own membership and auth system trees. The tree is an OR-Map keyed by target tree id; each value is a small composite record of an enablement flag (a disable-wins `RwFlag`) and the fixed merge mode (an `MvRegister` so concurrent divergent modes survive and stay detectable rather than silently overwriting one another).

Because the configuration is a converging tree, an operator flips a tree on once, on any cluster, and every enrolled peer converges to the same decision. Per-cluster propagation is **not** re-consented - the trust boundary is the existing peer enrolment, so authorization gates the authoring cluster only.

For the engine-side mechanics - the static anchor, the compiled snapshot, and the fail-closed ambiguity handling - see [runtime replication configuration](../lattice.replication/runtime-config.md).

## Core properties

- **Opt-in and absent by default.** Nothing registers unless the host calls `AddLatticeReplicationApi()` on the silo, and the facade does no background work until a method is called.
- **Fail-closed by construction.** Every operation authorizes its target tree through the existing Lattice access gate for the dedicated `LatticeOperation.Replication` capability, before touching engine state. An anonymous or unauthorized caller is denied with `LatticeAuthorizationDeniedException` and the engine is never consulted.
- **Mode fixed at enable time.** The merge mode is chosen when a tree is first enabled and cannot be changed in place; enabling an already-enabled tree under a different mode is rejected. The sanctioned way to change a mode is to disable then re-enable, which re-bootstraps the tree cleanly.
- **Disable never purges.** Disabling pauses shipping new mutations; it never deletes data already replicated to peers.
- **Permission-scoped discovery.** `GetReplicationConfigAsync` reports only the trees the caller is authorized to manage, so it never reveals the existence of a tree outside the caller's grant.

## Ordering

`AddLatticeReplicationApi()` must be called **after** `AddLatticeReplication(..., enableRuntimeConfig: true)` (which installs the dynamic config authority): that authority is the source of truth this facade drives. Calling it first fails fast at registration with an actionable message.

## Surface

The facade operations (each reached over the gRPC binding as one RPC, and over MCP as one tool):

| Operation | Purpose |
|---|---|
| Enable replication | Enable a tree under a fixed merge mode, optionally bootstrapping a non-empty tree from a named source cluster. |
| Disable replication | Pause shipping a tree without purging already-replicated peer data. Idempotent. |
| Get replication config | Report each authorized tree's enabled state, fixed merge mode, and ambiguity status. |

## Reference

- [API reference](api.md) - the public options and model types, and the facade operations by name.
- [Configuration](configuration.md) - the public options properties, their types, and defaults.
- [Architecture](architecture.md) - how the facade authorizes, delegates to the engine authority, and scopes discovery.

## See also

- [`Orleans.Lattice.Replication`](../lattice.replication/README.md) - the shipping, bootstrap, and merge engine this facade drives.
- [runtime replication configuration](../lattice.replication/runtime-config.md) - the engine-side config tree, compiled snapshot, and fail-closed resolution.
- [`Orleans.Lattice.Api.Replication.Grpc`](../lattice.api.replication.grpc/README.md) - the code-first gRPC binding and typed client.
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the control facade this one is modelled on.
