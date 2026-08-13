# Orleans.Lattice.Api.TreeAdmin

A transport-agnostic whole-tree administration control facade for [Orleans.Lattice](../../README.md).

## What is it?

`Orleans.Lattice.Api.TreeAdmin` is the **whole-tree administration control plane** of a cluster. Where each existing control facade owns one responsibility - [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md) manages schema policy, versioning, remediation, and compliance; [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) manages capture and restore; [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md) manages runtime replication config - this facade presents one coherent tree-administration surface for an operator dashboard, a CLI, or an internal admin service.

It follows **composition over absorption**: it does not re-implement or merge the single-responsibility facades. Instead it **wraps** them by delegation - at this foundation stage it wraps [`ILatticeSchemaControl`](../lattice.api.schema/README.md) - so schema behavior, wire format, and authorization are unchanged and there is no breaking change to any facade it composes.

It is built the same way as the other API facades:

- **A transport-agnostic facade.** A single control surface (`ILatticeTreeAdmin`, a public contract in the shared [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) package) exposes tree-administration operations over plain request / response records. It has no wire dependency, so the same surface serves an in-process consumer and a remote one.
- **A code-first gRPC binding** (the sibling `Orleans.Lattice.Api.TreeAdmin.Grpc` package) that projects this facade onto a remotely callable service and typed client. This package ships no transport of its own; it is the contract every binding adapts over.

## Scope of this release

The facade is discoverable and now surfaces its first operations:

- The read-only **capability probe** composes the wrapped schema facade's own probe, reports whole-tree administration authority from the caller's whole-tree `Admin` authority, and reports a `CanViewDiagnostics` flag from the caller's whole-tree read authority.
- Six read-only **diagnostics and storage-accounting operations** wrap the existing public grain surface (`ILattice`, `ILatticeAdmin`) rather than re-implementing shard fan-out: per-shard hotness, whole-tree diagnostics, shard-map topology inspection, per-shard leaf-projection digest, rolled-up tree statistics, and cluster-wide storage accounting. Each authorizes through the shared core fail-closed access gate before dialing the grain - the per-tree verbs on whole-tree `Read` authority and the cluster-wide storage summary on the distinct `Telemetry` capability.
- Seven **tree lifecycle and per-tree registry configuration operations** wrap the existing internal tree registry (`ILatticeRegistry`) rather than re-implementing registration or config: explicit tree creation (idempotent, with optional initial sizing), existence checks, alias assignment / resolution, per-tree configuration read / update (publish-events, projection-digest, history-retention), and the registry-persisted shard-map read. The mutating verbs (create, set-alias, set-config) authorize on whole-tree `Admin` authority and reject reserved system tree ids (the `_lattice_` namespace); the read verbs (exists, resolve-alias, get-config, get-shard-map) authorize on whole-tree `Read` authority.
- Four **tree soft-delete lifecycle operations** wrap the existing public tree grain (`ILattice`) rather than re-implementing the soft-delete coordinator: soft-delete, recover, hard-purge, and a read of the tree's deletion status. The three mutating verbs (delete, recover, purge) authorize on whole-tree `TreeLifecycle` authority - the dedicated destructive-lifecycle capability, held separately from routine `Admin` - and reject reserved system tree ids; hard-purge additionally requires an explicit confirmation flag. The status read authorizes on whole-tree `Read` authority.
- A streamed, resumable **bulk-load (tree-creation) operation** wraps the existing public tree grain (`ILattice`) rather than re-implementing tree construction: a begin / append / commit chunked protocol grafts strictly-ascending key/value entries onto an empty tree under a stable, caller-supplied operation id, so a broken stream resumes from its last un-acknowledged chunk and re-driven chunks deduplicate. All three verbs authorize on the distinct whole-tree `BulkLoad` authority and reject reserved system tree ids; begin additionally rejects a tree that is not empty.
- The **schema-management tools** (from the sibling schema facade) are surfaced under the tree-administration MCP group by delegation.
- The remaining whole-tree lifecycle operations (resize, reshard, and the rest) land in later releases, each delegating to (or composing) the appropriate single-responsibility facade rather than re-implementing it here.

## Core properties

- **Opt-in and absent by default.** Nothing registers unless the host calls `AddLatticeTreeAdminApi()` on the silo, and once added the facade does no background work until a method is called.
- **Composition, not absorption.** The facade owns no admin plane of its own; it wraps the schema control facade by delegation and reaches the existing public grain surface for read-only diagnostics and the internal tree registry for lifecycle and per-tree configuration. It introduces no wire or alias change to any composed facade or grain.
- **Fail-closed by construction.** Every operation authorizes through the shared core access gate (or the wrapped facade's own gate) before doing any work - whole-tree `Read` for the per-tree diagnostics and lifecycle-read verbs, whole-tree `Admin` for the mutating registry-configuration verbs, the distinct whole-tree `TreeLifecycle` capability for the destructive soft-delete / recover / purge verbs, the distinct whole-tree `BulkLoad` capability for the bulk-load verbs, the distinct cluster-wide `Telemetry` capability for the storage summary, and schema-management authority for a schema mutation.
- **Read-only capability probe.** A caller can ask, with no side effects, which tree-administration operations it may perform over a given tree. The probe is advisory only: it never replaces the per-operation authorization each real call still performs.

## Ordering

`AddLatticeTreeAdminApi()` must be called **after** `AddLatticeSchemaApi(...)`: the facade composes the schema control facade (`ILatticeSchemaControl`) by delegation, so that facade must be registered first. Calling it out of order fails fast at registration with an actionable message.

## Surface

The facade operations (each reached over the gRPC binding as one RPC):

| Operation | Purpose |
|---|---|
| Probe capabilities | Report, with no side effects, which tree-administration operations the caller may perform over a tree, embedding the composed schema capabilities. |
| Get shard hotness | Read a tree's per-shard read/write hotness with tree-level totals. Requires whole-tree read authority. |
| Get diagnostics | Read a whole-tree diagnostic report; the `deep` flag walks leaf state for authoritative counts. Requires whole-tree read authority. |
| Inspect shard map | Inspect a tree's shard-map topology (physical tree id, virtual/physical shard counts, map version). Requires whole-tree read authority. |
| Get projection digest | Read a single shard's leaf-projection content digest for cheap divergence detection. Requires whole-tree read authority. |
| Get tree stats | Read a tree's rolled-up topology, live-key counts, and storage byte breakdown in one call. Requires whole-tree read authority. |
| Get storage usage | Read cluster-wide storage accounting; the `deep` flag forces a fresh leaf-walk instead of the cheap cached WAL-poll aggregate. Requires cluster telemetry authority. |
| Create tree | Explicitly create (register) a tree with an optional initial sizing (shard count, max leaf keys, max internal children). Idempotent under matching parameters; reserved system tree ids are rejected. Requires whole-tree admin authority. |
| Check tree exists | Report whether a tree is registered. Requires whole-tree read authority. |
| Set tree alias | Point a logical tree at a physical tree. Reserved system tree ids are rejected. Requires whole-tree admin authority. |
| Resolve tree alias | Resolve the physical tree a logical tree maps to. Requires whole-tree read authority. |
| Get tree config | Read a tree's registry-backed configuration (sizing, alias, and per-tree overrides). Requires whole-tree read authority. |
| Set tree config | Apply a partial per-tree configuration update (publish-events, projection-digest, history-retention), each override written only when its apply flag is set. Reserved system tree ids are rejected. Requires whole-tree admin authority. |
| Get shard map | Read a tree's registry-persisted shard map (custom-map flag, version, virtual/physical shard counts, physical shard indices). Distinct from the diagnostics live-routing inspection. Requires whole-tree read authority. |
| Get tree deletion status | Read a tree's soft-deletion status (whether deleted, when, its recovery deadline, whether a purge is in progress or complete, and whether it can still be recovered). Requires whole-tree read authority. |
| Delete tree | Soft-delete a tree, opening its recovery window. Reserved system tree ids are rejected. Requires whole-tree tree-lifecycle authority. |
| Recover tree | Recover a soft-deleted tree within its recovery window. Reserved system tree ids are rejected. Requires whole-tree tree-lifecycle authority. |
| Purge tree | Irreversibly hard-purge a soft-deleted tree; an explicit confirmation flag is required and a false or omitted value is rejected. Reserved system tree ids are rejected. Requires whole-tree tree-lifecycle authority. |
| Begin bulk-load | Open a streamed, resumable bulk-load (tree-creation) session over an empty tree under a stable, idempotent operation id. The target tree must start empty (no live keys, no tombstones); a populated tree is rejected. Reserved system tree ids are rejected. Requires whole-tree bulk-load authority. |
| Append bulk-load chunk | Graft one strictly-ascending chunk of key/value entries onto an open session at a zero-based, monotonically increasing chunk index, returning the accepted-entry count and the next expected index. Re-sending the same chunk index with the same operation id is idempotent, so a broken stream resumes from its last un-acknowledged chunk. Requires whole-tree bulk-load authority. |
| Commit bulk-load | Close an open bulk-load session and report the tree's observed live-key count for a client-side sanity check. Reserved system tree ids are rejected. Requires whole-tree bulk-load authority. |

## See also

- [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md) - the schema control facade this foundation composes by delegation.
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) - the shared control-surface contract package that publishes `ILatticeTreeAdmin`.
- [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md) - the MCP server binding that advertises the tree-administration group.
