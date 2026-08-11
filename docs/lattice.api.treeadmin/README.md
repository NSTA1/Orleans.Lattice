# Orleans.Lattice.Api.TreeAdmin

A transport-agnostic whole-tree administration control facade for [Orleans.Lattice](../../README.md).

## What is it?

`Orleans.Lattice.Api.TreeAdmin` is the **whole-tree administration control plane** of a cluster. Where each existing control facade owns one responsibility - [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md) manages schema policy, versioning, remediation, and compliance; [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) manages capture and restore; [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md) manages runtime replication config - this facade presents one coherent tree-administration surface for an operator dashboard, a CLI, or an internal admin service.

It follows **composition over absorption**: it does not re-implement or merge the single-responsibility facades. Instead it **wraps** them by delegation - at this foundation stage it wraps [`ILatticeSchemaControl`](../lattice.api.schema/README.md) - so schema behavior, wire format, and authorization are unchanged and there is no breaking change to any facade it composes.

It is built the same way as the other API facades:

- **A transport-agnostic facade.** A single control surface (`ILatticeTreeAdmin`, a public contract in the shared [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) package) exposes tree-administration operations over plain request / response records. It has no wire dependency, so the same surface serves an in-process consumer and a remote one.
- **A code-first gRPC binding** (the sibling `Orleans.Lattice.Api.TreeAdmin.Grpc` package) that projects this facade onto a remotely callable service and typed client. This package ships no transport of its own; it is the contract every binding adapts over.

## Scope of this release

This is the **scaffolding foundation** for the tree-administration control plane. The facade is **discoverable but ships no whole-tree operations yet**:

- The only facade operation is the read-only **capability probe**, which composes the wrapped schema facade's own probe and reports whole-tree administration authority as a default-deny flag.
- The MCP tool group for tree administration is **advertised (to an administrator-granted caller) but empty** - it contributes no tools yet. This proves the discovery, permission-scoping, and gRPC wiring are complete end to end.
- The whole-tree lifecycle operations (bulk-load, delete, resize, reshard, and the rest) land in later releases, each delegating to (or composing) the appropriate single-responsibility facade rather than re-implementing it here.

## Core properties

- **Opt-in and absent by default.** Nothing registers unless the host calls `AddLatticeTreeAdminApi()` on the silo, and once added the facade does no background work until a method is called.
- **Composition, not absorption.** The facade owns no admin plane of its own; it delegates to the facades it wraps. It introduces no wire or alias change to any composed facade.
- **Fail-closed by construction.** The composed operations authorize through the wrapped facades' own gates. The whole-tree administration authority flag is reported default-deny until the lifecycle operations (and their gate) land.
- **Read-only capability probe.** A caller can ask, with no side effects, which tree-administration operations it may perform over a given tree. The probe is advisory only: it never replaces the per-operation authorization each real call still performs.

## Ordering

`AddLatticeTreeAdminApi()` must be called **after** `AddLatticeSchemaApi(...)`: the facade composes the schema control facade (`ILatticeSchemaControl`) by delegation, so that facade must be registered first. Calling it out of order fails fast at registration with an actionable message.

## Surface

The facade operations (each reached over the gRPC binding as one RPC):

| Operation | Purpose |
|---|---|
| Probe capabilities | Report, with no side effects, which tree-administration operations the caller may perform over a tree, embedding the composed schema capabilities. |

## See also

- [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md) - the schema control facade this foundation composes by delegation.
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) - the shared control-surface contract package that publishes `ILatticeTreeAdmin`.
- [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md) - the MCP server binding that advertises the tree-administration group.
