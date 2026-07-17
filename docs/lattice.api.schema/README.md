# Orleans.Lattice.Api.Schema

A transport-agnostic schema-management control facade for [Orleans.Lattice.Schema](../lattice.schema/README.md).

## What is it?

`Orleans.Lattice.Api.Schema` is the **control plane** of a cluster's schema system. The [`Orleans.Lattice.Schema`](../lattice.schema/README.md) package adds enforcement, policy storage, dead-letter handling, remediation, compliance scanning, and the optional versioning engine reached through .NET service interfaces; this package adds the administrative surface an operator dashboard, a CLI, or an internal admin service needs to manage policies, inspect rejected writes, drive remediation, and audit compliance - over a single surface with no wire dependency.

It is built the same way as the read-only [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md), read-write [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md), and backup [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) facades:

- **A transport-agnostic facade.** A single control surface (`ILatticeSchemaControl`, a public contract in the shared [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) package) exposes policy, dead-letter, versioning, remediation, compliance-audit, and capability-probe operations over plain request / response records. It has no wire dependency, so the same surface serves an in-process consumer and a remote one.
- **A code-first gRPC binding** (the sibling [`Orleans.Lattice.Api.Schema.Grpc`](../lattice.api.schema.grpc/README.md) package) that projects this facade onto a remotely callable service and typed client. This package ships no transport of its own; it is the contract every binding adapts over.

## Core properties

- **Opt-in and absent by default.** Nothing registers unless the host calls `AddLatticeSchemaApi()` on the silo, and once added the facade does no background work until a method is called.
- **Fail-closed by construction.** Every operation authorizes the tree's scope through the shared `SchemaAccessAuthorizer` before touching the admin plane. Reads require Read authority; mutations require SchemaAdmin authority.
- **Bounded-memory dead-letter streaming.** Dead-letter inspection can stream diverted schema-rejected writes without materializing the whole set in memory.
- **Versioning stays separate.** Schema versioning is enabled by `AddLatticeSchemaVersioning(...)`. A version operation on a silo without versioning registered throws a clear `InvalidOperationException` at call time rather than failing dependency resolution.
- **Read-only capability probe.** A caller can ask, with no side effects, which schema operations it may perform over a given tree. The probe runs fail-closed Read and SchemaAdmin checks and reports the result as capability flags, so a UI can grey out actions the caller cannot perform without ever mutating state. The probe is advisory only: it never replaces the per-operation authorization each real call still performs.

## Ordering

`AddLatticeSchemaApi()` must be called **after** `AddLatticeSchemaEnforcement(...)`: the schema engine is the source of truth for the policy, dead-letter, remediation, compliance, and authorization seams this facade drives. Calling it first fails fast at registration with an actionable message.

`AddLatticeSchemaVersioning(...)` is separate and optional. Register it when versioning operations should be available.

## Surface

The facade operations (each reached over the gRPC binding as one RPC):

| Operation | Purpose |
|---|---|
| Set policy | Set a tree's write-validation policy. |
| Clear policy | Remove a tree's policy and report whether one was present. |
| Get policy | Read a tree's policy, or absent. |
| List dead letters | Stream the diverted schema-rejected writes with bounded memory. |
| Count dead letters | Count the diverted schema-rejected writes. |
| Set version config | Set a tree's versioning config. |
| Get version config | Read a tree's version config, or absent. |
| Advance target version | Advance the declared target version without migrating existing values. |
| Advance and migrate | Advance the target version and migrate existing values to it. |
| Migrate to target version | Migrate existing values up to the current target version. |
| Clear version config | Remove the tree's version config and report whether one was present. |
| Remediate | Apply a value transform across a tree and adopt a target policy. |
| Get remediation status | Read the status or last report of remediation for a tree. |
| Scan compliance | Run a read-only compliance audit and return counts and reasons. |
| Probe capabilities | Report, with no side effects, which schema operations the caller may perform over a tree. |

## Reference

- [API reference](api.md) - the public options and model types, and the facade operations by name.
- [Configuration](configuration.md) - the public options properties, their types, and defaults.
- [Architecture](architecture.md) - how the facade authorizes, streams dead letters, gates versioning, and audits compliance.

## See also

- [`Orleans.Lattice.Schema`](../lattice.schema/README.md) - the enforcement, versioning, dead-letter, remediation, and compliance engine this facade drives.
- [`Orleans.Lattice.Api.Schema.Grpc`](../lattice.api.schema.grpc/README.md) - the code-first gRPC binding and typed client.
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) - the shared control-surface contract package that publishes `ILatticeSchemaControl`.
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the sibling control facade this package mirrors.
