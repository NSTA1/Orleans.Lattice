# Orleans.Lattice.Api.Schema

Optional, opt-in **schema-management control facade** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Exposes a single
transport-agnostic admin surface that drives the
[`Orleans.Lattice.Schema`](https://www.nuget.org/packages/Orleans.Lattice.Schema)
engine - set / clear / inspect enforcement policy, envelope versioning
(set / advance / migrate / clear), background remediation, and a read-only
per-tree compliance audit - from one place. A sibling package projects this
facade onto a code-first gRPC surface.

## Design

The facade mirrors the read-only `Orleans.Lattice.Api.State` and the backup
`Orleans.Lattice.Api.Backup` control facades: the facade is the contract,
transports bind over it, and it costs nothing until it is registered.

- **Read-only compliance audit.** `ScanComplianceAsync` streams a tree's current
  values through its compiled policy and returns per-tree counts of compliant vs
  non-compliant values, grouped by failure reason. It is a pure read - it never
  rewrites or dead-letters data - and is cancellable with best-effort progress.
- **Bounded-memory enumeration.** Dead-letter listing is streamed, so a large
  strict-mode queue enumerates with bounded memory.

## Security

Every operation authorizes its tree through the same schema access gate the
engine uses, before touching the admin plane. A mutation (set / clear policy,
version-config change, remediation) authorizes on `SchemaAdmin` authority; a read
(inspect policy / version config / dead letters / remediation status, or the
compliance audit) authorizes on ordinary `Read` authority. A capability probe
reports the caller's allowed operation set per tree with no side effects.

- **Opt-in and absent by default.** Nothing is registered unless the host calls
  `AddLatticeSchemaApi()`, and once added the facade does no background work until
  a method is called.
- **Must be registered after `AddLatticeSchemaEnforcement(...)`.** The call fails
  fast with an actionable message otherwise. Version operations additionally
  require `AddLatticeSchemaVersioning(...)`.

## Usage

```csharp
siloBuilder
    .AddLattice(/* ... */)
    .AddLatticeSchemaEnforcement()
    .AddLatticeSchemaApi();
```

Bind a transport over the facade to drive schema management remotely: the sibling
`Orleans.Lattice.Api.Schema.Grpc` package projects it onto a code-first gRPC
surface.
