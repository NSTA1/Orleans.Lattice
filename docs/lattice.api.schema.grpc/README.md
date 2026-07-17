# Orleans.Lattice.Api.Schema.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.Schema](../lattice.api.schema/README.md) - projects the schema-management control facade onto a gRPC service and a public typed client, over the same Orleans-serialized records, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.Schema.Grpc` is the remote transport for the cluster's schema control plane. A host references it when a dashboard, a CLI, or a future bridge needs to manage schema policy, inspect dead letters, drive versioning and remediation, scan compliance, or probe capabilities over the network rather than in-process.

It provides:

- **A code-first gRPC service.** One RPC per facade operation - unary for policy, count, versioning, remediation, compliance, capability, and auth-scheme calls, and server-streaming for dead-letter draining - bound from C# definitions rather than a `.proto`.
- **A public typed client.** `LatticeSchemaApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC `CallInvoker`.
- **Shared Orleans marshalling.** Every wire message is one of the package's `[GenerateSerializer]` records, serialized with the Orleans binary serializer, so client and server stay in lock-step by construction.
- **Two-layer, fail-closed authorization.** A transport meta-authorizer gates every RPC at the edge, and the facade's own scope authorization re-authorizes the resolved caller. Both default to deny.

Schema administration changes write-validation rules and can rewrite existing values, so the binding fails closed: with no authorizer registered, every protected call is rejected with `PermissionDenied`.

## Core properties

- **Public client, internal service.** Callers consume `LatticeSchemaApiGrpcClient`; the service, marshallers, method definitions, and interceptor are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`.
- **Two load-bearing gates.** The transport meta-authorizer decides whether a call may run at all; the credential the identity bridge resolves then feeds the schema engine's own fail-closed scope authorization. Neither replaces the other.
- **Discoverable sign-in.** An unauthenticated `GetAuthScheme` RPC lets a client discover how to authenticate before it holds a credential.

## RPCs

The gRPC service name is `orleans.lattice.api.schema`.

| RPC | Kind | Facade operation |
|---|---|---|
| `SetPolicy` | unary | Set policy |
| `ClearPolicy` | unary | Clear policy |
| `GetPolicy` | unary | Get policy |
| `StreamDeadLetters` | server-streaming | List dead letters |
| `CountDeadLetters` | unary | Count dead letters |
| `SetVersionConfig` | unary | Set version config |
| `GetVersionConfig` | unary | Get version config |
| `AdvanceTargetVersion` | unary | Advance target version |
| `AdvanceAndMigrate` | unary | Advance and migrate |
| `MigrateToTargetVersion` | unary | Migrate to target version |
| `ClearVersionConfig` | unary | Clear version config |
| `Remediate` | unary | Remediate |
| `GetRemediationStatus` | unary | Get remediation status |
| `ScanCompliance` | unary | Scan compliance |
| `ProbeCapabilities` | unary | Probe capabilities |
| `GetAuthScheme` | unary (unauthenticated) | Advertise accepted auth schemes |

## Quick Start

Register the binding on a silo that already has `AddLatticeSchemaApi`, then map its routes. The host must expose the control facade in the same service provider - typically by co-hosting Orleans with `AddLattice(...).AddLatticeSchemaEnforcement(...).AddLatticeSchemaApi()` on the same host. Register `AddLatticeSchemaVersioning(...)` when version RPCs should succeed.

## Client

`LatticeSchemaApiGrpcClient` is created over a caller-supplied `CallInvoker` and an `IServiceProvider` with Orleans serialization registered (`AddSerializer()`). The typed client carries no address, TLS, retry, deadline, or credential policy of its own.

A call the server rejects arrives as a `PermissionDenied` or `Unauthenticated` transport failure; the client translates auth failures consistently. Other unmapped server faults are returned with a safe gRPC status rather than leaking implementation details. See [Architecture](architecture.md#status-mapping) for the mapping.

## Reference

- [API reference](api.md) - the public client, options, authorization behaviour, and wire message records.
- [Configuration](configuration.md) - the public options properties, their types, and defaults.
- [Architecture](architecture.md) - the two-layer authorization model and the code-first binding.

## See also

- [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md) - the transport-agnostic facade this binding adapts.
- [`Orleans.Lattice.Schema`](../lattice.schema/README.md) - the schema enforcement and versioning engine underneath.
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) - the shared control-surface contract package.
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the sibling control facade this binding mirrors.
