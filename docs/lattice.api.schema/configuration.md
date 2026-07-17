# Orleans.Lattice.Api.Schema configuration

The package has one public options type, `LatticeApiSchemaOptions`, bound through `AddLatticeSchemaApi(configure)` and resolvable via `IOptions<LatticeApiSchemaOptions>`.

## `LatticeApiSchemaOptions`

The options object is reserved for future read-bounding and audit-tuning knobs the control facade may honour.

| Property | Type | Default | Meaning |
|---|---|---|---|
| (none) | - | - | The current facade has no tunable properties. |

The empty options type is intentional: it keeps registration and configuration shape aligned with the sibling control-API facades while leaving room for future bounded-read and audit controls without changing the extension method.

## What is configured elsewhere

This facade drives the schema engine but does not re-expose its configuration. Policy semantics, value transforms, dead-letter storage, remediation behaviour, compliance reporting, and versioning are configured on [`Orleans.Lattice.Schema`](../lattice.schema/README.md). Versioning operations require the separate `AddLatticeSchemaVersioning(...)` registration. Transport concerns - authorization, credentials, TLS, deadlines - live on the [gRPC binding](../lattice.api.schema.grpc/configuration.md), not here.
