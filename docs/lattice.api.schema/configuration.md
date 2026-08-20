# Orleans.Lattice.Api.Schema configuration

The transport-agnostic package exposes `LatticeApiSchemaOptions`, bound through `AddLatticeSchemaApi(configure)` and resolvable via `IOptions<LatticeApiSchemaOptions>`. The sibling gRPC binding also exposes `LatticeSchemaApiGrpcOptions` for transport authorization and credential settings.

## `LatticeApiSchemaOptions`

The options object is reserved for future read-bounding and audit-tuning knobs the control facade may honour.

| Property | Type | Default | Meaning |
|---|---|---|---|
| (none) | - | - | The current facade has no tunable properties. |

The empty options type is intentional: it keeps registration and configuration shape aligned with the sibling control-API facades while leaving room for future bounded-read and audit controls without changing the extension method.

## Sibling gRPC options

When the remote binding is installed, `AddLatticeSchemaApiGrpc(configure)` binds `LatticeSchemaApiGrpcOptions` for server-side transport concerns.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the authorization interceptor enforces the transport authorization gate on protected inbound calls. Set to `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `authorization` | The inbound request-header name carrying the caller's credential token. |
| `CredentialScheme` | `string` | `Bearer` | The authentication scheme stamped on the bridged credential. A matching scheme prefix on the header value is stripped before the remaining token is used. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | empty | The auth schemes advertised from the unauthenticated `GetAuthScheme` RPC. Each descriptor must carry only public configuration - never a secret. |

See the [gRPC configuration](../lattice.api.schema.grpc/configuration.md) page for client transport setup and the full fail-closed default behaviour.

## What is configured elsewhere

This facade drives the schema engine but does not re-expose its configuration. Policy semantics, value transforms, dead-letter storage, remediation behaviour, compliance reporting, and versioning are configured on [`Orleans.Lattice.Schema`](../lattice.schema/README.md). Versioning operations require the separate `AddLatticeSchemaVersioning(...)` registration. Transport concerns - authorization, credentials, TLS, deadlines - live on the [gRPC binding](../lattice.api.schema.grpc/configuration.md), not here.
