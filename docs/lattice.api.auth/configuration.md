# Orleans.Lattice.Api.Auth configuration

The facade package (`Orleans.Lattice.Api.Auth`) has one public options type, `LatticeApiAuthOptions`, the configuration and control facade for membership and authorization policy administration. It is bound through the `AddLatticeAuthApi` registration extension and resolvable via `IOptions<LatticeApiAuthOptions>`. The sibling gRPC binding package (`Orleans.Lattice.Api.Auth.Grpc`) adds one more public options type, `LatticeAuthApiGrpcOptions`, documented in [gRPC binding options](#grpc-binding-options) below.

The facade adds no authorization posture of its own beyond requiring an administrator: every operation routes through the same enforcement the in-cluster data path uses, anchored on the authorization package's bootstrap root-of-trust. Its single knob bounds the debugging / dashboard reads so a single call cannot enumerate an unbounded rule set.

## `LatticeApiAuthOptions`

Bounds the introspection reads of the auth control facade. Bind it through `AddLatticeAuthApi(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `MaxExplanationRules` | `int` | `1000` | Largest number of applying rules an explain / effective-permissions introspection result collects before it stops scanning, bounding the work and payload of a single introspection call. |

## gRPC binding options

`LatticeAuthApiGrpcOptions` (namespace `Orleans.Lattice.Api.Auth.Grpc`) controls the server-side gRPC binding of the auth control plane. Because administering policy is the most sensitive surface in the cluster, the defaults are fail-closed. Bind it through the gRPC package's `AddLatticeAuthApiGrpc(configure)` registration extension (the endpoint is then mapped with `MapLatticeAuthApiGrpc`).

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the binding's authorization interceptor enforces the registered authorizer on every inbound admin call (default-deny). Turning it off does not open the surface: the facade's own per-call administrator check still runs. |
| `CredentialHeaderName` | `string` | `"authorization"` | The inbound request-header (gRPC metadata) name carrying the caller's credential token, bridged into the ambient Lattice credential so the facade's administrator check can resolve the caller's subject. |
| `CredentialScheme` | `string` | `"Bearer"` | The authentication scheme stamped on the bridged credential, matched by a registered credential authenticator to resolve the caller's subject. A case-insensitive scheme prefix on the header value (for example `"Bearer "`) is stripped before the remaining token is used. |
