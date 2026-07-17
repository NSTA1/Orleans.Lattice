# Orleans.Lattice.Api.Schema.Grpc configuration

The package has one public server-side options type, `LatticeSchemaApiGrpcOptions`, bound through `AddLatticeSchemaApiGrpc(configure)`. The client (`LatticeSchemaApiGrpcClient`) carries no options of its own - transport concerns live on the `CallInvoker` / `GrpcChannel` the caller supplies.

## `LatticeSchemaApiGrpcOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the authorization interceptor enforces the transport authorization gate on every protected inbound call. Default-deny: the binding fails closed unless a host configures authorization or turns enforcement off. Set to `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `authorization` | The inbound request-header (gRPC metadata) name carrying the caller's credential token, bridged into the ambient Lattice credential so the schema access gate can resolve the caller's subject. |
| `CredentialScheme` | `string` | `Bearer` | The authentication scheme stamped on the bridged credential, matched by a registered credential authenticator to resolve the caller's subject. A case-insensitive scheme prefix on the header value (for example `"Bearer "`) is stripped before the remaining token is used. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | empty | The auth schemes the endpoint advertises from its unauthenticated `GetAuthScheme` RPC, in preference order. Empty by default (the endpoint advertises nothing, so a client falls back to manual or Basic selection). Each descriptor must carry only public configuration - never a secret. |

`AdvertisedAuthSchemes` is a mutable list property: populate it in the configure delegate (for example by adding descriptors to the list).

## Fail-closed defaults

Out of the box the binding leaves `RequireAuthorization` at `true` and uses a default-deny authorization posture, so every protected call is rejected with `PermissionDenied` until the host opts in - either by configuring authorization or by setting `RequireAuthorization = false` behind a trusted boundary. `GetAuthScheme` is the only unauthenticated exception. See [Architecture](architecture.md) for how the transport gate and the facade's own tree-scope authorization combine.

## Client transport

The typed client is configured entirely through the `CallInvoker` the caller passes to `LatticeSchemaApiGrpcClient.Create`:

- **Address, TLS, retries, deadlines** - set on the `GrpcChannel` / `CallInvoker`.
- **Call credentials** - attach on the channel or per call; the header name and scheme the server reads are `CredentialHeaderName` / `CredentialScheme` above.
- **Serialization** - the `IServiceProvider` passed to `Create` must have Orleans serialization registered (`AddSerializer()`) so the client and server marshallers match.
