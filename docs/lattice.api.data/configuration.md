# Orleans.Lattice.Api.Data configuration

The facade package has one public options type, `LatticeApiDataOptions`, which carries the read-bounding knobs for a bounded range read and the drain-step knob for a bounded range delete. It is bound through the `AddLatticeDataApi` registration extension and resolvable via `IOptions<LatticeApiDataOptions>`. The sibling gRPC package also exposes `LatticeDataApiGrpcOptions` for server-side transport authorization and credential bridging.

The data API adds no authorization posture of its own: every operation routes through the gated `ILattice` surface, so the cluster's access gate is the single source of enforcement. These knobs bound range reads and the per-step range-delete drain size.

## `LatticeApiDataOptions`

Bounds a bounded range read and the per-step drain of a bounded range delete served by the data-plane facade. Bind it through `AddLatticeDataApi(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultRangePageSize` | `int` | `100` | Page size used for a bounded range read when the request leaves its page size unset (`0` or negative). |
| `MaxRangePageSize` | `int` | `1000` | Largest bounded-range-read page size honoured; larger requested page sizes are clamped down. |
| `RangeDeleteStepSize` | `int` | `256` | Batch size the facade drains per step during a bounded range delete; values below `1` fall back to `1`. |

## `LatticeDataApiGrpcOptions`

Server-side options for the sibling `Orleans.Lattice.Api.Data.Grpc` binding. Configure them through `AddLatticeDataApiGrpc(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the authorization interceptor enforces `ILatticeDataApiAuthorizer` on inbound data-API RPCs. Default-deny: the binding fails closed unless a host registers a permissive authorizer or turns enforcement off behind a trusted boundary. |
| `CredentialHeaderName` | `string` | `authorization` | The inbound gRPC metadata header carrying the caller's credential token, bridged into the ambient Lattice credential. |
| `CredentialScheme` | `string` | `Bearer` | The authentication scheme stamped on the bridged credential. A case-insensitive scheme prefix on the header value is stripped before the token is used. |
