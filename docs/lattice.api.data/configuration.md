---
agent_spec: "docs/agents/api/data.json"
---

# Orleans.Lattice.Api.Data configuration

The facade package exposes `LatticeApiDataOptions`, which carries the read-bounding knobs for a bounded range read and the drain-step knob for a bounded range delete. It is bound through the `AddLatticeDataApi` registration extension and resolvable via `IOptions<LatticeApiDataOptions>`. The sibling gRPC package exposes `LatticeDataApiGrpcOptions` for server-side transport authorization and credential bridging; it is documented in the [gRPC binding configuration](../lattice.api.data.grpc/configuration.md).

The data API adds no authorization posture of its own: every operation routes through the gated `ILattice` surface, so the cluster's access gate is the single source of enforcement. These knobs bound range reads and the per-step range-delete drain size.

## `LatticeApiDataOptions`

Bounds a bounded range read and the per-step drain of a bounded range delete served by the data-plane facade. Bind it through `AddLatticeDataApi(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultRangePageSize` | `int` | `100` | Page size used for a bounded range read when the request leaves its page size unset (`0` or negative). |
| `MaxRangePageSize` | `int` | `1000` | Largest bounded-range-read page size honoured; larger requested page sizes are clamped down. The effective page size is never below `1`, so a non-positive default or maximum still yields one-entry pages rather than a rejected request. |
| `RangeDeleteStepSize` | `int` | `256` | Batch size the facade drains per step during a bounded range delete; values below `1` fall back to `1`. |

## Registration

`AddLatticeDataApi(Action<LatticeApiDataOptions>? configure = null)` is the only registration method. It throws `InvalidOperationException` unless `AddLattice(...)` was called on the same silo builder first, applies `configure` when supplied, and always registers the options so `IOptions<LatticeApiDataOptions>` resolves with the defaults above even when no delegate is passed. A repeat call layers its delegate over the earlier configuration without registering a second facade.

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [gRPC binding configuration](../lattice.api.data.grpc/configuration.md) - `LatticeDataApiGrpcOptions`.
