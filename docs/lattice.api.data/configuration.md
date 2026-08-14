# Orleans.Lattice.Api.Data configuration

The package has one public options type, `LatticeApiDataOptions`, which carries the read-bounding knobs for a bounded range read and the drain-step knob for a bounded range delete. It is bound through the `AddLatticeDataApi` registration extension and resolvable via `IOptions<LatticeApiDataOptions>`.

The data API adds no authorization posture of its own: every operation routes through the gated `ILattice` surface, so the cluster's access gate is the single source of enforcement. These knobs bound range reads and the per-step range-delete drain size.

## `LatticeApiDataOptions`

Bounds a bounded range read and the per-step drain of a bounded range delete served by the data-plane facade. Bind it through `AddLatticeDataApi(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultRangePageSize` | `int` | `100` | Page size used for a bounded range read when the request leaves its page size unset (`0` or negative). |
| `MaxRangePageSize` | `int` | `1000` | Largest bounded-range-read page size honoured; larger requested page sizes are clamped down. |
| `RangeDeleteStepSize` | `int` | `256` | Batch size the facade drains per step during a bounded range delete; values below `1` fall back to `1`. |
