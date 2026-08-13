namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.Data</c> add-on, the
/// read-write external data-plane API. Bound by
/// <see cref="LatticeApiDataServiceCollectionExtensions.AddLatticeDataApi"/> and
/// resolvable via <c>IOptions&lt;LatticeApiDataOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The type carries the read-bounding knobs the facade honours for a bounded
/// range read: the page-size default and cap. The data API adds no
/// authorization posture of its own - every operation routes through the gated
/// <see cref="ILattice"/> surface, so the cluster's access gate is the single
/// source of enforcement.
/// </remarks>
public sealed class LatticeApiDataOptions
{
    /// <summary>
    /// Page size used for a bounded range read when the request leaves
    /// <c>PageSize</c> unset (<c>0</c> or negative). Defaults to <c>100</c>.
    /// </summary>
    public int DefaultRangePageSize { get; set; } = 100;

    /// <summary>
    /// Largest bounded-range-read page size honoured; larger requested page
    /// sizes are clamped down. Defaults to <c>1000</c>.
    /// </summary>
    public int MaxRangePageSize { get; set; } = 1000;

    /// <summary>
    /// Batch size the facade drains per step when serving a bounded range
    /// delete. Larger values tombstone more keys per grain hop at the cost of a
    /// longer single call; the whole range is always drained regardless. Values
    /// below <c>1</c> fall back to <c>1</c>. Defaults to <c>256</c>.
    /// </summary>
    public int RangeDeleteStepSize { get; set; } = 256;
}
