using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Operator;

/// <summary>
/// Issues strictly-monotonic <see cref="HybridLogicalClock"/> values for
/// operator-driven facts. Registered as a singleton so concurrent Blazor
/// circuits, gRPC calls, and background actions never receive the same
/// timestamp. Backed by <see cref="HybridLogicalClock.Tick"/>, which
/// guarantees the returned value is strictly greater than the previous one
/// (using wall-clock time as a floor).
/// </summary>
public sealed class OperatorClock
{
    private readonly object _gate = new();
    private HybridLogicalClock _last = HybridLogicalClock.Zero;

    /// <summary>Returns the next monotonic HLC, strictly greater than any previously issued value.</summary>
    public HybridLogicalClock Next()
    {
        lock (_gate)
        {
            _last = HybridLogicalClock.Tick(_last);
            return _last;
        }
    }
}
