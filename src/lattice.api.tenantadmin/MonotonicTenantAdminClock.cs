using Orleans.Lattice;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The production <see cref="ITenantAdminClock"/>: a strictly monotonic
/// <see cref="HybridLogicalClock"/> source. Each call to <see cref="Next"/>
/// advances a per-instance previous stamp with
/// <see cref="HybridLogicalClock.Tick(HybridLogicalClock)"/> under a lock, which
/// both tracks wall-clock time (so an operator sees realistic stamps) and
/// guarantees strict per-instance monotonicity (so a burst of admin writes at the
/// same wall-clock tick still each supersede the previous one via the clock's
/// counter component).
/// </summary>
/// <remarks>
/// <para>
/// The seed is <c>Tick(Zero)</c> at construction, i.e. wall-now, so the first
/// stamp already reflects real time.
/// </para>
/// <para>
/// <b>Single-writer caveat.</b> Strict monotonicity is per-instance. In a cluster
/// with more than one silo authoring tenant-admin writes concurrently, two silos
/// could in principle emit the same wall-clock-plus-counter stamp; the registry's
/// LWW join then breaks the tie by writer id, so convergence is preserved but a
/// same-stamp concurrent write from a lexicographically smaller writer id would
/// lose. This is inherent to the tenancy engine's HLC-LWW design and acceptable
/// for a low-frequency, effectively single-writer control-plane administration
/// path.
/// </para>
/// </remarks>
internal sealed class MonotonicTenantAdminClock : ITenantAdminClock
{
    private readonly object _lock = new();
    private HybridLogicalClock _previous = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

    /// <inheritdoc />
    public HybridLogicalClock Next()
    {
        lock (_lock)
        {
            _previous = HybridLogicalClock.Tick(_previous);
            return _previous;
        }
    }
}
