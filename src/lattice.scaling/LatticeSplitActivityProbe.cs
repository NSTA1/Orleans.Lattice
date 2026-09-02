using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Production <see cref="ISplitActivityProbe"/>: reads the cluster-wide
/// split-activity snapshot from
/// <see cref="Orleans.Lattice.ILatticeAdmin.GetSplitActivityAsync(System.Threading.CancellationToken)"/>
/// and reports whether any adaptive shard split is in flight, which suppresses
/// scale-in for that tick.
/// <para>
/// <b>Cost.</b> One call to the cluster's split-admission singleton per sample
/// tick, on the sampling timer and never on the scrape path. The admin surface
/// answers from footprints the autonomic monitors already publish, so this
/// causes no fan-out across trees or shards.
/// </para>
/// <para>
/// <b>Degradation is fail-open, deliberately.</b> When no grain factory is
/// available (the package added outside a silo, as in a bare unit-test
/// container) or the administrative call fails, the probe reports "no split in
/// flight" rather than throwing. Reporting <see langword="true"/> on failure
/// would be the fail-closed choice, but this gate only defers scale-in and a
/// persistently unreachable admin surface would then suppress scale-in forever -
/// converting a small, self-correcting risk (a silo drained mid-split, which
/// costs a split some rework but no correctness) into an unbounded cost ceiling.
/// The failure is logged so the degradation is visible.
/// </para>
/// </summary>
internal sealed class LatticeSplitActivityProbe(
    IGrainFactory? grainFactory = null,
    ILogger<LatticeSplitActivityProbe>? logger = null) : ISplitActivityProbe
{
    // LatticeConstants.AdminGrainKey is internal to the core assembly; the literal
    // is mirrored here (the single cluster-wide admin grain key) because the
    // scaling package is not on core's InternalsVisibleTo list.
    private const string AdminGrainKey = "_lattice_admin";

    private readonly IGrainFactory? _grainFactory = grainFactory;
    private readonly ILogger _logger = logger ?? NullLogger<LatticeSplitActivityProbe>.Instance;

    /// <inheritdoc />
    public async ValueTask<bool> AnySplitInFlightAsync(CancellationToken cancellationToken)
    {
        var admin = _grainFactory?.GetGrain<ILatticeAdmin>(AdminGrainKey);
        if (admin is null)
        {
            return false;
        }

        try
        {
            var activity = await admin.GetSplitActivityAsync(cancellationToken).ConfigureAwait(false);
            return activity.AnyInFlight;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "Failed to read cluster split activity; treating this tick as no split in flight.");
            return false;
        }
    }
}
