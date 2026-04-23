using Microsoft.Extensions.Logging;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Appends replog entries to the sibling <c>_replog__{tree}</c>
/// lattice tree. A thin wrapper so the filter, the anti-entropy sweep,
/// and tests all use the same encoding and HLC-selection rules.
/// </summary>
/// <remarks>
/// <b>FUTURE seam.</b> When the library surfaces a native change
/// feed, callers replace <see cref="AppendAsync"/> with a
/// subscription and this class (plus <see cref="ReplogKeyCodec"/>)
/// disappears.
/// </remarks>
internal sealed class ReplicationLogWriter(
    IGrainFactory grains,
    ReplicationTopology topology,
    ILogger<ReplicationLogWriter> logger)
{
    private long _wallCeiling;
    private int _counter;
    private readonly object _hlcLock = new();

    /// <summary>
    /// Appends one replog entry for <paramref name="tree"/>. The
    /// entry's HLC is minted locally — we don't have access to the
    /// lattice's internal HLC from outside the library, so the
    /// replog carries a best-effort monotonic approximation:
    /// wall-clock ticks on every call, with a per-process counter
    /// as a within-tick tiebreaker. Cross-cluster ordering uses this
    /// HLC + the cluster-id tiebreaker baked into the replog key.
    /// </summary>
    public async Task AppendAsync(
        string tree,
        string originalKey,
        ReplicationOp op,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(originalKey);

        var hlc = NextHlc();
        var replogKey = ReplogKeyCodec.Encode(hlc, topology.LocalCluster, op, originalKey);
        var replogTreeId = ReplicationConstants.ReplogTreePrefix + tree;

        // Envelope value is empty — the key alone is enough (see
        // ReplogKeyCodec). Using a single-byte placeholder avoids
        // "empty byte[] treated as delete" corner cases inside any
        // downstream storage provider.
        byte[] marker = [1];

        try
        {
            var replog = grains.GetGrain<ILattice>(replogTreeId);
            await replog.SetAsync(replogKey, marker, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Replog append failure is non-fatal — the anti-entropy
            // sweep will resurface the write on the next periodic
            // reconciliation. Log at Warning so it's visible.
            logger.LogWarning(ex,
                "Replog append failed for tree {Tree} key {Key} op {Op}",
                tree, originalKey, op);
        }
    }

    /// <summary>
    /// Mint the next HLC for a replog entry. Monotonic within a
    /// single process; across processes the cluster-id tiebreaker in
    /// the replog key keeps ordering deterministic.
    /// </summary>
    private HybridLogicalClock NextHlc()
    {
        lock (_hlcLock)
        {
            var now = DateTime.UtcNow.Ticks;
            if (now > _wallCeiling)
            {
                _wallCeiling = now;
                _counter = 0;
            }
            else
            {
                _counter++;
            }
            return new HybridLogicalClock { WallClockTicks = _wallCeiling, Counter = _counter };
        }
    }
}
