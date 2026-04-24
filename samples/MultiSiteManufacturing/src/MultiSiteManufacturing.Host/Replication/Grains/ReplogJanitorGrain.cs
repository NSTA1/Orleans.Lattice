using Microsoft.Extensions.Logging;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace MultiSiteManufacturing.Host.Replication.Grains;

/// <summary>
/// Reminder-driven replog janitor — see <see cref="IReplogJanitorGrain"/>.
/// </summary>
internal sealed class ReplogJanitorGrain(
    IGrainFactory grains,
    ReplicationTopology topology,
    ILogger<ReplogJanitorGrain> logger) : Grain, IReplogJanitorGrain, IRemindable
{
    /// <summary>Retention window: never prune entries younger than this.</summary>
    public static readonly TimeSpan Retention = TimeSpan.FromHours(24);

    private const string SweepReminder = "sweep";
    private static readonly TimeSpan SweepPeriod = TimeSpan.FromMinutes(10);

    private string _tree = string.Empty;

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _tree = this.GetPrimaryKeyString();
        await this.RegisterOrUpdateReminder(SweepReminder, dueTime: SweepPeriod, period: SweepPeriod);
        await base.OnActivateAsync(cancellationToken);
    }

    public async Task<int> SweepAsync()
    {
        if (topology.Peers.Count == 0)
        {
            return 0;
        }

        // Find the min cursor across every peer replicator for this
        // tree. A never-ticked replicator leaves Cursor at HLC Zero,
        // which correctly blocks pruning until it catches up.
        var minCursor = HybridLogicalClock.Zero;
        var first = true;
        foreach (var peer in topology.Peers)
        {
            var rep = grains.GetGrain<IReplicatorGrain>($"{_tree}|{peer.Name}");
            var status = await rep.GetStatusAsync();
            if (first || status.Cursor.CompareTo(minCursor) < 0)
            {
                minCursor = status.Cursor;
                first = false;
            }
        }

        // Subtract the retention window.
        var cutoffTicks = Math.Max(0, minCursor.WallClockTicks - Retention.Ticks);
        var cutoff = new HybridLogicalClock { WallClockTicks = cutoffTicks, Counter = 0 };

        if (cutoff.WallClockTicks == 0)
        {
            return 0;
        }

        var replogTreeId = ReplicationConstants.ReplogTreePrefix + _tree;
        var replog = grains.GetGrain<ILattice>(replogTreeId);

        var start = ReplogKeyCodec.ReplogStartBound;
        var end = ReplogKeyCodec.StartAfter(cutoff, topology.LocalCluster);

        var toDelete = new List<string>();
        await foreach (var key in replog.ScanKeysAsync(start, end))
        {
            toDelete.Add(key);
        }

        foreach (var key in toDelete)
        {
            await replog.DeleteAsync(key);
        }

        if (toDelete.Count > 0)
        {
            logger.LogInformation(
                "Replog janitor pruned {Count} entries for tree {Tree} below cutoff {Cutoff}",
                toDelete.Count, _tree, cutoff);
        }

        return toDelete.Count;
    }

    public Task ReceiveReminder(string reminderName, TickStatus status) =>
        string.Equals(reminderName, SweepReminder, StringComparison.Ordinal)
            ? SweepAsync()
            : Task.CompletedTask;
}
