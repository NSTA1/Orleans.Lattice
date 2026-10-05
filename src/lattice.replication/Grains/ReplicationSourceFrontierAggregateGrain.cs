using Orleans.Lattice.Backup;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="IReplicationSourceFrontierAggregateGrain"/>.</summary>
internal sealed class ReplicationSourceFrontierAggregateGrain(
    IReplicatedTreeMembership membership,
    [PersistentState("replication-source-frontier-aggregate", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationSourceFrontierAggregateState> state)
    : Grain, IReplicationSourceFrontierAggregateGrain
{
    /// <summary>
    /// How long a tree's report stays fresh. A shipper reports at least every
    /// <see cref="ReplicationShipperGrain.SourceFrontierReportInterval"/>.
    /// </summary>
    internal static TimeSpan ReportTtl { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>How long a lineage re-seed slot lasts without being renewed.</summary>
    internal static TimeSpan LineageReseedLeaseTtl { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>How many trees may re-seed the peer for a lineage change at once.</summary>
    internal static int LineageReseedConcurrency { get; set; } = 1;

    /// <summary>Time source; settable for tests.</summary>
    internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    private readonly Dictionary<string, (Guid Lineage, HybridLogicalClock Watermark, DateTimeOffset At)> _reports = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTimeOffset> _leases = new(StringComparer.Ordinal);
    private HashSet<string>? _knownTrees;
    private bool _raisedForActivation;

    /// <inheritdoc />
    public async Task<(HybridLogicalClock OriginLowWatermark, long Generation)> ReportAsync(
        string treeId, Guid receiverLineage, HybridLogicalClock treeLowWatermark, bool holdsLineageReseed)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var now = TimeProvider.GetUtcNow();
        var trees = membership.ReplicatedTrees;

        // Every activation starts a generation: reports from before it are gone.
        var raise = !_raisedForActivation;
        // A lineage change, a first sighting of a lineage included, starts a generation.
        if (_reports.TryGetValue(treeId, out var previous)
                ? previous.Lineage != receiverLineage
                : receiverLineage != Guid.Empty)
        {
            raise = true;
        }

        if (_knownTrees is null)
        {
            _knownTrees = new HashSet<string>(trees, StringComparer.Ordinal);
        }
        else
        {
            foreach (var tree in trees)
            {
                if (_knownTrees.Add(tree))
                {
                    raise = true;
                }
            }
        }

        if (raise)
        {
            state.State.Generation++;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.Generation--;
                throw;
            }

            _raisedForActivation = true;
        }

        _reports[treeId] = (receiverLineage, treeLowWatermark, now);
        if (holdsLineageReseed && _leases.ContainsKey(treeId))
        {
            _leases[treeId] = now + LineageReseedLeaseTtl;
        }

        return (Aggregate(trees, now), state.State.Generation);
    }

    /// <inheritdoc />
    public Task<bool> TryAcquireLineageReseedAsync(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var now = TimeProvider.GetUtcNow();
        foreach (var lapsed in _leases.Where(l => l.Value <= now).Select(l => l.Key).ToList())
        {
            _leases.Remove(lapsed);
        }

        if (!_leases.ContainsKey(treeId))
        {
            if (_leases.Count >= Math.Max(1, LineageReseedConcurrency))
            {
                return Task.FromResult(false);
            }

            _leases[treeId] = now + LineageReseedLeaseTtl;
        }

        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task ReleaseLineageReseedAsync(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        _leases.Remove(treeId);
        return Task.CompletedTask;
    }

    private HybridLogicalClock Aggregate(IReadOnlyCollection<string> trees, DateTimeOffset now)
    {
        if (trees.Count == 0)
        {
            return HybridLogicalClock.Zero;
        }

        HybridLogicalClock? aggregate = null;
        foreach (var tree in trees)
        {
            if (!_reports.TryGetValue(tree, out var report)
                || now - report.At > ReportTtl
                || report.Watermark == HybridLogicalClock.Zero)
            {
                return HybridLogicalClock.Zero;
            }

            if (aggregate is not { } current || report.Watermark.CompareTo(current) < 0)
            {
                aggregate = report.Watermark;
            }
        }

        return aggregate ?? HybridLogicalClock.Zero;
    }
}
