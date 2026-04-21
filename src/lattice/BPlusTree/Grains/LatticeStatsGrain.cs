using System.Collections.Immutable;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree diagnostics aggregator. Normal single-activation grain keyed by
/// <c>treeId</c>. Fans out to every physical shard on cache-miss, caching
/// each mode (shallow/deep) for <see cref="LatticeOptions.DiagnosticsCacheTtl"/>.
/// Also retains a bounded ring buffer of recent adaptive-split events.
/// </summary>
internal sealed class LatticeStatsGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    LatticeOptionsResolver optionsResolver,
    ILogger<LatticeStatsGrain> logger) : ILatticeStats
{
    /// <summary>Maximum number of recent-split events retained.</summary>
    internal const int RecentSplitsCapacity = 32;

    private string TreeId => context.GrainId.Key.ToString()!;

    private TreeDiagnosticReport? _cachedShallow;
    private TreeDiagnosticReport? _cachedDeep;
    private readonly Queue<RecentSplit> _recentSplits = new();

    /// <inheritdoc />
    public async Task<TreeDiagnosticReport> GetReportAsync(bool deep, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var options = await optionsResolver.ResolveAsync(TreeId);
        var ttl = options.DiagnosticsCacheTtl;
        var now = DateTimeOffset.UtcNow;

        var cached = deep ? _cachedDeep : _cachedShallow;
        if (cached is { } c && ttl > TimeSpan.Zero && (now - c.SampledAt) < ttl)
        {
            return c;
        }

        var report = await BuildReportAsync(deep, cancellationToken);

        if (deep) _cachedDeep = report;
        else _cachedShallow = report;

        return report;
    }

    /// <inheritdoc />
    public Task RecordSplitAsync(int shardIndex, DateTime atUtc)
    {
        if (_recentSplits.Count >= RecentSplitsCapacity) _recentSplits.Dequeue();
        _recentSplits.Enqueue(new RecentSplit { ShardIndex = shardIndex, AtUtc = atUtc });

        // Invalidate cached reports so the next caller sees the fresh split history.
        _cachedShallow = null;
        _cachedDeep = null;
        return Task.CompletedTask;
    }

    private async Task<TreeDiagnosticReport> BuildReportAsync(bool deep, CancellationToken cancellationToken)
    {
        // Resolve routing (physical tree ID + shard map) via the public entry point
        // so registry-alias resolution is handled uniformly.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        var routing = await lattice.GetRoutingAsync(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();

        var physicalShardIndices = routing.Map.GetPhysicalShardIndices();
        var virtualShardCount = routing.Map.VirtualShardCount;

        var tasks = new Task<ShardDiagnosticReport>[physicalShardIndices.Count];
        for (var i = 0; i < physicalShardIndices.Count; i++)
        {
            var shardIndex = physicalShardIndices[i];
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}");
            tasks[i] = GetShardDiagnosticsAsync(shard, shardIndex, deep);
        }

        var shardReports = await Task.WhenAll(tasks);
        cancellationToken.ThrowIfCancellationRequested();

        var sortedShards = shardReports.OrderBy(s => s.ShardIndex).ToImmutableArray();

        long totalLive = 0;
        long totalTombstones = 0;
        foreach (var s in sortedShards)
        {
            totalLive += s.LiveKeys;
            totalTombstones += s.Tombstones;
        }

        return new TreeDiagnosticReport
        {
            TreeId = TreeId,
            ShardCount = sortedShards.Length,
            VirtualShardCount = virtualShardCount,
            TotalLiveKeys = totalLive,
            TotalTombstones = totalTombstones,
            Shards = sortedShards,
            RecentSplits = _recentSplits.ToImmutableArray(),
            SampledAt = DateTimeOffset.UtcNow,
            Deep = deep,
        };
    }

    private async Task<ShardDiagnosticReport> GetShardDiagnosticsAsync(IShardRootGrain shard, int shardIndex, bool deep)
    {
        try
        {
            var report = await shard.GetDiagnosticsAsync(deep);
            return report with { ShardIndex = shardIndex };
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Diagnostics fan-out failed for shard {ShardIndex} in tree {TreeId}", shardIndex, TreeId);
            return new ShardDiagnosticReport { ShardIndex = shardIndex };
        }
    }
}
