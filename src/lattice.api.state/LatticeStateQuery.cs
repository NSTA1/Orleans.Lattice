using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Default <see cref="ILatticeStateQuery"/> implementation. Registered as a
/// silo singleton by <c>AddLatticeStateApi</c>; it dials the core
/// <see cref="ILattice"/> grain surface via the cluster grain factory and
/// resolves effective options via the named-options monitor.
/// </summary>
internal sealed class LatticeStateQuery(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> options) : ILatticeStateQuery
{
    private readonly IGrainFactory _grainFactory = grainFactory
        ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly IOptionsMonitor<LatticeOptions> _options = options
        ?? throw new ArgumentNullException(nameof(options));

    public async Task<TreeSummaryResult> GetTreeSummaryAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        if (!await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return TreeSummaryResult.NotFound(treeId);
        }

        var report = await tree.DiagnoseAsync(deep, cancellationToken).ConfigureAwait(false);
        return TreeSummaryResult.Found(MapTree(treeId, report, BuildConfig(treeId, report)));
    }

    public async Task<ShardSummariesResult> GetShardSummariesAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        if (!await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return ShardSummariesResult.NotFound(treeId);
        }

        var report = await tree.DiagnoseAsync(deep, cancellationToken).ConfigureAwait(false);
        var shards = report.Shards.IsDefault
            ? Array.Empty<ShardStateSummary>()
            : report.Shards
                .OrderBy(s => s.ShardIndex)
                .Select(MapShard)
                .ToArray();

        return ShardSummariesResult.Found(treeId, shards);
    }

    private TreeConfigSummary BuildConfig(string treeId, TreeDiagnosticReport report)
    {
        var opts = _options.Get(treeId);
        return new TreeConfigSummary
        {
            ShardCount = report.ShardCount,
            VirtualShardCount = report.VirtualShardCount,
            WalPartitions = opts.WalPartitions,
            SoftDeleteDuration = opts.SoftDeleteDuration,
        };
    }

    private static TreeStateSummary MapTree(string treeId, TreeDiagnosticReport report, TreeConfigSummary config)
    {
        var minDepth = 0;
        var maxDepth = 0;
        var splitting = 0;
        if (!report.Shards.IsDefaultOrEmpty)
        {
            minDepth = int.MaxValue;
            foreach (var shard in report.Shards)
            {
                if (shard.Depth < minDepth) minDepth = shard.Depth;
                if (shard.Depth > maxDepth) maxDepth = shard.Depth;
                if (shard.SplitInProgress) splitting++;
            }
        }

        return new TreeStateSummary
        {
            TreeId = treeId,
            Lifecycle = TreeLifecycleState.Active,
            ShardCount = report.ShardCount,
            TotalLiveKeys = report.TotalLiveKeys,
            TombstoneCount = report.TotalTombstones,
            MinDepth = minDepth == int.MaxValue ? 0 : minDepth,
            MaxDepth = maxDepth,
            ShardsSplitting = splitting,
            Config = config,
            SampledAt = report.SampledAt,
        };
    }

    private static ShardStateSummary MapShard(ShardDiagnosticReport shard) => new()
    {
        ShardIndex = shard.ShardIndex,
        Depth = shard.Depth,
        RootIsLeaf = shard.RootIsLeaf,
        LiveKeys = shard.LiveKeys,
        Tombstones = shard.Tombstones,
        OpsPerSecond = shard.OpsPerSecond,
        SplitInProgress = shard.SplitInProgress,
    };
}
