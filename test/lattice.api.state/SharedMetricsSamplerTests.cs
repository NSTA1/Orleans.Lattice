using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Unit coverage for <see cref="SharedMetricsSampler"/>'s per-tree sampling
/// contract: the tile aggregates and per-shard hotness rows are derived from a
/// single deep per-shard walk (no redundant second fan-out), and a tree
/// reporting WAL saturation has its detail paused - the sampler skips the walk
/// entirely and serves a degraded, fan-out-free snapshot.
/// </summary>
[TestFixture]
public partial class SharedMetricsSamplerTests
{
    private const string TreeId = "sampler-tree";

    [Test]
    public async Task Sample_derives_tiles_and_hotness_from_a_single_shard_walk()
    {
        var query = new RecordingStateQuery
        {
            Shards =
            {
                [TreeId] = new[]
                {
                    Shard(index: 0, depth: 2, liveKeys: 10, tombstones: 3, opsPerSecond: 4.5, splitting: false),
                    Shard(index: 1, depth: 3, liveKeys: 20, tombstones: 1, opsPerSecond: 9.0, splitting: true),
                },
            },
        };

        var sampler = CreateSampler(query, signal: null);

        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = new[] { TreeId }, IncludeShardHotness = true },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Exactly one deep per-shard walk backs both tiles and hotness; the
            // old tree-summary fan-out is gone entirely.
            Assert.That(query.ShardSummaryCalls, Is.EqualTo(1), "one shard walk");
            Assert.That(query.DeepRequests, Is.EqualTo(new[] { true }), "walk is deep");
            Assert.That(query.TreeSummaryCalls, Is.Zero, "no redundant tree-summary fan-out");
            Assert.That(query.ShardCountCalls, Is.Zero, "no routing read on the healthy path");
        });

        var metrics = result[TreeId];
        Assert.Multiple(() =>
        {
            Assert.That(metrics.DetailPaused, Is.False);
            Assert.That(metrics.ShardCount, Is.EqualTo(2));
            Assert.That(metrics.LiveKeys, Is.EqualTo(30));
            Assert.That(metrics.Tombstones, Is.EqualTo(4));
            Assert.That(metrics.MinDepth, Is.EqualTo(2));
            Assert.That(metrics.MaxDepth, Is.EqualTo(3));
            Assert.That(metrics.ShardsSplitting, Is.EqualTo(1));
            Assert.That(metrics.Lifecycle, Is.EqualTo(TreeLifecycleState.Active));
            Assert.That(metrics.ShardHotness, Has.Count.EqualTo(2));
            Assert.That(metrics.ShardHotness[1].ShardIndex, Is.EqualTo(1));
            Assert.That(metrics.ShardHotness[1].OpsPerSecond, Is.EqualTo(9.0));
            Assert.That(metrics.ShardHotness[1].LiveKeys, Is.EqualTo(20));
            Assert.That(metrics.ShardHotness[1].SplitInProgress, Is.True);
        });
    }

    [Test]
    public async Task Sample_without_hotness_still_walks_shards_once_and_omits_hotness()
    {
        var query = new RecordingStateQuery
        {
            Shards = { [TreeId] = new[] { Shard(index: 0, depth: 1, liveKeys: 5, tombstones: 0, opsPerSecond: 1.0, splitting: false) } },
        };

        var sampler = CreateSampler(query, signal: null);

        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = new[] { TreeId }, IncludeShardHotness = false },
            CancellationToken.None);

        Assert.That(query.ShardSummaryCalls, Is.EqualTo(1));
        Assert.That(result[TreeId].LiveKeys, Is.EqualTo(5));
        Assert.That(result[TreeId].ShardHotness, Is.Empty);
    }

    [Test]
    public async Task Sample_saturated_tree_pauses_detail_without_walking_shards()
    {
        var query = new RecordingStateQuery
        {
            Shards = { [TreeId] = new[] { Shard(index: 0, depth: 4, liveKeys: 99, tombstones: 9, opsPerSecond: 50.0, splitting: true) } },
            ShardCounts = { [TreeId] = 7 },
        };

        var signal = new StubSaturationSignal();
        signal.Set(TreeId, WalSaturationState.Saturated);

        var sampler = CreateSampler(query, signal);

        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = new[] { TreeId }, IncludeShardHotness = true },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            // The saturated tree's shard roots are never walked; only the cheap
            // routing read runs.
            Assert.That(query.ShardSummaryCalls, Is.Zero, "no per-shard walk under saturation");
            Assert.That(query.ShardCountCalls, Is.EqualTo(1), "one fan-out-free routing read");
        });

        var metrics = result[TreeId];
        Assert.Multiple(() =>
        {
            Assert.That(metrics.DetailPaused, Is.True);
            Assert.That(metrics.ShardCount, Is.EqualTo(7), "shard count from routing");
            Assert.That(metrics.Lifecycle, Is.EqualTo(TreeLifecycleState.Active));
            Assert.That(metrics.LiveKeys, Is.Zero);
            Assert.That(metrics.Tombstones, Is.Zero);
            Assert.That(metrics.ShardsSplitting, Is.Zero);
            Assert.That(metrics.ShardHotness, Is.Empty);
        });
    }

    [Test]
    public async Task Sample_throttled_tree_is_not_paused()
    {
        var query = new RecordingStateQuery
        {
            Shards = { [TreeId] = new[] { Shard(index: 0, depth: 2, liveKeys: 12, tombstones: 0, opsPerSecond: 3.0, splitting: false) } },
        };

        var signal = new StubSaturationSignal();
        signal.Set(TreeId, WalSaturationState.Throttled);

        var sampler = CreateSampler(query, signal);

        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = new[] { TreeId }, IncludeShardHotness = true },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(query.ShardSummaryCalls, Is.EqualTo(1), "throttled still walks");
            Assert.That(result[TreeId].DetailPaused, Is.False);
            Assert.That(result[TreeId].LiveKeys, Is.EqualTo(12));
        });
    }

    private static SharedMetricsSampler CreateSampler(ILatticeStateQuery query, IWalSaturationSignal? signal)
    {
        var services = new ServiceCollection();
        if (signal is not null)
        {
            services.AddSingleton(signal);
        }

        return new SharedMetricsSampler(
            query,
            Options.Create(new LatticeApiStateOptions()),
            services.BuildServiceProvider());
    }

    private static ShardStateSummary Shard(int index, int depth, long liveKeys, long tombstones, double opsPerSecond, bool splitting) => new()
    {
        ShardIndex = index,
        Depth = depth,
        RootIsLeaf = depth <= 1,
        LiveKeys = liveKeys,
        Tombstones = tombstones,
        OpsPerSecond = opsPerSecond,
        SplitInProgress = splitting,
    };

    private sealed class RecordingStateQuery : ILatticeStateQuery
    {
        public Dictionary<string, ShardStateSummary[]> Shards { get; } = new(StringComparer.Ordinal);

        public Dictionary<string, int> ShardCounts { get; } = new(StringComparer.Ordinal);

        public int TreeSummaryCalls { get; private set; }

        public int ShardSummaryCalls { get; private set; }

        public int ShardCountCalls { get; private set; }

        public List<bool> DeepRequests { get; } = new();

        public Task<TreeSummaryResult> GetTreeSummaryAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
        {
            TreeSummaryCalls++;
            return Task.FromResult(TreeSummaryResult.NotFound(treeId));
        }

        public Task<ShardSummariesResult> GetShardSummariesAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
        {
            ShardSummaryCalls++;
            DeepRequests.Add(deep);
            return Task.FromResult(Shards.TryGetValue(treeId, out var shards)
                ? ShardSummariesResult.Found(treeId, shards)
                : ShardSummariesResult.NotFound(treeId));
        }

        public Task<int?> GetPhysicalShardCountAsync(string treeId, CancellationToken cancellationToken = default)
        {
            ShardCountCalls++;
            return Task.FromResult(ShardCounts.TryGetValue(treeId, out var count) ? count : (int?)null);
        }

        public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<TreeStructureResult> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<EntryScanResult> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<EntryDetailResult> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<EntryHistoryResult> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<int> GetDeadLetterCountAsync(string treeId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }

    private sealed class StubSaturationSignal : IWalSaturationSignal
    {
        private readonly Dictionary<string, WalSaturationState> _states = new(StringComparer.Ordinal);

        public void Set(string treeId, WalSaturationState state) => _states[treeId] = state;

        public WalSaturationState GetCurrentState(string treeId)
            => _states.TryGetValue(treeId, out var state) ? state : WalSaturationState.Healthy;

        public WalSaturationState GetAggregateState()
            => _states.Count == 0 ? WalSaturationState.Healthy : _states.Values.Max();

        public Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }
}
