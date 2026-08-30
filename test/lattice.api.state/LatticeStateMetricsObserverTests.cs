using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeStateMetricsObserver"/>'s delta diff - the
/// per-subscriber half of the metrics feed that turns the shared sampler's full
/// per-tree map into an incremental snapshot.
///
/// The diff is what keeps a live dashboard cheap, so its two failure modes both
/// matter: reporting a tree as changed when nothing moved makes the feed as
/// expensive as a full poll, while missing a real change (a tree disappearing, or
/// per-shard hotness moving while every tree-level total stays identical) leaves
/// a stale dashboard that looks healthy.
/// </summary>
[TestFixture]
public sealed class LatticeStateMetricsObserverTests
{
    private const string TreeA = "metrics-tree-a";
    private const string TreeB = "metrics-tree-b";

    private static (LatticeStateMetricsObserver Observer, MutableStateQuery Query) Create()
    {
        var query = new MutableStateQuery();
        var sampler = new SharedMetricsSampler(
            query,
            Options.Create(new LatticeApiStateOptions()),
            new ServiceCollection().BuildServiceProvider());
        return (new LatticeStateMetricsObserver(sampler), query);
    }

    private static TreeMetricsRequest Request(bool hotness = false) => new()
    {
        IncludeShardHotness = hotness,
        SampleInterval = TimeSpan.FromMilliseconds(20),
    };

    [Test]
    public async Task SampleAsync_returns_an_initial_snapshot_ordered_by_tree_id()
    {
        var (observer, query) = Create();
        query.SetTrees(TreeB, TreeA);

        var snapshot = await observer.SampleAsync(Request(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.IsInitial, Is.True);
            Assert.That(snapshot.Trees.Select(t => t.TreeId), Is.EqualTo(new[] { TreeA, TreeB }));
        });
    }

    [Test]
    public async Task SampleAsync_rejects_a_null_request()
    {
        var (observer, _) = Create();

        Assert.That(
            async () => await observer.SampleAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
        await Task.CompletedTask;
    }

    [Test]
    public void ObserveAsync_rejects_a_null_request()
    {
        var (observer, _) = Create();

        Assert.That(
            async () =>
            {
                await foreach (var _ in observer.ObserveAsync(null!, CancellationToken.None))
                {
                    break;
                }
            },
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ObserveAsync_reports_a_vanished_tree_as_removed()
    {
        var (observer, query) = Create();
        query.SetTrees(TreeA, TreeB);

        var snapshots = await CollectAsync(
            observer,
            Request(),
            afterFirst: () => query.SetTrees(TreeA));

        var delta = snapshots[^1];
        Assert.Multiple(() =>
        {
            Assert.That(snapshots[0].IsInitial, Is.True);
            Assert.That(delta.IsInitial, Is.False);
            Assert.That(delta.RemovedTreeIds, Is.EqualTo(new[] { TreeB }),
                "A dropped tree must be reported explicitly; a subscriber cannot infer it from an absence.");
        });
    }

    [Test]
    public async Task ObserveAsync_orders_removed_tree_ids_by_tree_id()
    {
        var (observer, query) = Create();
        query.SetTrees("metrics-tree-c", TreeB, TreeA);

        var snapshots = await CollectAsync(observer, Request(), afterFirst: () => query.SetTrees());

        Assert.That(snapshots[^1].RemovedTreeIds, Is.EqualTo(new[] { TreeA, TreeB, "metrics-tree-c" }));
    }

    [Test]
    public async Task ObserveAsync_reports_a_tree_whose_only_change_is_shard_hotness()
    {
        var (observer, query) = Create();
        query.SetTrees(TreeA);

        // Every tree-level aggregate stays identical; only the per-shard rate
        // moves, so the diff must fall through to the element-wise hotness
        // comparison rather than short-circuit on the totals.
        var snapshots = await CollectAsync(
            observer,
            Request(hotness: true),
            afterFirst: () => query.OpsPerSecond = 42.0);

        var delta = snapshots[^1];
        Assert.Multiple(() =>
        {
            Assert.That(delta.IsInitial, Is.False);
            Assert.That(delta.Trees.Select(t => t.TreeId), Is.EqualTo(new[] { TreeA }));
            Assert.That(delta.Trees[0].ShardHotness[0].OpsPerSecond, Is.EqualTo(42.0));
        });
    }

    [Test]
    public async Task ObserveAsync_emits_an_empty_delta_when_nothing_changed()
    {
        var (observer, query) = Create();
        query.SetTrees(TreeA);

        var snapshots = await CollectAsync(observer, Request(), afterFirst: () => { });

        var delta = snapshots[^1];
        Assert.Multiple(() =>
        {
            Assert.That(delta.IsInitial, Is.False);
            Assert.That(delta.Trees, Is.Empty,
                "An unchanged tree must not be re-sent, otherwise the delta feed costs as much as a full poll.");
            Assert.That(delta.RemovedTreeIds, Is.Empty);
        });
    }

    /// <summary>
    /// Reads the initial snapshot, applies <paramref name="afterFirst"/>, then
    /// reads until a delta snapshot arrives.
    /// </summary>
    private static async Task<List<TreeMetricsSnapshot>> CollectAsync(
        LatticeStateMetricsObserver observer,
        TreeMetricsRequest request,
        Action afterFirst)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var snapshots = new List<TreeMetricsSnapshot>();

        await foreach (var snapshot in observer.ObserveAsync(request, timeout.Token))
        {
            snapshots.Add(snapshot);
            if (snapshots.Count == 1)
            {
                afterFirst();
                continue;
            }

            break;
        }

        Assert.That(snapshots, Has.Count.GreaterThanOrEqualTo(2), "expected an initial snapshot and a delta");
        return snapshots;
    }

    /// <summary>
    /// A state query whose visible tree set and per-shard rate can be changed
    /// between sampling ticks, so the observer's diff sees real movement.
    /// </summary>
    private sealed class MutableStateQuery : ILatticeStateQuery
    {
        private volatile string[] _trees = [];

        public double OpsPerSecond { get; set; }

        public void SetTrees(params string[] trees) => _trees = trees;

        public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new TreeCatalogPage
            {
                Entries = _trees
                    .Select(id => new TreeCatalogEntry { TreeId = id, Config = new TreeConfigSummary() })
                    .ToList(),
            });

        public Task<ShardSummariesResult> GetShardSummariesAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
            => Task.FromResult(ShardSummariesResult.Found(treeId, new[]
            {
                new ShardStateSummary
                {
                    ShardIndex = 0,
                    Depth = 1,
                    RootIsLeaf = true,
                    LiveKeys = 4,
                    Tombstones = 0,
                    OpsPerSecond = OpsPerSecond,
                    SplitInProgress = false,
                },
            }));

        public Task<int?> GetPhysicalShardCountAsync(string treeId, CancellationToken cancellationToken = default)
            => Task.FromResult((int?)null);

        public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ViewCatalogPage());

        public Task<TreeSummaryResult> GetTreeSummaryAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)
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

        public Task CancelScanAsync(string treeId, string? cursor = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<EntryDetailResult> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<EntryHistoryResult> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<int> GetDeadLetterCountAsync(string treeId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
}
