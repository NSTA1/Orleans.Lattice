using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Round-trip every state-API DTO through Orleans serialization to prove the
/// wire model is coherent and stable across a grain / transport boundary.
/// </summary>
[TestFixture]
public sealed class StateDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void TreeConfigSummary_round_trips()
    {
        var original = new TreeConfigSummary
        {
            ShardCount = 4,
            VirtualShardCount = 4096,
            MaxLeafKeys = 32,
            MaxInternalChildren = 16,
            WalPartitions = 8,
            SoftDeleteDuration = TimeSpan.FromHours(2),
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void TreeStateSummary_round_trips()
    {
        var original = new TreeStateSummary
        {
            TreeId = "tree-a",
            Lifecycle = TreeLifecycleState.SoftDeleted,
            ShardCount = 4,
            TotalLiveKeys = 1234,
            TombstoneCount = 56,
            MinDepth = 1,
            MaxDepth = 3,
            ShardsSplitting = 1,
            Config = new TreeConfigSummary { ShardCount = 4, VirtualShardCount = 4096 },
            SampledAt = DateTimeOffset.UnixEpoch,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void ShardStateSummary_round_trips()
    {
        var original = new ShardStateSummary
        {
            ShardIndex = 2,
            Depth = 3,
            RootIsLeaf = false,
            LiveKeys = 500,
            Tombstones = 9,
            OpsPerSecond = 12.5,
            SplitInProgress = true,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void NodeStateSummary_round_trips_with_children()
    {
        var original = new NodeStateSummary
        {
            Kind = NodeKind.ShardRoot,
            NodeId = "root-0",
            ShardIndex = 0,
            Depth = 0,
            KeyRangeLow = "a",
            KeyRangeHigh = "m",
            ChildCount = 1,
            SubtreeKeyCount = 100,
            SubtreeTombstoneCount = 2,
            HasMoreChildren = true,
            Children = new[]
            {
                new NodeStateSummary { Kind = NodeKind.Leaf, NodeId = "leaf-0", ShardIndex = 0, Depth = 1 },
            },
        };

        var copy = RoundTrip(original);
        Assert.That(copy.NodeId, Is.EqualTo("root-0"));
        Assert.That(copy.Children, Has.Count.EqualTo(1));
        Assert.That(copy.Children[0].NodeId, Is.EqualTo("leaf-0"));
    }

    [Test]
    public void ViewStateSummary_round_trips()
    {
        var original = new ViewStateSummary
        {
            ViewName = "view-a",
            SourceTreeId = "tree-a",
            Lag = 42,
            EntryCount = 7,
            LastDigest = "deadbeef",
            IsAggregation = true,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void ViewStateSummary_round_trips_with_unsampled_stats()
    {
        var original = new ViewStateSummary
        {
            ViewName = "view-b",
            SourceTreeId = "tree-b",
            Lag = null,
            EntryCount = null,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void CatalogRequest_round_trips()
    {
        var original = new CatalogRequest
        {
            PageSize = 25,
            PageToken = "tree-k",
            IncludeSystemTrees = true,
            IncludeViewStats = true,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void TreeCatalogEntry_round_trips()
    {
        var original = new TreeCatalogEntry
        {
            TreeId = "tree-a",
            IsAlias = true,
            PhysicalTreeId = "tree-a-phys",
            Lifecycle = TreeLifecycleState.SoftDeleted,
            ShardCount = 8,
            Config = new TreeConfigSummary { ShardCount = 8, VirtualShardCount = 4096, MaxLeafKeys = 64 },
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void TreeCatalogPage_round_trips()
    {
        var original = new TreeCatalogPage
        {
            Entries = new[]
            {
                new TreeCatalogEntry
                {
                    TreeId = "tree-a",
                    ShardCount = 4,
                    Config = new TreeConfigSummary { ShardCount = 4, VirtualShardCount = 4096 },
                },
            },
            NextPageToken = "tree-a",
        };

        var copy = RoundTrip(original);
        Assert.That(copy.NextPageToken, Is.EqualTo("tree-a"));
        Assert.That(copy.Entries, Has.Count.EqualTo(1));
        Assert.That(copy.Entries[0].TreeId, Is.EqualTo("tree-a"));
    }

    [Test]
    public void ViewCatalogPage_round_trips()
    {
        var original = new ViewCatalogPage
        {
            Entries = new[]
            {
                new ViewStateSummary { ViewName = "view-a", SourceTreeId = "tree-a" },
            },
            NextPageToken = null,
        };

        var copy = RoundTrip(original);
        Assert.That(copy.NextPageToken, Is.Null);
        Assert.That(copy.Entries, Has.Count.EqualTo(1));
        Assert.That(copy.Entries[0].ViewName, Is.EqualTo("view-a"));
    }

    [Test]
    public void EntryRecord_round_trips()
    {
        var original = new EntryRecord
        {
            Key = "k1",
            ValuePreview = new byte[] { 1, 2, 3 },
            ValueLength = 10,
            Truncated = true,
            Hlc = HybridLogicalClock.Zero,
            IsTombstone = false,
            ExpiresAtTicks = 123456,
            CrdtShape = "OrSet",
        };

        var copy = RoundTrip(original);
        Assert.That(copy.Key, Is.EqualTo("k1"));
        Assert.That(copy.ValuePreview, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(copy.ValueLength, Is.EqualTo(10));
        Assert.That(copy.Truncated, Is.True);
        Assert.That(copy.ExpiresAtTicks, Is.EqualTo(123456));
        Assert.That(copy.CrdtShape, Is.EqualTo("OrSet"));
    }

    [Test]
    public void StructureRequest_round_trips()
    {
        var original = new StructureRequest
        {
            TreeId = "tree-a",
            ShardIndex = 3,
            SubPathNodeId = "node-7",
            DepthLimit = 5,
            MaxNodes = 250,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void EntryScanRequest_round_trips()
    {
        var original = new EntryScanRequest
        {
            TreeId = "tree-a",
            StartInclusive = "a",
            EndExclusive = "m",
            Reverse = true,
            PageSize = 64,
            ContinuationToken = "cursor-1",
            ValuePreviewBudget = 512,
            Predicate = LatticePredicateTranslator.Translate<ScanPerson>(p => p.Age >= 18),
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.StartInclusive, Is.EqualTo("a"));
            Assert.That(copy.EndExclusive, Is.EqualTo("m"));
            Assert.That(copy.Reverse, Is.True);
            Assert.That(copy.PageSize, Is.EqualTo(64));
            Assert.That(copy.ContinuationToken, Is.EqualTo("cursor-1"));
            Assert.That(copy.ValuePreviewBudget, Is.EqualTo(512));
            Assert.That(copy.Predicate, Is.Not.Null);
        });
    }

    [Test]
    public void StateObserveRequest_round_trips()
    {
        var original = new StateObserveRequest
        {
            TreeId = "tree-a",
            StartInclusive = "a",
            EndExclusive = "m",
            ContinuationToken = "cursor-1",
            IncludeMaintenance = true,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void StateChangeNotification_round_trips()
    {
        var original = new StateChangeNotification
        {
            TreeId = "tree-a",
            Key = "k1",
            EndExclusiveKey = "k9",
            Kind = StateChangeKind.DeleteRange,
            Hlc = HybridLogicalClock.Zero,
            Category = MutationCategory.Maintenance,
            Position = "1|7|9",
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void TreeMetricsRequest_round_trips()
    {
        var original = new TreeMetricsRequest
        {
            TreeIds = new[] { "tree-a", "tree-b" },
            IncludeShardHotness = true,
            IncludeViewLag = true,
            IncludeSystemTrees = true,
            SampleInterval = TimeSpan.FromMilliseconds(500),
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeIds, Is.EqualTo(new[] { "tree-a", "tree-b" }));
            Assert.That(copy.IncludeShardHotness, Is.True);
            Assert.That(copy.IncludeViewLag, Is.True);
            Assert.That(copy.IncludeSystemTrees, Is.True);
            Assert.That(copy.SampleInterval, Is.EqualTo(TimeSpan.FromMilliseconds(500)));
        });
    }

    [Test]
    public void TreeMetricsSnapshot_round_trips_with_deltas()
    {
        var original = new TreeMetricsSnapshot
        {
            SampledAt = DateTimeOffset.UnixEpoch,
            IsInitial = false,
            Trees = new[]
            {
                new TreeMetrics
                {
                    TreeId = "tree-a",
                    Lifecycle = TreeLifecycleState.Active,
                    ShardCount = 2,
                    LiveKeys = 100,
                    Tombstones = 3,
                    MinDepth = 1,
                    MaxDepth = 2,
                    ShardsSplitting = 1,
                    ViewCount = 1,
                    ViewLagTotal = 7,
                    ShardHotness = new[]
                    {
                        new ShardHotness { ShardIndex = 0, OpsPerSecond = 12.5, LiveKeys = 50, SplitInProgress = true },
                    },
                },
            },
            RemovedTreeIds = new[] { "tree-z" },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.IsInitial, Is.False);
            Assert.That(copy.RemovedTreeIds, Is.EqualTo(new[] { "tree-z" }));
            Assert.That(copy.Trees, Has.Count.EqualTo(1));
            Assert.That(copy.Trees[0].TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.Trees[0].ViewLagTotal, Is.EqualTo(7));
            Assert.That(copy.Trees[0].ShardHotness, Has.Count.EqualTo(1));
            Assert.That(copy.Trees[0].ShardHotness[0].OpsPerSecond, Is.EqualTo(12.5));
        });
    }
}
