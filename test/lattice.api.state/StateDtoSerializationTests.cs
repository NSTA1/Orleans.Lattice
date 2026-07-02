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
            IsHistory = true,
        };

        var roundTripped = RoundTrip(original);
        Assert.That(roundTripped, Is.EqualTo(original));
        Assert.That(roundTripped.IsHistory, Is.True, "the additive IsHistory flag must survive the wire round-trip");
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
            SourceTreeId = "orders",
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
    public void TagIndexStateSummary_round_trips()
    {
        var original = new TagIndexStateSummary
        {
            IndexName = "orders-by-status",
            TreeId = "tag-orders-by-status",
            ShardCount = 3,
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void TagIndexCatalogPage_round_trips()
    {
        var original = new TagIndexCatalogPage
        {
            Entries = new[]
            {
                new TagIndexStateSummary { IndexName = "idx-a", TreeId = "tag-idx-a", ShardCount = 2 },
            },
            NextPageToken = "tag-idx-a",
        };

        var copy = RoundTrip(original);
        Assert.That(copy.NextPageToken, Is.EqualTo("tag-idx-a"));
        Assert.That(copy.Entries, Has.Count.EqualTo(1));
        Assert.That(copy.Entries[0].IndexName, Is.EqualTo("idx-a"));
    }

    [Test]
    public void CoveredTreeCatalogPage_round_trips()
    {
        var original = new CoveredTreeCatalogPage
        {
            Entries = new[] { "eu", "us", "za" },
            NextPageToken = "za",
        };

        var copy = RoundTrip(original);
        Assert.That(copy.Entries, Is.EqualTo(new[] { "eu", "us", "za" }));
        Assert.That(copy.NextPageToken, Is.EqualTo("za"));
    }

    [Test]
    public void TagMember_round_trips()
    {
        var original = new TagMember { TreeId = "eu", Key = "key-00001" };

        var copy = RoundTrip(original);
        Assert.That(copy, Is.EqualTo(original));
    }

    [Test]
    public void TagMemberScanRequest_round_trips()
    {
        var original = new TagMemberScanRequest
        {
            IndexName = "by-status",
            Tag = "open",
            PageSize = 250,
            PageToken = "eu\0key-00001",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.IndexName, Is.EqualTo("by-status"));
            Assert.That(copy.Tag, Is.EqualTo("open"));
            Assert.That(copy.PageSize, Is.EqualTo(250));
            Assert.That(copy.PageToken, Is.EqualTo("eu\0key-00001"));
        });
    }

    [Test]
    public void TagMemberScanPage_round_trips()
    {
        var original = new TagMemberScanPage
        {
            Entries = new[]
            {
                new TagMember { TreeId = "eu", Key = "key-00001" },
                new TagMember { TreeId = "us", Key = "key-00002" },
            },
            NextPageToken = "us\0key-00002",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Entries, Has.Count.EqualTo(2));
            Assert.That(copy.Entries[0], Is.EqualTo(new TagMember { TreeId = "eu", Key = "key-00001" }));
            Assert.That(copy.Entries[1].Key, Is.EqualTo("key-00002"));
            Assert.That(copy.NextPageToken, Is.EqualTo("us\0key-00002"));
        });
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
            CurrentMembers = new[]
            {
                new CrdtMemberValue
                {
                    Element = new byte[] { 7, 8 },
                    ReplicaId = "rA",
                    Ordinal = 9,
                },
            },
        };

        var copy = RoundTrip(original);
        Assert.That(copy.Key, Is.EqualTo("k1"));
        Assert.That(copy.ValuePreview, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(copy.ValueLength, Is.EqualTo(10));
        Assert.That(copy.Truncated, Is.True);
        Assert.That(copy.ExpiresAtTicks, Is.EqualTo(123456));
        Assert.That(copy.CrdtShape, Is.EqualTo("OrSet"));
        Assert.That(copy.CurrentMembers, Has.Count.EqualTo(1));
        Assert.That(copy.CurrentMembers[0].Element, Is.EqualTo(new byte[] { 7, 8 }));
        Assert.That(copy.CurrentMembers[0].ReplicaId, Is.EqualTo("rA"));
        Assert.That(copy.CurrentMembers[0].Ordinal, Is.EqualTo(9));
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
            IndexName = "by-status",
            Tag = "open",
            Mode = EntryScanMode.LivePointInTime,
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
            Assert.That(copy.IndexName, Is.EqualTo("by-status"));
            Assert.That(copy.Tag, Is.EqualTo("open"));
            Assert.That(copy.Mode, Is.EqualTo(EntryScanMode.LivePointInTime));
        });
    }

    [Test]
    public void EntryScanRequest_default_mode_is_snapshot()
    {
        // The wire default (an unset field) must decode as Snapshot so an
        // existing caller keeps the released point-in-time semantics.
        var copy = RoundTrip(new EntryScanRequest { TreeId = "tree-a" });
        Assert.That(copy.Mode, Is.EqualTo(EntryScanMode.Snapshot));
    }

    [Test]
    public void EntryScanCancelRequest_round_trips()
    {
        var original = new EntryScanCancelRequest
        {
            TreeId = "tree-a",
            ContinuationToken = "cursor-1",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.ContinuationToken, Is.EqualTo("cursor-1"));
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

    [Test]
    public void EntryHistoryRequest_round_trips()
    {
        var original = new EntryHistoryRequest
        {
            TreeId = "tree-a",
            Key = "k1",
            FromHlc = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 },
            ToHlc = new HybridLogicalClock { WallClockTicks = 900, Counter = 2 },
            Limit = 50,
            ContinuationToken = "cursor-1",
            ValuePreviewBudget = 128,
            Reverse = true,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.Key, Is.EqualTo("k1"));
            Assert.That(copy.FromHlc, Is.EqualTo(original.FromHlc));
            Assert.That(copy.ToHlc, Is.EqualTo(original.ToHlc));
            Assert.That(copy.Limit, Is.EqualTo(50));
            Assert.That(copy.ContinuationToken, Is.EqualTo("cursor-1"));
            Assert.That(copy.ValuePreviewBudget, Is.EqualTo(128));
            Assert.That(copy.Reverse, Is.True);
        });
    }

    [Test]
    public void EntryHistoryRequest_round_trips_with_open_bounds()
    {
        var original = new EntryHistoryRequest { TreeId = "tree-a", Key = "k1" };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.FromHlc, Is.Null);
            Assert.That(copy.ToHlc, Is.Null);
            Assert.That(copy.ContinuationToken, Is.Null);
            Assert.That(copy.Reverse, Is.False);
        });
    }

    [Test]
    public void RevisionRetention_round_trips()
    {
        var original = new RevisionRetention { Mode = HistoryRetentionMode.Hybrid, ValueRetained = true };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void EntryRevisionRecord_round_trips_with_member_changes()
    {
        var original = new EntryRevisionRecord
        {
            Hlc = new HybridLogicalClock { WallClockTicks = 500, Counter = 3 },
            Kind = HistoryRowKind.CrdtDelta,
            Category = MutationCategory.User,
            SourceKey = "k1",
            OriginClusterId = "west",
            ValuePreview = null,
            ValueLength = 0,
            Truncated = false,
            ValueHash = 0,
            Delta = new byte[] { 9, 8, 7 },
            Mode = LatticeMergeMode.OrSet,
            MemberChanges = new[]
            {
                new CrdtMemberChange
                {
                    Element = new byte[] { 1, 2 },
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = "r1",
                    Ordinal = 42,
                    WallClock = new HybridLogicalClock { WallClockTicks = 500, Counter = 3 },
                },
            },
            Retention = new RevisionRetention { Mode = HistoryRetentionMode.FullValue, ValueRetained = true },
            EndKey = null,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Hlc, Is.EqualTo(original.Hlc));
            Assert.That(copy.Kind, Is.EqualTo(HistoryRowKind.CrdtDelta));
            Assert.That(copy.Category, Is.EqualTo(MutationCategory.User));
            Assert.That(copy.SourceKey, Is.EqualTo("k1"));
            Assert.That(copy.OriginClusterId, Is.EqualTo("west"));
            Assert.That(copy.Delta, Is.EqualTo(new byte[] { 9, 8, 7 }));
            Assert.That(copy.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(copy.MemberChanges, Has.Count.EqualTo(1));
            Assert.That(copy.MemberChanges[0].Element, Is.EqualTo(new byte[] { 1, 2 }));
            Assert.That(copy.MemberChanges[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(copy.MemberChanges[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(copy.MemberChanges[0].Ordinal, Is.EqualTo(42));
            Assert.That(copy.Retention.Mode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(copy.Retention.ValueRetained, Is.True);
        });
    }

    [Test]
    public void EntryRevisionRecord_round_trips_metadata_only_set()
    {
        var original = new EntryRevisionRecord
        {
            Hlc = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            Kind = HistoryRowKind.Set,
            Category = MutationCategory.User,
            SourceKey = "k2",
            ValuePreview = null,
            ValueLength = 4096,
            Truncated = false,
            ValueHash = unchecked((long)0xDEADBEEFCAFEF00D),
            Mode = LatticeMergeMode.LwwRegister,
            Retention = new RevisionRetention { Mode = HistoryRetentionMode.MetadataOnly, ValueRetained = false },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.ValuePreview, Is.Null);
            Assert.That(copy.ValueLength, Is.EqualTo(4096));
            Assert.That(copy.ValueHash, Is.EqualTo(original.ValueHash));
            Assert.That(copy.MemberChanges, Is.Empty);
            Assert.That(copy.Retention.ValueRetained, Is.False);
        });
    }

    [Test]
    public void EntryHistoryBound_round_trips_every_value()
    {
        foreach (var value in Enum.GetValues<EntryHistoryBound>())
        {
            Assert.That(RoundTrip(value), Is.EqualTo(value));
        }
    }
}
