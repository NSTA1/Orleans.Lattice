using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the diagnostics aggregation on <see cref="ShardRootGrain"/>
/// (<c>ShardRootGrain.Diagnostics.cs</c>): the operator-driven deep walk
/// <c>RefreshLeafByteFootprintsAsync</c> - which descends the leftmost path to
/// the leaf level, walks the leaf sibling chain, sums each leaf's state and
/// snapshot footprint, and re-anchors the activation-scoped running totals -
/// and the shape-dependent branches of <c>GetDiagnosticsAsync</c>.
/// <para>
/// The deep walk is the self-heal for the O(1) <c>GetStorageUsageAsync</c>
/// read path: the cached totals drift whenever a leaf stops publishing (a
/// reactivation, a lost publish), and only this walk re-anchors them to an
/// authoritative figure. It is driven with substituted node grains so every
/// tree shape - no root, a leaf root, and a two-level internal root over a
/// multi-leaf chain - is deterministic and needs no cluster.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainDiagnosticsTests
{
    private const string TreeId = "tree-a";

    /// <summary>
    /// Builds a grain over a substituted factory, plus the lookup tables the
    /// tests seed so a node id resolves to a specific leaf, internal node, or
    /// snapshot storage double.
    /// </summary>
    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }

        public required IGrainFactory Factory { get; init; }

        public required FakePersistentState<ShardRootState> State { get; init; }

        public required Dictionary<Guid, IBPlusLeafGrain> Leaves { get; init; }

        public required Dictionary<Guid, IBPlusInternalGrain> Internals { get; init; }

        public required Dictionary<Guid, ILeafSnapshotStorageGrain> Snapshots { get; init; }

        /// <summary>
        /// Registers a leaf whose stats report <paramref name="stateBytes"/> /
        /// <paramref name="liveKeys"/> / <paramref name="tombstones"/>, whose
        /// persisted snapshot is <paramref name="snapshotBytes"/> long, and whose
        /// next sibling is <paramref name="next"/>. Returns its node id.
        /// </summary>
        public GrainId AddLeaf(
            long stateBytes,
            int liveKeys,
            long snapshotBytes,
            GrainId? next = null,
            int tombstones = 0)
        {
            var key = Guid.NewGuid();
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.GetStatsAsync().Returns(Task.FromResult(new LeafStats
            {
                LiveKeys = liveKeys,
                Tombstones = tombstones,
                StateBytes = stateBytes,
            }));
            leaf.CountAsync().Returns(Task.FromResult(liveKeys));
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            Leaves[key] = leaf;

            var snapshot = Substitute.For<ILeafSnapshotStorageGrain>();
            snapshot.GetSnapshotByteSizeAsync(Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(snapshotBytes));
            Snapshots[key] = snapshot;

            return LeafId(key);
        }

        /// <summary>
        /// Registers an internal node whose leftmost child is
        /// <paramref name="leftmostChild"/>. Returns its node id.
        /// </summary>
        public GrainId AddInternal(GrainId leftmostChild, bool childrenAreLeaves)
        {
            var key = Guid.NewGuid();
            var node = Substitute.For<IBPlusInternalGrain>();
            node.GetLeftmostChildWithMetadataAsync()
                .Returns(Task.FromResult((leftmostChild, childrenAreLeaves)));
            Internals[key] = node;
            return InternalId(key);
        }
    }

    /// <summary>A leaf node id whose key round-trips through <c>GetGuidKey()</c>.</summary>
    private static GrainId LeafId(Guid key) => GrainId.Create("leaf", key.ToString("N"));

    /// <summary>An internal node id whose key round-trips through <c>GetGuidKey()</c>.</summary>
    private static GrainId InternalId(Guid key) => GrainId.Create("internal", key.ToString("N"));

    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/0"));

        var state = new FakePersistentState<ShardRootState>();
        var factory = Substitute.For<IGrainFactory>();
        var leaves = new Dictionary<Guid, IBPlusLeafGrain>();
        var internals = new Dictionary<Guid, IBPlusInternalGrain>();
        var snapshots = new Dictionary<Guid, ILeafSnapshotStorageGrain>();

        // The diagnostics walk resolves every node by its Guid key, so route the
        // three grain interfaces through the lookup tables the tests seed. An id no
        // test registered would otherwise resolve to a fresh substitute that
        // silently reports zeroes, so an unregistered lookup fails loudly instead.
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
            Resolve(leaves, call.ArgAt<Guid>(0), "leaf"));
        // The shared BoundedLeafWalk these walks now route through (issue 1972)
        // resolves the chain by GrainId, so the same table answers both
        // overloads: a leaf node id's key round-trips through GetGuidKey().
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(call =>
            Resolve(leaves, call.ArgAt<GrainId>(0).GetGuidKey(), "leaf"));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(call =>
            Resolve(internals, call.ArgAt<Guid>(0), "internal node"));
        factory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(call =>
            Resolve(snapshots, call.ArgAt<Guid>(0), "snapshot storage"));

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness
        {
            Grain = grain,
            Factory = factory,
            State = state,
            Leaves = leaves,
            Internals = internals,
            Snapshots = snapshots,
        };
    }

    private static T Resolve<T>(Dictionary<Guid, T> table, Guid key, string what)
        => table.TryGetValue(key, out var grain)
            ? grain
            : throw new InvalidOperationException($"The walk resolved an unregistered {what} '{key}'.");

    [Test]
    public async Task RefreshLeafByteFootprintsAsync_returns_zero_for_a_shard_with_no_root()
    {
        var h = CreateHarness();

        var usage = await h.Grain.RefreshLeafByteFootprintsAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.Zero);
            Assert.That(usage.SnapshotBytes, Is.Zero);
            Assert.That(usage.LiveKeys, Is.Zero);
        });
    }

    [Test]
    public async Task RefreshLeafByteFootprintsAsync_walks_a_leaf_root()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = h.AddLeaf(stateBytes: 120, liveKeys: 4, snapshotBytes: 30);
        h.State.State.RootIsLeaf = true;

        var usage = await h.Grain.RefreshLeafByteFootprintsAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(120L));
            Assert.That(usage.SnapshotBytes, Is.EqualTo(30L));
            Assert.That(usage.LiveKeys, Is.EqualTo(4L));
        });
    }

    [Test]
    public async Task RefreshLeafByteFootprintsAsync_descends_the_leftmost_path_and_sums_the_leaf_chain()
    {
        var h = CreateHarness();
        // Build the chain right-to-left so each leaf can point at its successor.
        var third = h.AddLeaf(stateBytes: 7, liveKeys: 1, snapshotBytes: 3);
        var second = h.AddLeaf(stateBytes: 20, liveKeys: 2, snapshotBytes: 5, next: third);
        var first = h.AddLeaf(stateBytes: 100, liveKeys: 9, snapshotBytes: 11, next: second);
        // A two-level internal root: root -> mid -> leaf chain.
        var mid = h.AddInternal(first, childrenAreLeaves: true);
        h.State.State.RootNodeId = h.AddInternal(mid, childrenAreLeaves: false);
        h.State.State.RootIsLeaf = false;

        var usage = await h.Grain.RefreshLeafByteFootprintsAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(127L));
            Assert.That(usage.SnapshotBytes, Is.EqualTo(19L));
            Assert.That(usage.LiveKeys, Is.EqualTo(12L));
        });
    }

    [Test]
    public async Task RefreshLeafByteFootprintsAsync_re_anchors_the_cached_running_totals()
    {
        var h = CreateHarness();
        var rootLeafId = h.AddLeaf(stateBytes: 120, liveKeys: 4, snapshotBytes: 30);
        h.State.State.RootNodeId = rootLeafId;
        h.State.State.RootIsLeaf = true;

        // Seed a stale cached total, as a reactivation or a lost publish would.
        await h.Grain.PublishLeafByteFootprintAsync(
            Guid.NewGuid(), new LeafByteFootprint { StateBytes = 999, SnapshotBytes = 999, LiveKeys = 999 });
        await h.Grain.RefreshLeafByteFootprintsAsync(CancellationToken.None);

        var usage = await h.Grain.GetStorageUsageAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(120L), "the O(1) read path must see the re-anchored total");
            Assert.That(usage.SnapshotBytes, Is.EqualTo(30L));
            Assert.That(usage.LiveKeys, Is.EqualTo(4L));
        });
    }

    [Test]
    public void RefreshLeafByteFootprintsAsync_observes_cancellation_before_walking()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = h.AddLeaf(stateBytes: 1, liveKeys: 1, snapshotBytes: 1);
        h.State.State.RootIsLeaf = true;
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await h.Grain.RefreshLeafByteFootprintsAsync(cts.Token));
    }

    [Test]
    public void RefreshLeafByteFootprintsAsync_observes_cancellation_mid_leaf_chain()
    {
        var h = CreateHarness();
        using var cts = new CancellationTokenSource();
        var second = h.AddLeaf(stateBytes: 5, liveKeys: 1, snapshotBytes: 0);
        var first = h.AddLeaf(stateBytes: 5, liveKeys: 1, snapshotBytes: 0, next: second);
        // Cancel as soon as the first leaf's snapshot size is read, so the walk is
        // interrupted between chain hops rather than before it starts.
        h.Snapshots[first.GetGuidKey()].GetSnapshotByteSizeAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                cts.Cancel();
                return Task.FromResult(0L);
            });
        h.State.State.RootNodeId = h.AddInternal(first, childrenAreLeaves: true);
        h.State.State.RootIsLeaf = false;

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await h.Grain.RefreshLeafByteFootprintsAsync(cts.Token));
    }

    [Test]
    public void RefreshLeafByteFootprintsAsync_observes_cancellation_while_descending_to_the_leaf_level()
    {
        var h = CreateHarness();
        using var cts = new CancellationTokenSource();
        var leaf = h.AddLeaf(stateBytes: 1, liveKeys: 1, snapshotBytes: 0);
        var mid = h.AddInternal(leaf, childrenAreLeaves: true);
        var rootId = h.AddInternal(mid, childrenAreLeaves: false);
        // Cancel once the root's leftmost child has been read, so the descent is
        // interrupted before it reaches the leaf level.
        h.Internals[rootId.GetGuidKey()].GetLeftmostChildWithMetadataAsync().Returns(_ =>
        {
            cts.Cancel();
            return Task.FromResult((mid, false));
        });
        h.State.State.RootNodeId = rootId;
        h.State.State.RootIsLeaf = false;

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await h.Grain.RefreshLeafByteFootprintsAsync(cts.Token));
    }

    [Test]
    public async Task PublishLeafByteFootprintAsync_Removed_for_an_unknown_leaf_is_a_noop()
    {
        var h = CreateHarness();
        await h.Grain.PublishLeafByteFootprintAsync(
            Guid.NewGuid(), new LeafByteFootprint { StateBytes = 40, SnapshotBytes = 8, LiveKeys = 2 });

        // Retracting a leaf that never published must not subtract its (absent)
        // contribution and drive the running totals negative.
        await h.Grain.PublishLeafByteFootprintAsync(Guid.NewGuid(), LeafByteFootprint.Removed);

        var usage = await h.Grain.GetStorageUsageAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(40L));
            Assert.That(usage.SnapshotBytes, Is.EqualTo(8L));
            Assert.That(usage.LiveKeys, Is.EqualTo(2L));
        });
    }

    [Test]
    public void GetStorageUsageAsync_observes_cancellation()
    {
        var h = CreateHarness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await h.Grain.GetStorageUsageAsync(cts.Token));
    }

    [Test]
    public async Task GetDiagnosticsAsync_reports_an_empty_shard_as_depth_zero()
    {
        var h = CreateHarness();

        var report = await h.Grain.GetDiagnosticsAsync(deep: false);

        Assert.Multiple(() =>
        {
            Assert.That(report.Depth, Is.Zero);
            Assert.That(report.LiveKeys, Is.Zero);
            Assert.That(report.Tombstones, Is.Zero);
            Assert.That(report.TombstoneRatio, Is.Zero);
            Assert.That(report.SplitInProgress, Is.False);
            Assert.That(report.BulkOperationPending, Is.False);
        });
    }

    [Test]
    public async Task GetDiagnosticsAsync_shallow_over_a_leaf_root_counts_without_reading_stats()
    {
        var h = CreateHarness();
        var leafId = h.AddLeaf(stateBytes: 0, liveKeys: 6, snapshotBytes: 0, tombstones: 4);
        h.State.State.RootNodeId = leafId;
        h.State.State.RootIsLeaf = true;

        var report = await h.Grain.GetDiagnosticsAsync(deep: false);

        Assert.Multiple(() =>
        {
            Assert.That(report.Depth, Is.EqualTo(1));
            Assert.That(report.RootIsLeaf, Is.True);
            Assert.That(report.LiveKeys, Is.EqualTo(6L));
            // The shallow pass must not pay for the tombstone-bearing stats read.
            Assert.That(report.Tombstones, Is.Zero);
        });
        await h.Leaves[leafId.GetGuidKey()].DidNotReceive().GetStatsAsync();
    }

    [Test]
    public async Task GetDiagnosticsAsync_deep_over_a_leaf_root_reports_the_tombstone_ratio()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = h.AddLeaf(stateBytes: 0, liveKeys: 6, snapshotBytes: 0, tombstones: 2);
        h.State.State.RootIsLeaf = true;

        var report = await h.Grain.GetDiagnosticsAsync(deep: true);

        Assert.Multiple(() =>
        {
            Assert.That(report.LiveKeys, Is.EqualTo(6L));
            Assert.That(report.Tombstones, Is.EqualTo(2L));
            Assert.That(report.TombstoneRatio, Is.EqualTo(0.25d).Within(1e-9));
        });
    }

    [Test]
    public async Task GetDiagnosticsAsync_deep_over_an_internal_root_aggregates_the_leaf_chain()
    {
        var h = CreateHarness();
        var second = h.AddLeaf(stateBytes: 0, liveKeys: 3, snapshotBytes: 0, tombstones: 1);
        var first = h.AddLeaf(stateBytes: 0, liveKeys: 5, snapshotBytes: 0, next: second, tombstones: 1);
        var mid = h.AddInternal(first, childrenAreLeaves: true);
        h.State.State.RootNodeId = h.AddInternal(mid, childrenAreLeaves: false);
        h.State.State.RootIsLeaf = false;

        var report = await h.Grain.GetDiagnosticsAsync(deep: true);

        Assert.Multiple(() =>
        {
            // depth starts at 1 and increments once per internal level walked.
            Assert.That(report.Depth, Is.EqualTo(3));
            Assert.That(report.RootIsLeaf, Is.False);
            Assert.That(report.LiveKeys, Is.EqualTo(8L));
            Assert.That(report.Tombstones, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task GetDiagnosticsAsync_shallow_over_an_internal_root_counts_the_leaf_chain()
    {
        var h = CreateHarness();
        var second = h.AddLeaf(stateBytes: 0, liveKeys: 3, snapshotBytes: 0);
        var first = h.AddLeaf(stateBytes: 0, liveKeys: 5, snapshotBytes: 0, next: second);
        h.State.State.RootNodeId = h.AddInternal(first, childrenAreLeaves: true);
        h.State.State.RootIsLeaf = false;

        var report = await h.Grain.GetDiagnosticsAsync(deep: false);

        Assert.Multiple(() =>
        {
            Assert.That(report.Depth, Is.EqualTo(2));
            Assert.That(report.LiveKeys, Is.EqualTo(8L));
            Assert.That(report.Tombstones, Is.Zero);
        });
    }

    [Test]
    public async Task GetDiagnosticsAsync_surfaces_the_in_flight_split_and_bulk_graft_flags()
    {
        var h = CreateHarness();
        h.State.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = ShardSplitPhase.BeginShadowWrite,
            ShadowTargetShardIndex = 1,
            MovedSlots = [0],
            VirtualShardCount = 2,
        };
        h.State.State.PendingBulkGraft = new PendingBulkGraft
        {
            OperationId = "bulk-1",
            ExistingRightmostLeafId = LeafId(Guid.NewGuid()),
            NewLeaves = [],
            RootWasLeaf = true,
        };

        var report = await h.Grain.GetDiagnosticsAsync(deep: false);

        Assert.Multiple(() =>
        {
            Assert.That(report.SplitInProgress, Is.True);
            Assert.That(report.BulkOperationPending, Is.True);
        });
    }
}
