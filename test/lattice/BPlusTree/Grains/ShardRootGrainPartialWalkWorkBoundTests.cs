using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 1972: four read-only leaf-chain walks in the
/// <c>ShardRootGrain</c> partials still ran to the end of the chain inside one
/// non-reentrant call, so a diagnostics scrape, a lag query, a storage-usage
/// re-anchor, or an operator projection rebuild held the shard - and queued
/// every other request behind it - for the whole chain.
/// <para>
/// The sweep that bounded the traversal walks (issues 1955, 1957, 1971, 1973)
/// audited <c>Traversal.cs</c>, so these four sites in <c>Diagnostics.cs</c> and
/// <c>ProjectionAdmin.cs</c> were missed, as were two leftmost-path descents
/// written <c>while (!childrenAreLeaves)</c> rather than <c>while (true)</c>.
/// </para>
/// <para>
/// The property each bounded site must hold is that driving its batches to
/// completion produces exactly the answer the single-call walk produced. These
/// aggregations are not idempotent: a resume position landing on an
/// already-visited leaf double-counts it, so the tests assert exact parity
/// rather than mere termination.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainPartialWalkWorkBoundTests
{
    private const string TreeId = "partial-walk-tree";
    private const string ShardKey = TreeId + "/0";

    private const int KeysPerLeaf = 2;
    private const int TombstonesPerLeaf = 1;
    private const long StateBytesPerLeaf = 100;
    private const long SnapshotBytesPerLeaf = 10;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required int LeafCount { get; init; }

        /// <summary>Number of <c>RebuildProjectionFromWalAsync</c> calls per leaf, by leaf index.</summary>
        public required int[] RebuildCalls { get; init; }

        /// <summary>Number of WAL head reads across the whole run.</summary>
        public required Func<int> HeadReads { get; init; }

        /// <summary>Ordered log of leaf interactions, used to assert call ordering.</summary>
        public required List<string> CallLog { get; init; }

        public long TotalLiveKeys => (long)LeafCount * KeysPerLeaf;
        public long TotalTombstones => (long)LeafCount * TombstonesPerLeaf;
        public long TotalStateBytes => LeafCount * StateBytesPerLeaf;
        public long TotalSnapshotBytes => LeafCount * SnapshotBytesPerLeaf;
    }

    /// <summary>
    /// Builds a forward leaf chain where leaf <c>i</c> owns keys
    /// <c>k{i:D3}-*</c> and declares the next leaf's first key as its exclusive
    /// high bound, so a bounded walk always has a real resume point to hand
    /// back.
    /// <para>
    /// Leaf grain ids are Guid-keyed because these sites address leaves through
    /// the <c>Guid</c> overload (<c>GrainId.GetGuidKey()</c>), which throws on a
    /// non-Guid key. Both the <c>GrainId</c> and <c>Guid</c> overloads are
    /// stubbed, because the shared <see cref="BoundedLeafWalk"/> resolves by
    /// <c>GrainId</c> while the per-leaf work here resolves by <c>Guid</c>.
    /// </para>
    /// <para>
    /// The root is modelled as an internal node so a resume key is re-descended
    /// to the leaf that owns it; a leaf root would collapse every descent onto
    /// one leaf and make a multi-leaf resume test meaningless.
    /// </para>
    /// </summary>
    private static Harness CreateChain(
        int leafCount,
        int maxLeavesPerBatch,
        int walPartitions = 1,
        long checkpointOffset = 5,
        long headOffset = 50)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var guids = new Guid[leafCount];
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
        {
            guids[i] = Guid.NewGuid();
            ids[i] = GrainId.Create("bplusleaf", guids[i].ToString("N"));
        }

        var rootGuid = Guid.NewGuid();
        var rootId = GrainId.Create("bplusinternal", rootGuid.ToString("N"));
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var rebuildCalls = new int[leafCount];
        var headReads = 0;
        var callLog = new List<string>();

        var root = Substitute.For<IBPlusInternalGrain>();
        var separators = new string?[leafCount];
        separators[0] = null;
        for (var i = 1; i < leafCount; i++) separators[i] = $"k{i:D3}-000";
        root.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
        {
            SeparatorKeys = separators,
            ChildIds = ids,
            ChildrenAreLeaves = true,
        }));
        root.GetLeftmostChildAsync().Returns(Task.FromResult(ids[0]));
        root.GetLeftmostChildWithMetadataAsync().Returns(Task.FromResult((ids[0], true)));
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusInternalGrain>(rootGuid).Returns(root);

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();

            leaf.GetStatsAsync().Returns(_ =>
            {
                lock (callLog) callLog.Add($"stats:{index}");
                return Task.FromResult(new LeafStats
                {
                    LiveKeys = KeysPerLeaf,
                    Tombstones = TombstonesPerLeaf,
                    StateBytes = StateBytesPerLeaf,
                });
            });
            leaf.CountAsync().Returns(_ =>
            {
                lock (callLog) callLog.Add($"count:{index}");
                return Task.FromResult(KeysPerLeaf);
            });
            leaf.GetProjectionCheckpointOffsetAsync().Returns(_ =>
            {
                lock (callLog) callLog.Add($"checkpoint:{index}");
                // Leaf 0 is the laggard, so the chain minimum is only correct
                // if the walk actually reached it.
                return Task.FromResult(index == 0 ? checkpointOffset : checkpointOffset + 100);
            });
            leaf.RebuildProjectionFromWalAsync().Returns(_ =>
            {
                lock (callLog) callLog.Add($"rebuild:{index}");
                Interlocked.Increment(ref rebuildCalls[index]);
                return Task.CompletedTask;
            });

            var high = index + 1 < leafCount ? $"k{index + 1:D3}-000" : null;
            leaf.GetKeyRangeAsync().Returns(_ =>
            {
                lock (callLog) callLog.Add($"keyrange:{index}");
                return Task.FromResult(new LeafKeyRange
                {
                    LowKeyInclusive = $"k{index:D3}-000",
                    HighKeyExclusive = high,
                });
            });

            var next = index + 1 < leafCount ? (GrainId?)ids[index + 1] : null;
            leaf.GetNextSiblingAsync().Returns(_ =>
            {
                lock (callLog) callLog.Add($"sibling:{index}");
                return Task.FromResult(next);
            });
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));

            factory.GetGrain<IBPlusLeafGrain>(ids[index]).Returns(leaf);
            factory.GetGrain<IBPlusLeafGrain>(guids[index]).Returns(leaf);

            var snapshot = Substitute.For<ILeafSnapshotStorageGrain>();
            snapshot.GetSnapshotByteSizeAsync(Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(SnapshotBytesPerLeaf));
            factory.GetGrain<ILeafSnapshotStorageGrain>(guids[index]).Returns(snapshot);
        }

        for (var p = 0; p < walPartitions; p++)
        {
            var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
            coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            {
                Interlocked.Increment(ref headReads);
                return Task.FromResult(headOffset);
            });
            factory.GetGrain<ILeafReplayCoordinatorGrain>($"{TreeId}/{p}").Returns(coordinator);
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = maxLeavesPerBatch,
                MaxScanPageDuration = TimeSpan.Zero,
                WalPartitions = walPartitions,
            },
            shardCount: 1,
            factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            LeafCount = leafCount,
            RebuildCalls = rebuildCalls,
            HeadReads = () => Volatile.Read(ref headReads),
            CallLog = callLog,
        };
    }

    // --- Site A: GetDiagnosticsAsync ---

    [Test]
    public async Task A_bounded_diagnostics_batch_stops_within_its_budget_and_reports_a_resume_key()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var page = await h.Grain.GetDiagnosticsBoundedAsync(deep: true, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.ResumeFromInclusive, Is.Not.Null,
                "a batch that stopped early must hand back a resume position, "
                + "otherwise the caller would treat a partial report as the answer");
            Assert.That(page.Report.LiveKeys, Is.LessThan(h.TotalLiveKeys),
                "precondition: the batch really did stop short of the whole chain");
            Assert.That(h.CallLog.Count(e => e.StartsWith("stats:", StringComparison.Ordinal)),
                Is.LessThanOrEqualTo(5),
                "the walk must release the non-reentrant shard once the leaf "
                + "budget is spent, instead of holding it for the whole chain");
        });
    }

    [Test]
    public async Task Driving_the_bounded_diagnostics_walk_counts_every_leaf_exactly_once()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        long liveKeys = 0;
        long tombstones = 0;
        string? cursor = null;
        var batches = 0;
        while (true)
        {
            var page = await h.Grain.GetDiagnosticsBoundedAsync(deep: true, cursor);
            liveKeys += page.Report.LiveKeys;
            tombstones += page.Report.Tombstones;
            batches++;
            Assert.That(batches, Is.LessThan(100), "the resumed walk must terminate");
            if (page.ResumeFromInclusive is not { } next) break;
            cursor = next;
        }

        Assert.Multiple(() =>
        {
            Assert.That(liveKeys, Is.EqualTo(h.TotalLiveKeys),
                "a resumed diagnostics walk must equal the unbounded walk exactly - "
                + "a resume position landing on an already-visited leaf would inflate it");
            Assert.That(tombstones, Is.EqualTo(h.TotalTombstones));
            Assert.That(batches, Is.GreaterThan(1),
                "precondition: the walk really was split into multiple batches");
        });
    }

    /// <summary>
    /// The O(1) shard-wide fields (depth, hotness, lifecycle flags) are
    /// authoritative on the first batch only. Recomputing them per batch would
    /// re-descend the internal levels for an identical answer, so a resumed
    /// batch carries only its key counts and the driver keeps the first
    /// batch's report.
    /// </summary>
    [Test]
    public async Task A_resumed_diagnostics_batch_carries_only_its_key_counts()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var first = await h.Grain.GetDiagnosticsBoundedAsync(deep: true, null);
        Assert.That(first.ResumeFromInclusive, Is.Not.Null, "precondition");
        var resumed = await h.Grain.GetDiagnosticsBoundedAsync(deep: true, first.ResumeFromInclusive);

        Assert.Multiple(() =>
        {
            Assert.That(first.Report.Depth, Is.GreaterThan(0),
                "the first batch computes the shard-wide fields");
            Assert.That(resumed.Report.Depth, Is.EqualTo(0),
                "a resumed batch must not re-descend the internal levels for an "
                + "answer the first batch already established");
            Assert.That(resumed.Report.LiveKeys, Is.GreaterThan(0),
                "but its counts must still carry information");
        });
    }

    [Test]
    public async Task The_unbounded_diagnostics_wrapper_still_reports_the_whole_chain()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var report = await h.Grain.GetDiagnosticsAsync(deep: true);

        Assert.Multiple(() =>
        {
            Assert.That(report.LiveKeys, Is.EqualTo(h.TotalLiveKeys));
            Assert.That(report.Tombstones, Is.EqualTo(h.TotalTombstones));
            Assert.That(report.Depth, Is.GreaterThan(0), "the shard-wide fields survive the shim");
        });
    }

    // --- Site B: GetShardMaterialiserLagAsync ---

    [Test]
    public async Task A_bounded_lag_batch_stops_within_its_budget_and_reports_a_resume_key()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var page = await h.Grain.GetShardMaterialiserLagBoundedAsync(null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.ResumeFromInclusive, Is.Not.Null);
            Assert.That(h.CallLog.Count(e => e.StartsWith("checkpoint:", StringComparison.Ordinal)),
                Is.LessThanOrEqualTo(5));
        });
    }

    /// <summary>
    /// Lag is <c>head - checkpoint</c>, so a fresher head read on a later batch
    /// would be measured against checkpoints gathered earlier and inflate the
    /// figure by whatever the tree committed mid-walk. The heads must therefore
    /// be pinned to the first batch.
    /// </summary>
    [Test]
    public async Task The_wal_heads_are_captured_on_the_first_lag_batch_only()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4, walPartitions: 2);

        var first = await h.Grain.GetShardMaterialiserLagBoundedAsync(null, CancellationToken.None);
        var headsAfterFirst = h.HeadReads();
        Assert.That(first.ResumeFromInclusive, Is.Not.Null, "precondition");

        var resumed = await h.Grain.GetShardMaterialiserLagBoundedAsync(
            first.ResumeFromInclusive, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(headsAfterFirst, Is.EqualTo(2), "one head read per WAL partition");
            Assert.That(h.HeadReads(), Is.EqualTo(2),
                "a resumed batch must not re-read the WAL heads, or lag inflates "
                + "by whatever committed between the batches");
            Assert.That(first.WalHeadOffsets, Has.Length.EqualTo(2));
            Assert.That(resumed.WalHeadOffsets, Is.Empty,
                "a resumed batch carries no heads; the driver keeps the first batch's");
        });
    }

    [Test]
    public async Task Driving_the_bounded_lag_walk_matches_the_unbounded_lag()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);
        var expected = await h.Grain.GetShardMaterialiserLagAsync(CancellationToken.None);

        var fresh = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);
        var page = await fresh.Grain.GetShardMaterialiserLagBoundedAsync(null, CancellationToken.None);
        var heads = page.WalHeadOffsets;
        var min = page.MinCheckpointOffset;
        var cursor = page.ResumeFromInclusive;
        while (cursor is not null)
        {
            page = await fresh.Grain.GetShardMaterialiserLagBoundedAsync(cursor, CancellationToken.None);
            if (page.MinCheckpointOffset < min) min = page.MinCheckpointOffset;
            cursor = page.ResumeFromInclusive;
        }

        Assert.Multiple(() =>
        {
            Assert.That(ShardRootGrain.ReduceMaterialiserLag(heads, min), Is.EqualTo(expected),
                "the min-reduce across batches must equal the single-call chain minimum");
            Assert.That(expected, Is.EqualTo(45), "head 50 minus the laggard leaf's checkpoint 5");
        });
    }

    /// <summary>
    /// An empty shard has no projection state, so the sum of the WAL heads IS
    /// the lag. The reducer reads the <c>long.MaxValue</c> minimum as the
    /// "visited no leaf" signal.
    /// </summary>
    [Test]
    public void An_empty_shard_reports_the_sum_of_the_wal_heads_as_its_lag()
    {
        Assert.That(ShardRootGrain.ReduceMaterialiserLag([7, 11], long.MaxValue), Is.EqualTo(18));
    }

    // --- Site C: RefreshLeafByteFootprintsAsync ---

    [Test]
    public async Task A_bounded_footprint_batch_stops_within_its_budget_and_reports_a_resume_key()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var page = await h.Grain.RefreshLeafByteFootprintsBoundedAsync(
            null, default, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.ResumeFromInclusive, Is.Not.Null);
            Assert.That(page.Usage.LeafStateBytes, Is.LessThan(h.TotalStateBytes));
        });
    }

    /// <summary>
    /// The activation-scoped totals must be re-anchored only on the batch that
    /// completes the walk. Re-anchoring per batch would leave the shard
    /// advertising a fraction of its own footprint to every concurrent
    /// <c>GetStorageUsageAsync</c> reader for the rest of the walk.
    /// </summary>
    [Test]
    public async Task The_activation_totals_are_re_anchored_only_on_the_final_footprint_batch()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var first = await h.Grain.RefreshLeafByteFootprintsBoundedAsync(
            null, default, CancellationToken.None);
        Assert.That(first.ResumeFromInclusive, Is.Not.Null, "precondition");

        var midWalk = await h.Grain.GetStorageUsageAsync(CancellationToken.None);
        Assert.That(midWalk.LeafStateBytes, Is.Zero,
            "a partial sum must never be published as the shard's total; the "
            + "pre-walk figure stands until the walk completes");

        var total = first.Usage;
        var cursor = first.ResumeFromInclusive;
        while (cursor is not null)
        {
            var page = await h.Grain.RefreshLeafByteFootprintsBoundedAsync(
                cursor, total, CancellationToken.None);
            total = new ShardStorageUsage
            {
                LeafStateBytes = total.LeafStateBytes + page.Usage.LeafStateBytes,
                SnapshotBytes = total.SnapshotBytes + page.Usage.SnapshotBytes,
                LiveKeys = total.LiveKeys + page.Usage.LiveKeys,
            };
            cursor = page.ResumeFromInclusive;
        }

        var settled = await h.Grain.GetStorageUsageAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(total.LeafStateBytes, Is.EqualTo(h.TotalStateBytes));
            Assert.That(total.SnapshotBytes, Is.EqualTo(h.TotalSnapshotBytes));
            Assert.That(total.LiveKeys, Is.EqualTo(h.TotalLiveKeys));
            Assert.That(settled.LeafStateBytes, Is.EqualTo(h.TotalStateBytes),
                "the final batch re-anchors the activation totals from the "
                + "whole-chain figure the driver threaded back in");
        });
    }

    [Test]
    public async Task The_unbounded_footprint_wrapper_still_sums_the_whole_chain()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var usage = await h.Grain.RefreshLeafByteFootprintsAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(h.TotalStateBytes));
            Assert.That(usage.SnapshotBytes, Is.EqualTo(h.TotalSnapshotBytes));
            Assert.That(usage.LiveKeys, Is.EqualTo(h.TotalLiveKeys));
        });
    }

    // --- Site F: RebuildShardProjectionAsync ---

    [Test]
    public async Task A_bounded_rebuild_batch_stops_within_its_budget_and_reports_a_resume_key()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var page = await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.ResumeFromInclusive, Is.Not.Null);
            Assert.That(page.LeavesRebuilt, Is.LessThan(h.LeafCount));
            Assert.That(page.LeavesRebuilt, Is.LessThanOrEqualTo(5));
        });
    }

    /// <summary>
    /// The reason this site cannot route through <see cref="BoundedLeafWalk"/>:
    /// the rebuild deactivates the leaf, so the sibling pointer and the resume
    /// key must be read while it is still activated. Reading them afterwards
    /// would force an immediate WAL replay purely to obtain a cursor, turning
    /// the deliberately lazy rebuild into an inline one.
    /// </summary>
    [Test]
    public async Task The_sibling_and_resume_key_are_read_before_the_rebuild_deactivates_the_leaf()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var page = await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None);
        Assert.That(page.ResumeFromInclusive, Is.Not.Null, "precondition: the batch yielded");

        var log = h.CallLog;
        for (var leaf = 0; leaf < page.LeavesRebuilt; leaf++)
        {
            var rebuild = log.IndexOf($"rebuild:{leaf}");
            var sibling = log.IndexOf($"sibling:{leaf}");
            Assert.That(sibling, Is.InRange(0, rebuild - 1),
                $"the sibling pointer for leaf {leaf} must be read before its rebuild");
        }

        var yieldLeaf = page.LeavesRebuilt - 1;
        Assert.That(log.IndexOf($"keyrange:{yieldLeaf}"),
            Is.InRange(0, log.IndexOf($"rebuild:{yieldLeaf}") - 1),
            "the resume key must be read before the rebuild deactivates the leaf");
    }

    [Test]
    public async Task Driving_the_bounded_rebuild_rebuilds_every_leaf_exactly_once()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        var rebuilt = 0;
        string? cursor = null;
        var batches = 0;
        while (true)
        {
            var page = await h.Grain.RebuildShardProjectionBoundedAsync(cursor, CancellationToken.None);
            rebuilt += page.LeavesRebuilt;
            batches++;
            Assert.That(batches, Is.LessThan(100), "the resumed rebuild must terminate");
            if (page.ResumeFromInclusive is not { } next) break;
            cursor = next;
        }

        Assert.Multiple(() =>
        {
            Assert.That(rebuilt, Is.EqualTo(h.LeafCount));
            Assert.That(h.RebuildCalls, Is.All.EqualTo(1),
                "no leaf may be rebuilt twice at a resume boundary, and none skipped");
            Assert.That(batches, Is.GreaterThan(1), "precondition: really split into batches");
        });
    }

    [Test]
    public async Task The_unbounded_rebuild_wrapper_still_rebuilds_the_whole_chain()
    {
        var h = CreateChain(leafCount: 40, maxLeavesPerBatch: 4);

        await h.Grain.RebuildShardProjectionAsync(CancellationToken.None);

        Assert.That(h.RebuildCalls, Is.All.EqualTo(1));
    }

    // --- Single-leaf shards ---

    /// <summary>
    /// A single-leaf shard has no sibling to resume into, so a resume key would
    /// re-descend to the same leaf and revisit it forever. Every bounded site
    /// must return a terminal page.
    /// </summary>
    [Test]
    public async Task A_single_leaf_shard_never_reports_a_resume_key_from_any_bounded_site()
    {
        var h = CreateChain(leafCount: 1, maxLeavesPerBatch: 1);

        var diagnostics = await h.Grain.GetDiagnosticsBoundedAsync(deep: true, null);
        var lag = await h.Grain.GetShardMaterialiserLagBoundedAsync(null, CancellationToken.None);
        var footprint = await h.Grain.RefreshLeafByteFootprintsBoundedAsync(
            null, default, CancellationToken.None);
        var rebuild = await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(diagnostics.ResumeFromInclusive, Is.Null);
            Assert.That(lag.ResumeFromInclusive, Is.Null);
            Assert.That(footprint.ResumeFromInclusive, Is.Null);
            Assert.That(rebuild.ResumeFromInclusive, Is.Null);
            Assert.That(rebuild.LeavesRebuilt, Is.EqualTo(1));
            Assert.That(diagnostics.Report.LiveKeys, Is.EqualTo(KeysPerLeaf));
        });
    }

    // --- The leftmost-path descents (the Class C leftover) ---

    /// <summary>
    /// Both leftmost-path descents in <c>Diagnostics.cs</c> are spelled
    /// <c>while (!childrenAreLeaves)</c>, so the issue-1955 audit - which swept
    /// for a bare <c>while (true)</c> in <c>Traversal.cs</c> - missed them. A
    /// cyclic leftmost-child pointer used to spin this non-reentrant grain
    /// forever; it must now surface as a typed exception.
    /// </summary>
    [Test]
    public void A_cyclic_leftmost_child_pointer_fails_the_descents_instead_of_spinning()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var rootGuid = Guid.NewGuid();
        var rootId = GrainId.Create("bplusinternal", rootGuid.ToString("N"));
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var node = Substitute.For<IBPlusInternalGrain>();
        // Points at itself and never declares a leaf level.
        node.GetLeftmostChildWithMetadataAsync().Returns(Task.FromResult((rootId, false)));
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(node);
        factory.GetGrain<IBPlusInternalGrain>(rootGuid).Returns(node);

        var grain = new ShardRootGrain(context, state, factory,
            TestOptionsResolver.Create(new LatticeOptions(), shardCount: 1, factory: factory),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await grain.GetDiagnosticsBoundedAsync(deep: false, null),
                Throws.InstanceOf<InvalidOperationException>()
                    .With.Message.Contains("depth descent exceeded"));
            Assert.That(async () => await grain.RefreshLeafByteFootprintsBoundedAsync(
                    null, default, CancellationToken.None),
                Throws.InstanceOf<InvalidOperationException>()
                    .With.Message.Contains("descent exceeded"));
        });
    }
}
