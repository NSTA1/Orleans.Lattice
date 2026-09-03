using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 1973: the cross-tree merge's per-shard drain is
/// work-bounded and resumes from a persisted <b>key</b> cursor.
/// <para>
/// The properties pinned here are the ones the shard driver depends on: a
/// bounded pass visits at most its budget, does not advance the shard cursor,
/// records where to resume, does not burn the poison-retry budget for making
/// partial progress, and a resumed sequence merges every source entry exactly
/// once. A structural change to the source chain between two passes must not
/// truncate the merge.
/// </para>
/// </summary>
public partial class TreeMergeGrainTests
{
    private static Dictionary<string, LwwValue<byte[]>> NumberedEntries(int count)
    {
        var result = new Dictionary<string, LwwValue<byte[]>>(count);
        var wall = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        for (var i = 0; i < count; i++)
        {
            result[$"entry-{i:D2}"] = LwwValue<byte[]>.Create(
                [(byte)i],
                new HybridLogicalClock { WallClockTicks = wall, Counter = i });
        }
        return result;
    }

    /// <summary>
    /// Seeds a merge that is already in flight over a single source shard whose
    /// leaf chain is <paramref name="leafCount"/> leaves long, and records every
    /// key the target accepts.
    /// </summary>
    private static (TreeMergeGrain Grain,
                    FakePersistentState<TreeMergeState> State,
                    IGrainFactory Factory,
                    IShardRootGrain SourceShard,
                    List<string> MergedKeys) CreateInFlightMerge(int leafCount, int leavesPerPass)
    {
        var options = new LatticeOptions { BackgroundDrainLeavesPerPass = leavesPerPass };
        var existing = new FakePersistentState<TreeMergeState>
        {
            State = new TreeMergeState
            {
                InProgress = true,
                NextShardIndex = 0,
                SourceTreeId = SourceTreeId,
                SourcePhysicalTreeId = SourceTreeId,
                TargetPhysicalTreeId = TargetTreeId,
                SourceShardCount = 1,
                SourcePhysicalShards = [0],
            },
        };

        var (grain, state, _, grainFactory, _) = CreateGrain(options, existing);

        var leafIds = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            leafIds[i] = GrainId.Create("leaf", $"merge-src-leaf-{i}");

        SetupSourceShardWithEntries(
            grainFactory, SourceTreeId, 0, NumberedEntries(leafCount), leafIds);

        var mergedKeys = new List<string>();
        var targetShard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TargetTreeId}/0").Returns(targetShard);
        grainFactory.GetGrain<IShardRootGrain>($"{TargetTreeId}/1").Returns(targetShard);
        targetShard.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(ci =>
            {
                mergedKeys.AddRange(((Dictionary<string, LwwValue<byte[]>>)ci[0]).Keys);
                return Task.FromResult<SplitResult?>(null);
            });

        return (grain, state, grainFactory,
            grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0"), mergedKeys);
    }

    [Test]
    public async Task Merge_drain_visits_at_most_the_configured_leaves_per_pass()
    {
        var (grain, state, _, _, mergedKeys) = CreateInFlightMerge(leafCount: 5, leavesPerPass: 2);

        await grain.ProcessNextShardAsync();

        Assert.Multiple(() =>
        {
            Assert.That(mergedKeys, Is.EquivalentTo(new[] { "entry-00", "entry-01" }));
            Assert.That(state.State.NextShardIndex, Is.EqualTo(0),
                "the shard cursor must not advance until the shard's leaf chain is swept");
            Assert.That(state.State.DrainCursorKey, Is.EqualTo(SourceLeafResumeKey(2)),
                "a yielded pass must persist where to resume");
        });
    }

    [Test]
    public async Task Merge_drain_resumes_from_the_persisted_key_cursor_and_merges_every_entry_once()
    {
        var (grain, state, _, _, mergedKeys) = CreateInFlightMerge(leafCount: 5, leavesPerPass: 2);

        // Three ticks: 2 + 2 + 1 leaves, then the shard cursor advances past the
        // only source shard.
        await grain.ProcessNextShardAsync();
        await grain.ProcessNextShardAsync();
        await grain.ProcessNextShardAsync();

        Assert.Multiple(() =>
        {
            Assert.That(mergedKeys, Is.EquivalentTo(new[]
            {
                "entry-00", "entry-01", "entry-02", "entry-03", "entry-04",
            }), "every source entry must be merged exactly once across the resumed passes");
            Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
            Assert.That(state.State.DrainCursorKey, Is.Null,
                "the cursor must clear when the shard advances, so a stale key cannot re-descend into the next shard");
        });
    }

    /// <summary>
    /// A bounded pass that yielded is forward progress, not a failed attempt.
    /// Burning the poison-retry budget for it would skip a large but perfectly
    /// healthy shard after two ticks.
    /// </summary>
    [Test]
    public async Task Merge_drain_does_not_burn_the_retry_budget_for_a_partial_pass()
    {
        var (grain, state, _, _, _) = CreateInFlightMerge(leafCount: 6, leavesPerPass: 1);

        for (var i = 0; i < 4; i++)
            await grain.ProcessNextShardAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextShardIndex, Is.EqualTo(0),
                "the shard must not have been poisoned while it was making progress");
            Assert.That(state.State.ShardRetries, Is.Zero);
            Assert.That(state.State.DrainCursorKey, Is.EqualTo(SourceLeafResumeKey(4)));
        });
    }

    /// <summary>
    /// The definition-of-done case for this grain: the source chain is
    /// structurally changed between two passes. A leaf splits after the pass
    /// that parked before it, and the resumed merge must still carry both halves
    /// across.
    /// </summary>
    [Test]
    public async Task Merge_drain_resumed_after_the_cursor_leaf_splits_merges_both_halves()
    {
        var (grain, state, factory, sourceShard, mergedKeys) =
            CreateInFlightMerge(leafCount: 3, leavesPerPass: 1);

        await grain.ProcessNextShardAsync();
        Assert.That(state.State.DrainCursorKey, Is.EqualTo(SourceLeafResumeKey(1)));

        var leaf1 = (await sourceShard.GetLeafIdForKeyAsync(SourceLeafResumeKey(1)))!.Value;
        var leaf2 = (await sourceShard.GetLeafIdForKeyAsync(SourceLeafResumeKey(2)))!.Value;

        // Leaf 1 splits; the right half is grafted in ahead of leaf 2 and holds
        // an entry no earlier pass could have seen.
        var rightHalf = GrainId.Create("leaf", "merge-src-leaf-1-right");
        var rightHalfLeaf = Substitute.For<IBPlusLeafGrain>();
        factory.GetGrain<IBPlusLeafGrain>(rightHalf).Returns(rightHalfLeaf);
        rightHalfLeaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(Task.FromResult(new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>> { ["entry-01-right"] = LwwValue<byte[]>.Create([9], default) },
            Version = new VersionVector(),
        }));
        rightHalfLeaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(leaf2));
        rightHalfLeaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = "k0001m",
            HighKeyExclusive = SourceLeafResumeKey(2),
        }));

        var leftHalf = factory.GetGrain<IBPlusLeafGrain>(leaf1);
        leftHalf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(rightHalf));
        leftHalf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = SourceLeafResumeKey(1),
            HighKeyExclusive = "k0001m",
        }));
        sourceShard.GetLeafIdForKeyAsync("k0001m").Returns(Task.FromResult<GrainId?>(rightHalf));

        for (var i = 0; i < 5 && state.State.NextShardIndex == 0; i++)
            await grain.ProcessNextShardAsync();

        Assert.Multiple(() =>
        {
            Assert.That(mergedKeys, Does.Contain("entry-01-right"),
                "the half grafted in between two passes must not be skipped");
            Assert.That(mergedKeys, Does.Contain("entry-02"),
                "the merge must continue past the split rather than truncate");
            Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
        });
    }
}
