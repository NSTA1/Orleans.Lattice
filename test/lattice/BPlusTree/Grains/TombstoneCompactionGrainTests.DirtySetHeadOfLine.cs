using System.Diagnostics.Metrics;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the dirty-set head-of-line block described in issue 2926.
/// <para>
/// In <c>TombstoneCompactionGrain.ProcessShardAsync</c> the dirty-set fast
/// path wraps each leaf in a bare <c>catch { RecordSkippedLeaf(...); throw; }</c>
/// that re-throws <em>before</em> <c>dirtyIndex++</c>, and the only call to
/// <c>ClearDirtyLeavesUpToAsync</c> sits on the full-completion branch. A
/// single un-compactable leaf at the head of the list therefore aborts every
/// pass, never advances the index, never drains the watermark, and starves
/// every leaf behind it.
/// </para>
/// <para>
/// The tests below encode the behaviour as it stands today, deliberately, so
/// that the defect is pinned rather than merely described. They are
/// <em>characterisation</em> tests: when a remedy for 2926 lands, they must be
/// re-derived from the chosen semantics rather than edited until they pass.
/// The assertion each one would carry under a correct implementation is stated
/// beside it.
/// </para>
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    /// <summary>
    /// Stubs a shard whose dirty-leaf snapshot names <paramref name="dirtyLeaves"/>,
    /// where the leaf at each index for which <paramref name="throwsAtIndex"/>
    /// returns true fails its <c>CompactTombstonesAsync</c> call.
    /// </summary>
    private static (IShardRootGrain ShardRoot, IBPlusLeafGrain[] Leaves) SetupShardWithDirtyLeavesWhere(
        IGrainFactory grainFactory,
        int shardIndex,
        HybridLogicalClock observedAdvance,
        Func<int, bool> throwsAtIndex,
        params GrainId[] dirtyLeaves)
    {
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{shardIndex}")
            .Returns(shardRoot);

        shardRoot.GetDirtyLeavesSinceLastCompactionAsync()
            .Returns(Task.FromResult(new DirtyLeavesSnapshot
            {
                DirtyLeaves = [.. dirtyLeaves],
                ObservedAdvance = observedAdvance,
            }));

        // The dirty-set path names its leaves outright, so neither the
        // leftmost-leaf lookup nor sibling navigation may be consulted.
        // Failing loudly here stops a regression quietly falling back to
        // the legacy chain walk and passing for the wrong reason.
        shardRoot.GetLeftmostLeafIdAsync()
            .Returns<GrainId?>(_ => throw new InvalidOperationException(
                "dirty-set fast path must not call GetLeftmostLeafIdAsync"));

        var leaves = new IBPlusLeafGrain[dirtyLeaves.Length];
        for (int i = 0; i < dirtyLeaves.Length; i++)
        {
            var leafMock = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(dirtyLeaves[i]).Returns(leafMock);
            if (throwsAtIndex(i))
            {
                leafMock.CompactTombstonesAsync(Arg.Any<TimeSpan>())
                    .Throws(new InvalidOperationException($"leaf {i} cannot compact"));
            }
            else
            {
                leafMock.CompactTombstonesAsync(Arg.Any<TimeSpan>()).Returns(Task.FromResult(0));
            }

            leafMock.GetNextSiblingAsync()
                .Returns<GrainId?>(_ => throw new InvalidOperationException(
                    "dirty-set fast path must not call GetNextSiblingAsync"));
            leaves[i] = leafMock;
        }

        return (shardRoot, leaves);
    }

    /// <summary>
    /// Positive control for the two tests below. Establishes that this harness
    /// can actually observe a dirty set draining, so that the "never drains"
    /// assertions are measured absence rather than a harness that could never
    /// have shown a drain in the first place.
    /// </summary>
    [Test]
    public async Task DirtySet_with_no_failing_leaf_drains_and_clears_the_watermark()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var head = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var behind = GrainId.Create("leaf", Guid.NewGuid().ToString());

        var (shardRoot, leaves) = SetupShardWithDirtyLeavesWhere(
            grainFactory, 0, advance, _ => false, head, behind);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        await leaves[0].Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await leaves[1].Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await shardRoot.Received(1).ClearDirtyLeavesUpToAsync(advance);
        Assert.That(state.State.CurrentShardDirtyLeaves, Is.Null,
            "a fully drained dirty set must not be retained for the next pass");
    }

    /// <summary>
    /// The defect. A throwing head leaf starves the leaf behind it across every
    /// pass, and the shard is abandoned without the watermark ever draining.
    /// <para>
    /// Under a correct implementation the third assertion below would instead be
    /// <c>await leaves[1].Received().CompactTombstonesAsync(...)</c>: the second
    /// pass would reach the leaf behind the blocker. Substituting that assertion
    /// today fails with
    /// <c>ReceivedCallsException : Expected to receive a call matching
    /// CompactTombstonesAsync(any TimeSpan). Actually received no matching
    /// calls.</c>, which is the executable demonstration of issue 2926.
    /// </para>
    /// </summary>
    [Test]
    public async Task DirtySet_head_of_line_failure_starves_every_leaf_behind_it()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var blocker = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var behind = GrainId.Create("leaf", Guid.NewGuid().ToString());

        var (shardRoot, leaves) = SetupShardWithDirtyLeavesWhere(
            grainFactory, 0, advance, i => i == 0, blocker, behind);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // Pass 1: the blocker throws. ProcessNextShardAsync converts the
        // shard-level exception into retry bookkeeping (ShardRetries -> 1)
        // and restores the pre-batch cursor.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(0),
            "the shard still holds its retry budget after one failure");

        // Pass 2: the same list is re-nominated from index 0 and the blocker
        // throws again. MaxRetriesPerShard is 1, so the budget is now spent.
        await grain.ProcessNextShardAsync();

        await leaves[0].Received(2).CompactTombstonesAsync(Arg.Any<TimeSpan>());

        // THE DEFECT. Two full passes over a two-leaf dirty set and the second
        // leaf was never once reached, because dirtyIndex never advances past
        // the throwing head.
        await leaves[1].DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());

        // And the watermark never drains, so the shard root still believes both
        // leaves are dirty and will nominate the identical list again.
        await shardRoot.DidNotReceive().ClearDirtyLeavesUpToAsync(Arg.Any<HybridLogicalClock>());

        // The shard is abandoned outright once the budget is spent. Note the
        // arity mismatch this exposes: MaxRetriesPerShard is a *shard*-scoped
        // budget being consumed by a single *leaf*-scoped fault.
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1),
            "the whole shard is skipped once one leaf exhausts the shard budget");
    }

    /// <summary>
    /// The skip emissions raised by a starved dirty set are attributed to the
    /// dirty-set path and never to the chain walk. This is the path partition
    /// only; it makes no claim about the order in which the dirty set is
    /// walked, which is dictionary-ordered and not positionally meaningful.
    /// </summary>
    [Test]
    public async Task DirtySet_head_of_line_skips_are_attributed_to_the_dirty_set_path()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var blocker = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var behind = GrainId.Create("leaf", Guid.NewGuid().ToString());

        SetupShardWithDirtyLeavesWhere(grainFactory, 0, advance, i => i == 0, blocker, behind);
        SetupShardWithLeaves(grainFactory, 1);

        var visited = new List<KeyValuePair<string, object?>[]>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == "orleans.lattice.compaction.leaves.visited")
                {
                    l.EnableMeasurementEvents(inst);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (value <= 0) return;
            visited.Add(tags.ToArray());
        });
        listener.Start();

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();
        await grain.ProcessNextShardAsync();

        Assert.That(visited, Is.Not.Empty,
            "the starved passes must emit at least one visited measurement");

        static string? TagValue(KeyValuePair<string, object?>[] tags, string key)
            => tags.FirstOrDefault(t => t.Key == key).Value as string;

        var skipped = visited.Where(t => TagValue(t, LatticeMetrics.TagOutcome) == "skipped").ToList();
        Assert.That(skipped, Has.Count.EqualTo(2),
            "one skip per pass - the blocker is re-visited and re-skipped every time");

        Assert.That(skipped.All(t => TagValue(t, LatticeMetrics.TagPath) == LatticeMetrics.PathDirtySet),
            Is.True, "every starved skip is attributed to the dirty-set path");
        Assert.That(skipped.Any(t => TagValue(t, LatticeMetrics.TagPath) == LatticeMetrics.PathWalk),
            Is.False, "no starved skip may be attributed to the legacy chain walk");
    }
}
