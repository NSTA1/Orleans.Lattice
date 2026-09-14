using System.Diagnostics.Metrics;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the remedy for the dirty-set head-of-line block described in issue 2926.
/// <para>
/// The defect these tests originally characterised: the dirty-set fast path
/// wrapped each leaf in a bare <c>catch { RecordSkippedLeaf(...); throw; }</c>
/// that re-threw <em>before</em> <c>dirtyIndex++</c>, while the only call to
/// <c>ClearDirtyLeavesUpToAsync</c> sat on the full-completion branch. A
/// single un-compactable leaf at the head of the list therefore aborted every
/// pass, never advanced the index, never drained the watermark, and starved
/// every leaf behind it for the life of the process.
/// </para>
/// <para>
/// The remedy is <b>skip-and-retain</b>, and the two halves are inseparable.
/// Skipping alone is worse than the defect: completing the shard calls
/// <c>ClearDirtyLeavesUpToAsync(advance)</c>, which discards every mark at or
/// below the watermark - the blocker's included - so the one leaf the exercise
/// exists to reclaim becomes the one leaf silently forgotten while the shard
/// reports clean success. So before the walk moves past a blocker, its dirty
/// mark is re-raised strictly above that watermark, and the drain's existing
/// strictly-greater preservation rule then retains exactly the skipped leaves.
/// </para>
/// <para>
/// These began as <em>characterisation</em> tests written against the unfixed
/// code (PR 2934) and have been re-derived here from the remedy's semantics,
/// not edited until they passed. The flip the original fixture named in this
/// doc comment - <c>await leaves[1].Received().CompactTombstonesAsync(...)</c>,
/// which then failed with <c>ReceivedCallsException</c> - is now asserted
/// positively below.
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
    /// The remedy. A throwing head leaf no longer starves the leaf behind it:
    /// the walk advances past the blocker in the same pass, and the shard
    /// completes rather than burning a shard-scoped retry on a leaf-scoped
    /// fault.
    /// <para>
    /// The ordering clause is the load-bearing one. The blocker's mark must be
    /// re-raised <em>before</em> the drain runs, because the drain discards
    /// everything at or below the watermark. Asserting both calls happened is
    /// not enough - in the opposite order the drain would discard the very
    /// mark the retain then re-raises against a watermark already passed.
    /// </para>
    /// </summary>
    [Test]
    public async Task DirtySet_head_of_line_failure_no_longer_starves_the_leaves_behind_it()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var blocker = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var behind = GrainId.Create("leaf", Guid.NewGuid().ToString());

        var (shardRoot, leaves) = SetupShardWithDirtyLeavesWhere(
            grainFactory, 0, advance, i => i == 0, blocker, behind);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // One pass is now enough: the blocker throws, is skipped, and the walk
        // continues to the leaf behind it.
        await grain.ProcessNextShardAsync();

        await leaves[0].Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());

        // THE FLIP. This is the assertion PR 2934 named as the one a remedy
        // must make true, and which failed with ReceivedCallsException against
        // the unfixed code.
        await leaves[1].Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());

        // The blocker's signal survives: re-raised strictly above the watermark
        // this pass drains to, so the drain below cannot discard it.
        await shardRoot.Received(1).RetainDirtyLeafAsync(blocker, advance);
        await shardRoot.DidNotReceive().RetainDirtyLeafAsync(behind, Arg.Any<HybridLogicalClock>());

        // And the drain still fires, so the leaves that DID compact are cleared.
        await shardRoot.Received(1).ClearDirtyLeavesUpToAsync(advance);

        // Retain strictly precedes drain. Reversed, the blocker would be
        // discarded and silently forgotten - the failure mode that makes naive
        // skipping worse than the original stall.
        Received.InOrder(() =>
        {
            shardRoot.RetainDirtyLeafAsync(blocker, advance);
            shardRoot.ClearDirtyLeavesUpToAsync(advance);
        });

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextShardIndex, Is.EqualTo(1),
                "the shard completed rather than being abandoned");
            Assert.That(state.State.ShardRetries, Is.Zero,
                "a leaf-scoped fault no longer spends the shard-scoped retry budget");
            Assert.That(state.State.CurrentShardDirtyLeaves, Is.Null,
                "a completed shard does not retain its leaf list for the next pass");
        });
    }

    /// <summary>
    /// The skip emissions raised by a blocked leaf are attributed to the
    /// dirty-set path and never to the chain walk. This is the path partition
    /// only; it makes no claim about the order in which the dirty set is
    /// walked, which is dictionary-ordered and not positionally meaningful.
    /// <para>
    /// It also pins the <em>loudness</em> half of the remedy. Now that a
    /// blocked leaf no longer stalls its shard, this counter is the only
    /// signal that a specific leaf is wedged, so it must still fire on a pass
    /// that otherwise reports clean success. A remedy that silenced it while
    /// the shard went green would be the exact "improves every observable
    /// while degrading the thing being measured" trade the design refuses.
    /// </para>
    /// </summary>
    [Test]
    public async Task DirtySet_head_of_line_skips_are_attributed_to_the_dirty_set_path()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
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

        // One pass now walks the whole shard, so the blocker is visited and
        // skipped exactly once rather than once per starved retry.
        await grain.ProcessNextShardAsync();

        Assert.That(visited, Is.Not.Empty,
            "the pass must emit at least one visited measurement");

        static string? TagValue(KeyValuePair<string, object?>[] tags, string key)
            => tags.FirstOrDefault(t => t.Key == key).Value as string;

        var skipped = visited.Where(t => TagValue(t, LatticeMetrics.TagOutcome) == "skipped").ToList();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextShardIndex, Is.EqualTo(1),
                "the shard completed, so this is the clean-success case the skip must survive");
            Assert.That(skipped, Has.Count.EqualTo(1),
                "the blocker is skipped once per pass, and the pass now covers the whole shard");
        });

        Assert.That(skipped.All(t => TagValue(t, LatticeMetrics.TagPath) == LatticeMetrics.PathDirtySet),
            Is.True, "every skip is attributed to the dirty-set path");
        Assert.That(skipped.Any(t => TagValue(t, LatticeMetrics.TagPath) == LatticeMetrics.PathWalk),
            Is.False, "no skip may be attributed to the legacy chain walk");
    }

    /// <summary>
    /// Captures the tag sets of every <c>outcome=skipped</c> measurement on the
    /// compaction leaves-visited counter for the lifetime of the returned
    /// handle. Shared by the fault arms, which since issue 2926 must assert on
    /// this counter directly: a skipped leaf no longer fails its shard, so the
    /// counter is the only remaining observable.
    /// </summary>
    private static SkippedLeafCapture CaptureSkippedLeafTags() => new();

    private static string? TriggerTagOf(KeyValuePair<string, object?>[] tags)
        => tags.FirstOrDefault(t => t.Key == LatticeMetrics.TagTrigger).Value as string;

    private sealed class SkippedLeafCapture
        : IReadOnlyList<KeyValuePair<string, object?>[]>, IDisposable
    {
        private readonly List<KeyValuePair<string, object?>[]> _skipped = [];
        private readonly MeterListener _listener;

        internal SkippedLeafCapture()
        {
            _listener = new MeterListener
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
            _listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                if (value <= 0) return;
                var copy = tags.ToArray();
                if (copy.Any(t => t.Key == LatticeMetrics.TagOutcome && (t.Value as string) == "skipped"))
                {
                    lock (_skipped) _skipped.Add(copy);
                }
            });
            _listener.Start();
        }

        public int Count { get { lock (_skipped) return _skipped.Count; } }

        public KeyValuePair<string, object?>[] this[int index]
        {
            get { lock (_skipped) return _skipped[index]; }
        }

        public IEnumerator<KeyValuePair<string, object?>[]> GetEnumerator()
        {
            lock (_skipped) return _skipped.ToList().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            => GetEnumerator();

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// The refusal clause. When the blocker's mark cannot be re-raised above
    /// the watermark, advancing past it would let the drain discard it, so the
    /// fault is re-raised instead and the pre-2926 head-of-line stall stands.
    /// A loud stall beats a quiet omission.
    /// <para>
    /// This is the one path on which a leaf-scoped fault still reaches the
    /// shard, and it is deliberate: it is precisely the case where the safe
    /// alternative is unavailable.
    /// </para>
    /// </summary>
    [Test]
    public async Task DirtySet_blocker_whose_mark_cannot_be_retained_fails_the_batch_without_draining()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var blocker = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var behind = GrainId.Create("leaf", Guid.NewGuid().ToString());

        var (shardRoot, leaves) = SetupShardWithDirtyLeavesWhere(
            grainFactory, 0, advance, i => i == 0, blocker, behind);
        shardRoot.RetainDirtyLeafAsync(Arg.Any<GrainId>(), Arg.Any<HybridLogicalClock>())
            .Returns(_ => Task.FromException(
                new InvalidOperationException("shard root unavailable")));
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        // The drain is the thing being defended. It must not run, because
        // running it would discard the mark the retain failed to lift.
        await shardRoot.DidNotReceive().ClearDirtyLeavesUpToAsync(Arg.Any<HybridLogicalClock>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ShardRetries, Is.EqualTo(1),
                "the batch failed, so the shard retry policy applies as it did before 2926");
            Assert.That(state.State.NextShardIndex, Is.Zero,
                "and the coordinator stays on the shard rather than advancing past it");
        });

        // The walk stopped at the blocker, exactly as it did before the remedy.
        await leaves[1].DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }
}
