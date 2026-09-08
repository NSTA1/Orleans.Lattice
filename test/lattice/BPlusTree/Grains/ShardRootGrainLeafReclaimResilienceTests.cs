using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Failure- and race-arm coverage for empty-leaf chain reclaim
/// (<c>ShardRootGrain.LeafReclaim</c>).
/// <para>
/// The happy path - an emptied leaf folded out of the chain and its range
/// handed to its predecessor - is pinned end to end by the integration
/// suite. What that suite cannot reach is the set of arms that only fire
/// when a concurrent writer, a split, or a storage fault lands in one of
/// the narrow windows the fold ordering is built around: a leaf that stops
/// being empty between the probe and the retirement latch, a predecessor
/// that a split moves out from under the compare-and-swap, a routing entry
/// that will not retire, and the repair arms that finish a fold interrupted
/// part-way by a crash.
/// </para>
/// <para>
/// Those arms carry the whole of the "never trade a slow scan for a corrupt
/// tree" safety argument, so each test below asserts the compensating action
/// actually happened - the leaf is unlatched, the fold is not counted, the
/// pass keeps going - rather than merely that nothing threw.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainLeafReclaimResilienceTests
{
    private const string TreeId = "reclaim-resilience-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// A three-leaf chain <c>A -&gt; B -&gt; C</c> under a single internal
    /// root, which is the smallest topology in which the middle leaf is a
    /// legitimate fold candidate: it has a predecessor to inherit its range
    /// and a successor whose back pointer has to be re-pointed.
    /// <para>
    /// The substitutes are a live model rather than fixed returns: an unlink,
    /// a widen, a back-pointer repair and a child removal all mutate the
    /// modelled chain, exactly as the real grains would. That matters because
    /// the walk re-probes the predecessor after every fold and keeps going -
    /// against frozen stubs it would re-fold the same leaf until the budget
    /// ran out, and every "reclaimed exactly one" assertion below would be
    /// measuring the stub instead of the grain.
    /// </para>
    /// </summary>
    private sealed class ReclaimHarness
    {
        public ShardRootGrain Grain { get; set; } = null!;
        public required IBPlusInternalGrain Root { get; init; }
        public required GrainId LeafA { get; init; }
        public required GrainId LeafB { get; init; }
        public required GrainId LeafC { get; init; }
        public required Dictionary<GrainId, IBPlusLeafGrain> Leaves { get; init; }
        public required Dictionary<GrainId, LeafReclaimProbe> Probes { get; init; }
        public required List<GrainId> ChildIds { get; init; }
        public required List<string?> Separators { get; init; }

        public IBPlusLeafGrain A => Leaves[LeafA];
        public IBPlusLeafGrain B => Leaves[LeafB];
        public IBPlusLeafGrain C => Leaves[LeafC];

        /// <summary>Rewrites one leaf's probe, which the walk re-reads on every pass.</summary>
        public void SetProbe(GrainId id, LeafReclaimProbe probe) => Probes[id] = probe;

        public RoutingTableSnapshot Snapshot() => new()
        {
            SeparatorKeys = [.. Separators],
            ChildIds = [.. ChildIds],
            ChildrenAreLeaves = true,
        };

        public bool RemoveChild(GrainId childId)
        {
            var index = ChildIds.IndexOf(childId);
            if (index < 0) return false;
            ChildIds.RemoveAt(index);
            Separators.RemoveAt(index);
            return true;
        }
    }

    private static ReclaimHarness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var rootId = GrainId.Create("internal", "reclaim-root");
        var leafA = GrainId.Create("leaf", "reclaim-leaf-a");
        var leafB = GrainId.Create("leaf", "reclaim-leaf-b");
        var leafC = GrainId.Create("leaf", "reclaim-leaf-c");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();

        // The healthy chain, as the walk should find it. Tests mutate single
        // entries to stage the race they are pinning.
        var probes = new Dictionary<GrainId, LeafReclaimProbe>
        {
            [leafA] = new()
            {
                LiveRowCount = 1,
                PrevSibling = null,
                NextSibling = leafB,
                LowKeyInclusive = null,
                HighKeyExclusive = "b",
            },
            [leafB] = new()
            {
                LiveRowCount = 0,
                PrevSibling = leafA,
                NextSibling = leafC,
                LowKeyInclusive = "b",
                HighKeyExclusive = "c",
            },
            [leafC] = new()
            {
                LiveRowCount = 1,
                PrevSibling = leafB,
                NextSibling = null,
                LowKeyInclusive = "c",
                HighKeyExclusive = null,
            },
        };

        var harness = new ReclaimHarness
        {
            Root = Substitute.For<IBPlusInternalGrain>(),
            LeafA = leafA,
            LeafB = leafB,
            LeafC = leafC,
            Leaves = [],
            Probes = probes,
            ChildIds = [leafA, leafB, leafC],
            Separators = [null, "b", "c"],
        };

        var root = harness.Root;
        root.GetRoutingTableAsync().Returns(_ => Task.FromResult(harness.Snapshot()));
        root.GetChildIdsAsync().Returns(_ => Task.FromResult(new List<GrainId>(harness.ChildIds)));
        root.RemoveChildAsync(Arg.Any<GrainId>())
            .Returns(ci => Task.FromResult(harness.RemoveChild(ci.Arg<GrainId>())));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(root);

        foreach (var id in new[] { leafA, leafB, leafC })
        {
            var self = id;
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.GetReclaimProbeAsync().Returns(_ => Task.FromResult(probes[self]));
            leaf.TryBeginRetirementAsync().Returns(Task.FromResult(true));
            leaf.AbandonRetirementAsync().Returns(Task.CompletedTask);
            leaf.ClearGrainStateAsync().Returns(Task.CompletedTask);

            leaf.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(ci =>
            {
                probes[self] = probes[self] with { PrevSibling = ci.Arg<GrainId?>() };
                return Task.CompletedTask;
            });

            leaf.AbsorbSuccessorRangeAsync(Arg.Any<string?>()).Returns(ci =>
            {
                probes[self] = probes[self] with { HighKeyExclusive = ci.Arg<string?>() };
                return Task.CompletedTask;
            });

            leaf.TryUnlinkSuccessorAsync(Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>())
                .Returns(ci =>
                {
                    probes[self] = probes[self] with
                    {
                        NextSibling = ci.ArgAt<GrainId?>(1),
                        HighKeyExclusive = ci.ArgAt<string?>(2),
                    };
                    return Task.FromResult(true);
                });

            harness.Leaves[id] = leaf;
        }

        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(ci => harness.Leaves[ci.Arg<GrainId>()]);

        harness.Grain = new ShardRootGrain(
            context, state, factory,
            TestOptionsResolver.Create(factory: factory),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return harness;
    }

    [Test]
    public async Task A_healthy_middle_leaf_is_folded_out_of_the_chain()
    {
        // Falsifier for every negative test below: with no fault staged the
        // same harness really does complete a fold, so a later "did not fold"
        // assertion is evidence about the arm and not about the harness.
        var h = CreateHarness();

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.EqualTo(1));
        await h.A.Received(1).TryUnlinkSuccessorAsync(h.LeafB, h.LeafC, "c");
        await h.Root.Received(1).RemoveChildAsync(h.LeafB);
        await h.C.Received(1).SetPrevSiblingAsync(h.LeafA);
        await h.B.Received(1).ClearGrainStateAsync();
        await h.B.DidNotReceive().AbandonRetirementAsync();
    }

    [Test]
    public async Task A_leaf_that_stopped_being_empty_after_the_probe_is_not_folded()
    {
        // The retirement latch re-runs the emptiness judgement against the leaf
        // as it is NOW. A write that landed between the probe and the latch is
        // the race this refusal exists for, and nothing destructive has run
        // yet, so the tree must be left exactly as it was found.
        var h = CreateHarness();
        h.B.TryBeginRetirementAsync().Returns(Task.FromResult(false));

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.Zero, "a refused latch must not be counted as a fold");
        await h.A.DidNotReceive().TryUnlinkSuccessorAsync(
            Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>());
        await h.Root.DidNotReceive().RemoveChildAsync(Arg.Any<GrainId>());
        await h.B.DidNotReceive().ClearGrainStateAsync();

        // Nothing was latched, so there is nothing to unlatch.
        await h.B.DidNotReceive().AbandonRetirementAsync();
    }

    [Test]
    public async Task A_split_that_lands_under_the_compare_and_swap_unlatches_the_leaf()
    {
        // The predecessor no longer points at this leaf, so a split inserted a
        // leaf between them after the probe. The fold is abandoned - and
        // critically the latch taken a moment ago must be released, or the
        // leaf stays frozen and refuses writes for the rest of its life.
        var h = CreateHarness();
        h.A.TryUnlinkSuccessorAsync(Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(false));

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.Zero);
        await h.B.Received(1).AbandonRetirementAsync();
        await h.Root.DidNotReceive().RemoveChildAsync(Arg.Any<GrainId>());
        await h.B.DidNotReceive().ClearGrainStateAsync();
    }

    [Test]
    public async Task A_throwing_compare_and_swap_unlatches_the_leaf_before_it_propagates()
    {
        // Same compensation as the declined swap, but on the exceptional path:
        // the latch is released and the fault is then allowed out, because a
        // swap whose outcome is unknown must not be reported as a clean
        // decline.
        var h = CreateHarness();
        h.A.TryUnlinkSuccessorAsync(Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>())
            .Returns<bool>(_ => throw new InvalidOperationException("predecessor unavailable"));

        Assert.That(async () => await h.Grain.ReclaimEmptyLeavesAsync(4),
            Throws.InstanceOf<InvalidOperationException>());

        await h.B.Received(1).AbandonRetirementAsync();
        await h.B.DidNotReceive().ClearGrainStateAsync();
    }

    [Test]
    public async Task A_routing_entry_that_will_not_retire_is_retried_and_then_given_up_on()
    {
        // Past the compare-and-swap the fold has committed, so a routing entry
        // that cannot be removed must not fail the fold: the range is already
        // served by the predecessor. It is worth retrying because until it
        // lands the latched leaf refuses writes on its range.
        var h = CreateHarness();
        h.Root.RemoveChildAsync(Arg.Any<GrainId>())
            .Returns<bool>(_ => throw new InvalidOperationException("parent unavailable"));

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.EqualTo(1),
            "the fold committed at the swap, so a failed routing retirement must still count");

        // MaxRetries is 2, so the initial attempt plus two retries.
        await h.Root.Received(3).RemoveChildAsync(h.LeafB);

        // The fold still finished its remaining tidy-up.
        await h.C.Received(1).SetPrevSiblingAsync(h.LeafA);
        await h.B.Received(1).ClearGrainStateAsync();
    }

    [Test]
    public async Task A_routing_entry_that_retires_on_a_retry_is_not_given_up_on()
    {
        // Falsifies the test above: the retry loop really does re-attempt and
        // succeed, rather than that arm merely counting three failures.
        var h = CreateHarness();
        var attempts = 0;
        h.Root.RemoveChildAsync(Arg.Any<GrainId>()).Returns<bool>(ci =>
            ++attempts == 1
                ? throw new InvalidOperationException("parent briefly unavailable")
                : h.RemoveChild(ci.Arg<GrainId>()));

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.EqualTo(1));
        Assert.That(attempts, Is.EqualTo(2), "the second attempt succeeded, so no third is made");
    }

    [Test]
    public async Task Tidy_up_that_fails_after_the_fold_committed_is_swallowed()
    {
        // Clearing the folded leaf's state runs AFTER the commit point and is
        // idempotent, so a failure in it is logged and swallowed: the leaf is
        // already unrouted and unlinked, and the next pass finishes the job.
        // Reporting the fold as failed here would have the pass treat a leaf it
        // has already unlinked as still present.
        var h = CreateHarness();
        h.B.ClearGrainStateAsync()
            .Returns(_ => Task.FromException(new InvalidOperationException("storage unavailable")));

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.EqualTo(1),
            "a fold that committed must be reported even when tidy-up failed");

        // The chain edits that precede the clear all landed, which is what
        // makes swallowing safe rather than merely quiet.
        await h.Root.Received(1).RemoveChildAsync(h.LeafB);
        await h.C.Received(1).SetPrevSiblingAsync(h.LeafA);
    }

    [Test]
    public async Task A_stale_back_pointer_is_repaired_as_the_walk_passes()
    {
        // A back pointer that does not name the predecessor the walk arrived
        // from is the fingerprint of a fold that unlinked a leaf and then died
        // before re-pointing the successor. It is repaired whether or not the
        // leaf is a fold candidate, so this stages a NON-candidate to prove the
        // repair is not a side effect of folding.
        var h = CreateHarness();
        var stranded = GrainId.Create("leaf", "reclaim-leaf-stranded");
        h.SetProbe(h.LeafB, h.Probes[h.LeafB] with
        {
            LiveRowCount = 3,          // not a candidate
            PrevSibling = stranded,    // and pointing at the wrong predecessor
        });

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.Zero);
        await h.B.Received(1).SetPrevSiblingAsync(h.LeafA);
    }

    [Test]
    public async Task A_back_pointer_that_already_names_the_predecessor_is_left_alone()
    {
        // Falsifies the repair test: on a healthy chain the walk must issue no
        // back-pointer write at all, or the assertion above would pass against
        // an implementation that re-pointed every leaf on every pass.
        var h = CreateHarness();
        h.SetProbe(h.LeafB, h.Probes[h.LeafB] with { LiveRowCount = 3 });

        await h.Grain.ReclaimEmptyLeavesAsync(4);

        await h.B.DidNotReceive().SetPrevSiblingAsync(Arg.Any<GrainId?>());
        await h.C.DidNotReceive().SetPrevSiblingAsync(Arg.Any<GrainId?>());
    }

    [Test]
    public async Task A_range_gap_between_chain_neighbours_is_closed()
    {
        // A predecessor whose high bound stops short of its successor's low
        // bound leaves a span no leaf declares. A write there routes to the
        // predecessor but falls outside the span its WAL materialiser accepts,
        // so the row would survive in cache and vanish on the next projection
        // rebuild. Closing it is monotonic.
        var h = CreateHarness();
        h.SetProbe(h.LeafA, h.Probes[h.LeafA] with { HighKeyExclusive = "aa" });

        await h.Grain.ReclaimEmptyLeavesAsync(4);

        await h.A.Received(1).AbsorbSuccessorRangeAsync("b");
    }

    [Test]
    public async Task A_predecessor_that_already_tiles_its_successor_is_left_alone()
    {
        // Falsifies the gap test: the healthy invariant is equality, and an
        // equal boundary must issue no widen at all.
        var h = CreateHarness();

        await h.Grain.ReclaimEmptyLeavesAsync(4);

        await h.A.DidNotReceive().AbsorbSuccessorRangeAsync(Arg.Any<string?>());
    }

    [Test]
    public async Task An_overlapping_predecessor_is_not_treated_as_a_gap()
    {
        // A predecessor wider than its successor's low bound is the transient
        // overlap a reclaim creates on purpose and repairs by retiring the
        // successor. Widening it further would be wrong.
        var h = CreateHarness();
        h.SetProbe(h.LeafA, h.Probes[h.LeafA] with { HighKeyExclusive = "bz" });

        await h.Grain.ReclaimEmptyLeavesAsync(4);

        await h.A.DidNotReceive().AbsorbSuccessorRangeAsync(Arg.Any<string?>());
    }

    [Test]
    public async Task A_walk_that_cannot_resume_where_it_stopped_restarts_from_the_head()
    {
        // A resume position is an optimisation, never a requirement, so a
        // resume that faults costs one re-walked prefix rather than the pass.
        // Stage it in two passes: the first stops on its budget with chain
        // still to its right, which is what records a resume position at all.
        var h = CreateHarness();
        h.SetProbe(h.LeafC, h.Probes[h.LeafC] with { LiveRowCount = 0 });

        var first = await h.Grain.ReclaimEmptyLeavesAsync(1);
        Assert.That(first, Is.EqualTo(1), "the first pass must stop on its budget to record a resume key");

        // The resume descent now faults once. The fallback re-walks from the
        // head, where leaf C is still an unfolded candidate.
        var faulted = false;
        h.Root.GetRoutingTableAsync().Returns(_ =>
        {
            if (!faulted)
            {
                faulted = true;
                throw new InvalidOperationException("routing table unavailable");
            }
            return Task.FromResult(h.Snapshot());
        });

        var second = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(faulted, Is.True, "the resume descent must have been the call that faulted");
        Assert.That(second, Is.EqualTo(1),
            "the pass recovered by restarting at the head and still folded the remaining empty leaf");
    }
}
