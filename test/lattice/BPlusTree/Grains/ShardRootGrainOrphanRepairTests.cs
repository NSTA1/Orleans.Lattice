using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the operator-invoked orphaned-leaf repair path
/// (<c>ShardRootGrain.OrphanRepair</c>, issue 3269).
/// <para>
/// An orphaned leaf is one spliced into a shard's sibling chain but reachable
/// by no descent from the shard root - what an interrupted split leaves behind
/// when it links the new sibling before its parent learns about it (issue
/// 3265). It is not cosmetic: nothing routes to it, so it never checkpoints,
/// so the write-ahead-log materialiser pin it published at birth never
/// advances and pins the trim floor forever, which stops the WAL trimming and
/// therefore stops it compacting. Before this pass the only remedy was to
/// rebuild the tree.
/// </para>
/// <para>
/// The empty-leaf reclaim pass cannot reach one, and the tests below pin why
/// rather than assuming it: <c>IsReclaimCandidate</c> rejects on its first
/// line for any leaf holding rows, and an orphan characteristically holds rows
/// - WAL replay admits records by <c>(ShardIndex, LowKeyInclusive,
/// HighKeyExclusive)</c> and never by leaf identity, so an orphan sharing
/// bounds with a live leaf materialises a full shadow copy of its range.
/// </para>
/// <para>
/// <b>What carries the safety argument here is the refusals.</b> The pass
/// destroys a leaf that is holding rows, so every negative test below asserts
/// the compensating action actually happened - nothing unlinked, nothing
/// cleared, the leaf unlatched - and the first test is a falsifier proving the
/// same harness really does complete a repair when nothing is staged against
/// it.
/// </para>
/// </summary>
[TestFixture]
public sealed partial class ShardRootGrainOrphanRepairTests
{
    private const string TreeId = "orphan-repair-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// A chain <c>A -&gt; B -&gt; C</c> in which <b>B is the orphan</b>: it is
    /// spliced between two live leaves from both sides, yet the root routes
    /// only to A and C, so no descent reaches it.
    /// <para>
    /// This mirrors the one orphan pair ever measured in the field - two
    /// unparented leaves spliced between two live ones, declaring the same
    /// bounds as a live leaf and holding a shadow copy of its rows - which is
    /// why B's keys are also present on A rather than being unique to B.
    /// </para>
    /// <para>
    /// The substitutes are a live model, not frozen returns: an unlink and a
    /// back-pointer repair mutate the modelled chain exactly as the real
    /// grains would. The walk re-probes the predecessor after every repair and
    /// keeps going, so against frozen stubs it would re-examine the same leaf
    /// until the budget ran out and every "exactly one" assertion below would
    /// be measuring the stub instead of the grain.
    /// </para>
    /// </summary>
    private sealed class OrphanHarness
    {
        public ShardRootGrain Grain { get; set; } = null!;
        public required IBPlusInternalGrain Root { get; init; }
        public required GrainId LeafA { get; init; }
        public required GrainId LeafB { get; init; }
        public required GrainId LeafC { get; init; }
        public required Dictionary<GrainId, IBPlusLeafGrain> Leaves { get; init; }
        public required Dictionary<GrainId, LeafReclaimProbe> Probes { get; init; }
        public required Dictionary<GrainId, List<string>> Keys { get; init; }
        public required List<GrainId> ChildIds { get; init; }
        public required List<string?> Separators { get; init; }

        public IBPlusLeafGrain A => Leaves[LeafA];
        public IBPlusLeafGrain B => Leaves[LeafB];
        public IBPlusLeafGrain C => Leaves[LeafC];

        public void SetProbe(GrainId id, LeafReclaimProbe probe) => Probes[id] = probe;

        public RoutingTableSnapshot Snapshot() => new()
        {
            SeparatorKeys = [.. Separators],
            ChildIds = [.. ChildIds],
            ChildrenAreLeaves = true,
        };
    }

    private static byte[] ValueFor(string key) => System.Text.Encoding.UTF8.GetBytes(key);

    /// <summary>
    /// Builds the chain. <paramref name="orphanKeys"/> are the rows the orphan
    /// materialised; <paramref name="liveKeys"/> are the rows leaf A holds and
    /// therefore the ones the pass can prove a duplicate of. Passing an orphan
    /// key that is absent from <paramref name="liveKeys"/> is how a test stages
    /// a uniquely-held row - the case the pass must refuse.
    /// </summary>
    private static OrphanHarness CreateHarness(
        string[]? orphanKeys = null,
        string[]? liveKeys = null,
        string? orphanHigh = "c")
    {
        orphanKeys ??= ["b1", "b2"];
        liveKeys ??= ["b1", "b2"];

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var rootId = GrainId.Create("internal", "orphan-root");
        var leafA = GrainId.Create("leaf", "orphan-leaf-a");
        var leafB = GrainId.Create("leaf", "orphan-leaf-b");
        var leafC = GrainId.Create("leaf", "orphan-leaf-c");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();

        var probes = new Dictionary<GrainId, LeafReclaimProbe>
        {
            // A is live and routed, and owns everything below "c".
            [leafA] = new()
            {
                LiveRowCount = liveKeys.Length,
                PrevSibling = null,
                NextSibling = leafB,
                LowKeyInclusive = null,
                HighKeyExclusive = "c",
            },

            // B is the orphan. Note LiveRowCount is NON-ZERO: this is what
            // makes it invisible to the empty-leaf reclaim pass forever, and
            // it is the ordinary case rather than a contrived one.
            [leafB] = new()
            {
                LiveRowCount = orphanKeys.Length,
                PrevSibling = leafA,
                NextSibling = leafC,
                LowKeyInclusive = "b",
                HighKeyExclusive = orphanHigh,
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

        var harness = new OrphanHarness
        {
            Root = Substitute.For<IBPlusInternalGrain>(),
            LeafA = leafA,
            LeafB = leafB,
            LeafC = leafC,
            Leaves = [],
            Probes = probes,
            Keys = new Dictionary<GrainId, List<string>>
            {
                [leafA] = [.. liveKeys],
                [leafB] = [.. orphanKeys],
                [leafC] = ["c1"],
            },

            // THE WHOLE POINT: B is absent from the root's children, so a
            // descent on any key in ["b","c") lands on A and never on B.
            ChildIds = [leafA, leafC],
            Separators = [null, "c"],
        };

        var root = harness.Root;
        root.GetRoutingTableAsync().Returns(_ => Task.FromResult(harness.Snapshot()));
        root.GetChildIdsAsync().Returns(_ => Task.FromResult(new List<GrainId>(harness.ChildIds)));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(root);

        foreach (var id in new[] { leafA, leafB, leafC })
        {
            var self = id;
            var leaf = Substitute.For<IBPlusLeafGrain>();

            leaf.GetReclaimProbeAsync().Returns(_ => Task.FromResult(probes[self]));
            leaf.TryBeginOrphanRetirementAsync().Returns(Task.FromResult(true));
            leaf.AbandonRetirementAsync().Returns(Task.CompletedTask);
            leaf.ClearGrainStateAsync().Returns(Task.CompletedTask);

            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(new List<string>(harness.Keys[self])));

            leaf.GetAsync(Arg.Any<string>()).Returns(ci =>
            {
                var key = ci.Arg<string>();
                return Task.FromResult<byte[]?>(
                    harness.Keys[self].Contains(key) ? ValueFor(key) : null);
            });

            leaf.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(ci =>
            {
                probes[self] = probes[self] with { PrevSibling = ci.Arg<GrainId?>() };
                return Task.CompletedTask;
            });

            // Models TryUnlinkSuccessorAsync faithfully, INCLUDING its
            // monotonic widen rule, so a caller that passed the wrong absorb
            // bound would be visible here as a widened predecessor rather than
            // being silently accepted.
            leaf.TryUnlinkSuccessorAsync(Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>())
                .Returns(ci =>
                {
                    var probe = probes[self];
                    if (probe.NextSibling != ci.ArgAt<GrainId>(0)) return Task.FromResult(false);

                    var absorb = ci.ArgAt<string?>(2);
                    var high = probe.HighKeyExclusive;
                    var widened = high is not null
                        && (absorb is null || string.CompareOrdinal(absorb, high) > 0)
                            ? absorb
                            : high;

                    probes[self] = probe with
                    {
                        NextSibling = ci.ArgAt<GrainId?>(1),
                        HighKeyExclusive = widened,
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

    private static Task<OrphanedLeafRepairPage> RepairAsync(OrphanHarness h) =>
        h.Grain.RepairOrphanedLeavesAsync(null, dryRun: false);

    private static Task<OrphanedLeafRepairPage> InspectAsync(OrphanHarness h) =>
        h.Grain.RepairOrphanedLeavesAsync(null, dryRun: true);

    // ========================================================================
    // The repair
    // ========================================================================

    [Test]
    public async Task A_verified_orphan_is_unspliced_and_its_pin_retired()
    {
        // Falsifier for every negative test below: with nothing staged against
        // it the same harness really does complete a repair, so a later "did
        // not repair" assertion is evidence about the arm and not about the
        // harness.
        var h = CreateHarness();

        var page = await RepairAsync(h);

        Assert.That(page.Findings, Has.Count.EqualTo(1));
        var finding = page.Findings[0];
        Assert.Multiple(() =>
        {
            Assert.That(finding.Disposition, Is.EqualTo(OrphanedLeafDisposition.Repaired));
            Assert.That(finding.LeafId, Is.EqualTo(h.LeafB.ToString()));
            Assert.That(finding.KeyCount, Is.EqualTo(2));
            Assert.That(finding.VerifiedKeyCount, Is.EqualTo(2),
                "every key must be proven duplicated before the leaf is removed");
            Assert.That(finding.UnverifiedKey, Is.Null);
            Assert.That(finding.IsRefusal, Is.False);
        });

        // Unspliced from both sides.
        await h.A.Received(1).TryUnlinkSuccessorAsync(h.LeafB, h.LeafC, Arg.Any<string?>());
        await h.C.Received(1).SetPrevSiblingAsync(h.LeafA);

        // The step that actually recovers the WAL: clearing the state is what
        // retires the materialiser pin that was gating the trim floor. An
        // unspliced-but-uncleared leaf still pins it, so this assertion is the
        // one that says the issue is fixed.
        await h.B.Received(1).ClearGrainStateAsync();
        await h.B.DidNotReceive().AbandonRetirementAsync();

        // The whole chain was examined, so there is nothing to resume.
        Assert.That(page.ResumeFromInclusive, Is.Null);
    }

    [Test]
    public async Task The_unsplice_does_not_widen_the_predecessor_onto_the_orphans_range()
    {
        // THE most important assertion in this fixture, and the one divergence
        // from the empty-leaf fold that is most likely to be "corrected" back
        // into a bug.
        //
        // Reclaim hands the folded leaf's range to the predecessor, because a
        // reclaimed leaf IS routed and its range would otherwise be orphaned.
        // An orphan is NOT routed, so the routed leaves already tile the
        // keyspace completely - here leaf A already owns everything below "c",
        // including the whole of B's declared range. Widening A onto it would
        // be a no-op at best and, on a chain where the orphan reaches further
        // right than its predecessor, would make A overlap a LIVE leaf, and
        // two leaves declaring one range materialise every record in it twice.
        var h = CreateHarness();

        var highBefore = h.Probes[h.LeafA].HighKeyExclusive;

        await RepairAsync(h);

        Assert.That(h.Probes[h.LeafA].HighKeyExclusive, Is.EqualTo(highBefore),
            "the predecessor's declared range must be untouched by an orphan unsplice");

        // Pinned at the call site as well as at the effect, because the effect
        // is only a no-op here by virtue of the argument being right: the
        // absorb bound must be the PREDECESSOR's own high bound, which makes
        // the widen provably no-op inside TryUnlinkSuccessorAsync. Passing
        // null - the obvious-looking "don't widen" value - would widen it to
        // unbounded.
        await h.A.Received(1).TryUnlinkSuccessorAsync(h.LeafB, h.LeafC, highBefore);
    }

    [Test]
    public async Task An_orphan_reaching_further_right_than_its_predecessor_still_does_not_widen_it()
    {
        // The test above cannot fail if the wrong absorb bound is passed,
        // because there the predecessor and the orphan declare the SAME high
        // bound - which is the shape the one measured orphan pair had, and
        // exactly why it is not sufficient evidence. Here the orphan declares
        // ["b","d"), reaching past its predecessor's "c" and over the whole of
        // the live leaf C.
        //
        // Passing the orphan's high bound - what the empty-leaf fold passes,
        // and the single most plausible wrong answer - widens A to "d" and
        // makes it overlap C. Two leaves declaring one range materialise every
        // record in it twice, which is the corruption this argument exists to
        // prevent.
        var h = CreateHarness(orphanHigh: "d");

        var page = await RepairAsync(h);

        Assert.That(page.Findings[0].Disposition, Is.EqualTo(OrphanedLeafDisposition.Repaired),
            "the repair must actually have run, or this proves nothing");
        Assert.That(h.Probes[h.LeafA].HighKeyExclusive, Is.EqualTo("c"),
            "the predecessor must not absorb an orphan's range, which a live leaf already owns");
        await h.A.Received(1).TryUnlinkSuccessorAsync(h.LeafB, h.LeafC, "c");
    }

    [Test]
    public async Task A_healthy_chain_is_left_alone()
    {
        // Control. Every leaf is routed, so the pass must find nothing and
        // touch nothing - this is what stops the pass being a tree shredder
        // that happens to pass its positive test.
        var h = CreateHarness();
        h.ChildIds.Insert(1, h.LeafB);
        h.Separators.Insert(1, "b");

        var page = await RepairAsync(h);

        Assert.That(page.Findings, Is.Empty);
        Assert.That(page.LeavesWalked, Is.EqualTo(2), "the walk must actually have run");
        await h.A.DidNotReceive().TryUnlinkSuccessorAsync(
            Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>());
        await h.B.DidNotReceive().ClearGrainStateAsync();
        await h.B.DidNotReceive().TryBeginOrphanRetirementAsync();
    }

    // ========================================================================
    // Failing closed
    // ========================================================================

    [Test]
    public async Task An_orphan_holding_a_key_no_live_leaf_holds_is_refused()
    {
        // THE load-bearing safety property. The orphan holds "b3", which leaf A
        // does not, so removing the orphan would destroy the only copy of that
        // row. The pass must refuse and change nothing.
        //
        // Key duplication is NOT assumed anywhere. In the one orphan pair ever
        // measured the keys were duplicated on live leaves, but that is a
        // sample of one, and nothing in the interrupted-split seam that mints
        // an orphan guarantees it.
        var h = CreateHarness(orphanKeys: ["b1", "b3"], liveKeys: ["b1", "b2"]);

        var page = await RepairAsync(h);

        Assert.That(page.Findings, Has.Count.EqualTo(1));
        var finding = page.Findings[0];
        Assert.Multiple(() =>
        {
            Assert.That(finding.Disposition,
                Is.EqualTo(OrphanedLeafDisposition.RefusedUnverifiedKeys));
            Assert.That(finding.UnverifiedKey, Is.EqualTo("b3"),
                "the operator must be told which row blocked the repair");
            Assert.That(finding.KeyCount, Is.EqualTo(2));
            Assert.That(finding.VerifiedKeyCount, Is.EqualTo(1));
            Assert.That(finding.IsRefusal, Is.True);
        });

        // Nothing at all happened to the tree.
        await h.B.DidNotReceive().TryBeginOrphanRetirementAsync();
        await h.A.DidNotReceive().TryUnlinkSuccessorAsync(
            Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>());
        await h.B.DidNotReceive().ClearGrainStateAsync();
        await h.C.DidNotReceive().SetPrevSiblingAsync(Arg.Any<GrainId?>());
    }

    [Test]
    public async Task An_orphan_carrying_blocking_state_is_refused_without_being_enumerated()
    {
        // A split, seal or prepared transaction can resurrect rows the leaf
        // does not currently hold, which would invalidate the key-duplication
        // proof after it was taken. Refused before the keys are even read,
        // because enumerating them could not produce a usable proof.
        var h = CreateHarness();
        h.SetProbe(h.LeafB, h.Probes[h.LeafB] with { HasBlockingState = true });

        var page = await RepairAsync(h);

        Assert.That(page.Findings, Has.Count.EqualTo(1));
        Assert.That(page.Findings[0].Disposition,
            Is.EqualTo(OrphanedLeafDisposition.RefusedBlockingState));

        await h.B.DidNotReceive().GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(),
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>());
        await h.B.DidNotReceive().ClearGrainStateAsync();
    }

    [Test]
    public async Task An_orphan_that_will_not_latch_is_refused_after_verification()
    {
        // The latch is the leaf's own last word, and it refuses on a mutation
        // in flight. On a leaf nothing routes to, a mutation in flight is
        // evidence AGAINST the unreachability finding, so it is reported
        // rather than waited out.
        var h = CreateHarness();
        h.B.TryBeginOrphanRetirementAsync().Returns(Task.FromResult(false));

        var page = await RepairAsync(h);

        Assert.That(page.Findings, Has.Count.EqualTo(1));
        Assert.That(page.Findings[0].Disposition,
            Is.EqualTo(OrphanedLeafDisposition.RefusedBlockingState));

        // Everything before the latch was read-only, so the tree is as found.
        await h.A.DidNotReceive().TryUnlinkSuccessorAsync(
            Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>());
        await h.B.DidNotReceive().ClearGrainStateAsync();

        // Nothing was latched, so there is nothing to unlatch.
        await h.B.DidNotReceive().AbandonRetirementAsync();
    }

    [Test]
    public async Task A_predecessor_that_moved_under_the_swap_unlatches_the_orphan()
    {
        // The compare-and-swap declined, so the chain changed under the walk.
        // The latch taken a moment ago must be released or the leaf stays
        // frozen for the rest of its life - and it is still an orphan, so a
        // frozen one is a pinned WAL that a re-run can no longer clear.
        var h = CreateHarness();
        h.A.TryUnlinkSuccessorAsync(Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>())
            .Returns(Task.FromResult(false));

        var page = await RepairAsync(h);

        Assert.That(page.Findings, Has.Count.EqualTo(1));
        Assert.That(page.Findings[0].Disposition,
            Is.EqualTo(OrphanedLeafDisposition.RefusedChainRace));

        await h.B.Received(1).AbandonRetirementAsync();
        await h.B.DidNotReceive().ClearGrainStateAsync();
        await h.C.DidNotReceive().SetPrevSiblingAsync(Arg.Any<GrainId?>());
    }

    [Test]
    public async Task A_throwing_swap_unlatches_the_orphan_before_it_propagates()
    {
        // Same compensation on the exceptional path: a swap whose outcome is
        // unknown must not leave the leaf latched.
        var h = CreateHarness();
        h.A.TryUnlinkSuccessorAsync(Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>())
            .Returns<Task<bool>>(_ => throw new TimeoutException("storage"));

        Assert.That(async () => await RepairAsync(h), Throws.TypeOf<TimeoutException>());

        await h.B.Received(1).AbandonRetirementAsync();
        await h.B.DidNotReceive().ClearGrainStateAsync();
    }

    [Test]
    public async Task A_leaf_that_routes_back_to_itself_on_one_of_its_keys_is_refused()
    {
        // The low-bound descent said the leaf is unreachable; a descent on one
        // of its own keys says it is not. Routing is a total function, so both
        // cannot be true, and a contradiction is not something to resolve in
        // favour of deleting a leaf.
        //
        // Staged by making the routing table name B as the owner of ["b","c")
        // WITHOUT B's low bound descending to it - the low bound "b" still
        // resolves through the separator list to A, while the key "b2" lands
        // on B.
        var h = CreateHarness();
        h.ChildIds.Insert(1, h.LeafB);
        h.Separators.Insert(1, "b2");

        var page = await RepairAsync(h);

        Assert.That(page.Findings, Has.Count.EqualTo(1));
        var finding = page.Findings[0];
        Assert.Multiple(() =>
        {
            Assert.That(finding.Disposition,
                Is.EqualTo(OrphanedLeafDisposition.RefusedRoutingContradiction));
            Assert.That(finding.UnverifiedKey, Is.EqualTo("b2"));
        });

        await h.B.DidNotReceive().TryBeginOrphanRetirementAsync();
        await h.B.DidNotReceive().ClearGrainStateAsync();
    }

    // ========================================================================
    // Inspection
    // ========================================================================

    [Test]
    public async Task A_dry_run_reports_the_orphan_as_repairable_and_changes_nothing()
    {
        var h = CreateHarness();

        var page = await InspectAsync(h);

        Assert.That(page.Findings, Has.Count.EqualTo(1));
        var finding = page.Findings[0];
        Assert.Multiple(() =>
        {
            Assert.That(finding.Disposition, Is.EqualTo(OrphanedLeafDisposition.Repairable));
            Assert.That(finding.VerifiedKeyCount, Is.EqualTo(2),
                "the dry run must run the SAME verification the repair would");
            Assert.That(finding.IsRefusal, Is.False);
        });

        await h.B.DidNotReceive().TryBeginOrphanRetirementAsync();
        await h.A.DidNotReceive().TryUnlinkSuccessorAsync(
            Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>());
        await h.B.DidNotReceive().ClearGrainStateAsync();
        await h.C.DidNotReceive().SetPrevSiblingAsync(Arg.Any<GrainId?>());
    }

    [Test]
    public async Task A_dry_run_reaches_the_same_refusal_the_repair_would()
    {
        // The property that makes the inspection verb worth having: an
        // operator who runs it and sees a refusal has learned what the repair
        // would do, not what a second implementation guesses it would do.
        var h = CreateHarness(orphanKeys: ["b1", "b3"], liveKeys: ["b1", "b2"]);

        var inspected = await InspectAsync(h);
        var repaired = await RepairAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(inspected.Findings[0].Disposition,
                Is.EqualTo(OrphanedLeafDisposition.RefusedUnverifiedKeys));
            Assert.That(repaired.Findings[0].Disposition,
                Is.EqualTo(inspected.Findings[0].Disposition));
            Assert.That(repaired.Findings[0].UnverifiedKey,
                Is.EqualTo(inspected.Findings[0].UnverifiedKey));
        });
    }

    // ========================================================================
    // Declining
    // ========================================================================

    [Test]
    public async Task A_single_leaf_shard_is_declined_rather_than_walked()
    {
        // One leaf, and it is the root, so it cannot be unreachable from
        // itself. Declining is reported as completion because a tree-level
        // drive fans out over every shard and most have nothing wrong.
        var h = CreateHarness();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = h.LeafA;
        state.State.RootIsLeaf = true;
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(h.A);
        var grain = new ShardRootGrain(
            context, state, factory,
            TestOptionsResolver.Create(factory: factory),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        var page = await grain.RepairOrphanedLeavesAsync(null, dryRun: false);

        Assert.Multiple(() =>
        {
            Assert.That(page.Findings, Is.Empty);
            Assert.That(page.LeavesWalked, Is.Zero);
            Assert.That(page.ResumeFromInclusive, Is.Null);
        });
        await h.A.DidNotReceive().GetReclaimProbeAsync();
    }

    [Test]
    public void A_cancelled_request_is_refused_before_any_work()
    {
        var h = CreateHarness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await h.Grain.RepairOrphanedLeavesAsync(null, false, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
