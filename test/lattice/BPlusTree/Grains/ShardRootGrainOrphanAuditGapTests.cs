using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the orphaned-leaf audit's blind spots and for the gap channel
/// that closes them (issue 3301).
/// <para>
/// <b>The defect this fixture exists for.</b> The audit enumerates candidate
/// leaves from the HEAD of the shard's sibling chain and advances purely by
/// <c>NextSibling</c>; descent is only the per-leaf predicate. The chain is
/// therefore both the enumeration path and the structure an orphan damages, so
/// a pointer severed part-way across the keyspace ended the walk, the drive
/// read the resulting null resume position as completion, and the shard was
/// reported examined and clean. Range scans are not anchored that way - they
/// enter the chain by descending on their own lower bound - so they reached the
/// segment past the break and went on emitting chain-repair warnings about
/// leaves the audit could not see. Both were telling the truth about the same
/// shard.
/// </para>
/// <para>
/// <b>Why the gaps are a separate channel from the findings.</b> A finding
/// says "there is an orphan here" and carries a disposition the pass actually
/// reached. A gap says "I could not tell whether there is an orphan here". A
/// vacuous clean result - no findings because nothing was examined - is worse
/// than an error, because an operator acts on it. Every arm below therefore
/// asserts the gap as well as the finding, and the first test is a falsifier
/// proving a healthy chain produces no gaps at all, so a later "reported a
/// gap" assertion is evidence about the arm and not about a fix that
/// manufactures gaps everywhere.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainOrphanAuditGapTests
{
    private const string TreeId = "orphan-audit-gap-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// One leaf in a modelled shard. <paramref name="Routed"/> false is what
    /// makes a leaf an orphan: it is spliced into the chain but absent from
    /// the root's children, so no descent reaches it.
    /// <para>
    /// <paramref name="Separator"/> is the key the ROOT files this leaf under,
    /// and it defaults to the leaf's own declared low bound because that is
    /// what a healthy tree looks like. Setting it to something else models the
    /// leaf whose declared bounds disagree with the routing that reaches it,
    /// which is the whole of what "descent-unreachable" means.
    /// </para>
    /// </summary>
    private sealed record LeafSpec(
        string Name,
        string? Low,
        string? High,
        bool Routed,
        string? Next,
        string[]? Keys = null,
        string? Separator = null,
        bool SeparatorOverridden = false);

    private sealed class ChainHarness
    {
        public ShardRootGrain Grain { get; set; } = null!;
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required Dictionary<string, GrainId> Ids { get; init; }
        public required Dictionary<GrainId, LeafReclaimProbe> Probes { get; init; }
        public required Dictionary<GrainId, IBPlusLeafGrain> Leaves { get; init; }
        public required Dictionary<GrainId, List<string>> Keys { get; init; }
        public required List<GrainId> ChildIds { get; init; }
        public required List<string?> Separators { get; init; }

        /// <summary>
        /// When set, the first routing-table read parks here. Used to hold one
        /// pass inside the grain while a second observes the latch, rather
        /// than racing it.
        /// </summary>
        public TaskCompletionSource? Gate { get; set; }

        /// <summary>Completes once <see cref="Gate"/> has actually been hit.</summary>
        public TaskCompletionSource? GateHit { get; set; }

        public GrainId Id(string name) => Ids[name];

        public RoutingTableSnapshot Snapshot() => new()
        {
            SeparatorKeys = [.. Separators],
            ChildIds = [.. ChildIds],
            ChildrenAreLeaves = true,
        };
    }

    private static byte[] ValueFor(string key) => System.Text.Encoding.UTF8.GetBytes(key);

    /// <summary>
    /// Builds a shard from an ordered leaf spec. The routed leaves become the
    /// root's children in the order given, each keyed by its own low bound, so
    /// a descent on any key lands on the routed leaf whose declared range
    /// contains it. Unrouted leaves exist only in the sibling chain.
    /// </summary>
    private static ChainHarness Build(params LeafSpec[] specs)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var rootId = GrainId.Create("internal", "audit-gap-root");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var ids = specs.ToDictionary(
            s => s.Name,
            s => GrainId.Create("leaf", "audit-gap-leaf-" + s.Name));

        var probes = new Dictionary<GrainId, LeafReclaimProbe>();
        var keys = new Dictionary<GrainId, List<string>>();

        foreach (var spec in specs)
        {
            var id = ids[spec.Name];
            var prev = specs.FirstOrDefault(s => s.Next == spec.Name);

            probes[id] = new LeafReclaimProbe
            {
                LiveRowCount = spec.Keys?.Length ?? 1,
                PrevSibling = prev is null ? null : ids[prev.Name],
                NextSibling = spec.Next is null ? null : ids[spec.Next],
                LowKeyInclusive = spec.Low,
                HighKeyExclusive = spec.High,
            };

            keys[id] = [.. spec.Keys ?? [spec.Name + "1"]];
        }

        var routed = specs.Where(s => s.Routed).ToList();

        var harness = new ChainHarness
        {
            State = state,
            Ids = ids,
            Probes = probes,
            Leaves = [],
            Keys = keys,
            ChildIds = [.. routed.Select(s => ids[s.Name])],
            Separators = [.. routed.Select(s => s.SeparatorOverridden ? s.Separator : s.Low)],
        };

        var factory = Substitute.For<IGrainFactory>();

        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(async _ =>
        {
            if (harness.Gate is { } gate)
            {
                harness.GateHit?.TrySetResult();
                await gate.Task;
            }

            return harness.Snapshot();
        });
        root.GetChildIdsAsync().Returns(_ => Task.FromResult(new List<GrainId>(harness.ChildIds)));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(root);

        foreach (var spec in specs)
        {
            var self = ids[spec.Name];
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

            leaf.TryUnlinkSuccessorAsync(Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>())
                .Returns(ci =>
                {
                    var probe = probes[self];
                    if (probe.NextSibling != ci.ArgAt<GrainId>(0)) return Task.FromResult(false);

                    probes[self] = probe with { NextSibling = ci.ArgAt<GrainId?>(1) };
                    return Task.FromResult(true);
                });

            harness.Leaves[self] = leaf;
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

    private static Task<OrphanedLeafRepairPage> InspectAsync(ChainHarness h, string? resume = null) =>
        h.Grain.RepairOrphanedLeavesAsync(resume, dryRun: true);

    private static Task<OrphanedLeafRepairPage> RepairAsync(ChainHarness h, string? resume = null) =>
        h.Grain.RepairOrphanedLeavesAsync(resume, dryRun: false);

    private static IEnumerable<OrphanedLeafAuditGapReason> Reasons(OrphanedLeafRepairPage page) =>
        page.Gaps.Select(g => g.Reason);

    /// <summary>
    /// A healthy shard: every leaf routed, the chain intact and terminating on
    /// the leaf a rightmost descent reaches.
    /// </summary>
    private static ChainHarness HealthyChain() => Build(
        new LeafSpec("a", null, "c", Routed: true, Next: "c"),
        new LeafSpec("c", "c", "e", Routed: true, Next: "e"),
        new LeafSpec("e", "e", null, Routed: true, Next: null));

    // ========================================================================
    // The falsifier
    // ========================================================================

    [Test]
    public async Task A_healthy_chain_reports_no_gaps_and_a_complete_verdict()
    {
        // Falsifier for every arm below. The end-of-chain check added for this
        // issue descends to the shard's rightmost leaf on EVERY terminal
        // pointer, so the failure mode of the fix itself is manufacturing a
        // gap on a shard that has nothing wrong with it. If this test ever goes
        // red, the gaps asserted below stop being evidence of anything.
        var h = HealthyChain();

        var page = await InspectAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(page.Findings, Is.Empty);
            Assert.That(page.Gaps, Is.Empty,
                "a healthy chain must not be reported as partially examined");
            Assert.That(page.ResumeFromInclusive, Is.Null);
            Assert.That(page.LeavesWalked, Is.EqualTo(2));
        });
    }

    // ========================================================================
    // The primary defect: an unvalidated end of chain
    // ========================================================================

    [Test]
    public async Task A_chain_severed_mid_keyspace_is_re_entered_and_the_orphan_past_the_break_is_found()
    {
        // THE headline test. Leaf c's successor pointer is severed while leaf
        // e - the shard's rightmost leaf by descent - still lives past it with
        // an orphan spliced after it.
        //
        // On the unfixed walk this shard reports LeavesWalked 1, Findings [],
        // ResumeFromInclusive null, which the drive reads as "examined, clean".
        // That is the exact reading the field produced on a tree that was
        // simultaneously emitting thousands of chain-repair warnings a minute.
        var h = Build(
            new LeafSpec("a", null, "c", Routed: true, Next: "c"),
            new LeafSpec("c", "c", "e", Routed: true, Next: null),
            new LeafSpec("e", "e", null, Routed: true, Next: "x", Keys: ["e1"]),
            new LeafSpec("x", "e", "f", Routed: false, Next: null, Keys: ["e1"]));

        var page = await InspectAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(Reasons(page), Does.Contain(OrphanedLeafAuditGapReason.ChainTruncated),
                "the severed pointer is a real defect this pass walks around rather than repairs");
            Assert.That(page.Findings, Has.Count.EqualTo(1),
                "the orphan past the break must be reachable by the audit, exactly once");
        });

        var finding = page.Findings[0];
        Assert.Multiple(() =>
        {
            Assert.That(finding.LeafId, Is.EqualTo(h.Id("x").ToString()));
            Assert.That(finding.Disposition, Is.EqualTo(OrphanedLeafDisposition.Repairable));
            Assert.That(finding.VerifiedKeyCount, Is.EqualTo(1));
        });

        var gap = page.Gaps.Single(g => g.Reason == OrphanedLeafAuditGapReason.ChainTruncated);
        Assert.Multiple(() =>
        {
            Assert.That(gap.LeafId, Is.EqualTo(h.Id("c").ToString()),
                "the gap names the leaf whose successor pointer was severed");
            Assert.That(gap.KeyHint, Is.EqualTo("e"), "and the key the walk re-entered on");
            Assert.That(gap.ShardIndex, Is.EqualTo(0));
        });

        // An inspection leaves the orphan spliced, so the chain still ends on
        // a leaf nothing routes to and the pass genuinely cannot establish
        // what lies past it. Saying so is the point of the channel - the
        // repair below removes the orphan and the same shard then comes back
        // with a complete verdict.
        Assert.That(Reasons(page),
            Does.Contain(OrphanedLeafAuditGapReason.ChainTruncatedUnrecoverable));
    }

    [Test]
    public async Task The_repair_reaches_an_orphan_past_a_break_as_well_as_the_audit()
    {
        // Load-bearing for what an operator is told. Inspection and repair
        // share RepairOrphanedLeavesCoreAsync and dryRun gates only the
        // mutating tail, so before this fix BOTH were blind to these leaves and
        // they had no remedy short of a tree rebuild. This pins that the shared
        // enumeration now actually delivers the repair, not just the verdict.
        var h = Build(
            new LeafSpec("a", null, "c", Routed: true, Next: "c"),
            new LeafSpec("c", "c", "e", Routed: true, Next: null),
            new LeafSpec("e", "e", null, Routed: true, Next: "x", Keys: ["e1"]),
            new LeafSpec("x", "e", "f", Routed: false, Next: null, Keys: ["e1"]));

        var page = await RepairAsync(h);

        Assert.That(page.Findings, Has.Count.EqualTo(1));
        Assert.That(page.Findings[0].Disposition, Is.EqualTo(OrphanedLeafDisposition.Repaired));

        // Having removed the orphan, the chain ends where the tree ends, so
        // the shard's verdict is complete apart from the severed pointer the
        // pass walked around.
        Assert.That(Reasons(page),
            Does.Not.Contain(OrphanedLeafAuditGapReason.ChainTruncatedUnrecoverable));

        // The step that recovers the WAL: clearing the state retires the
        // materialiser pin holding the trim floor down.
        await h.Leaves[h.Id("x")].Received(1).ClearGrainStateAsync();
        await h.Leaves[h.Id("e")].Received(1)
            .TryUnlinkSuccessorAsync(h.Id("x"), null, Arg.Any<string?>());
    }

    [Test]
    public async Task A_severed_chain_with_no_key_to_re_enter_on_is_reported_unrecoverable()
    {
        // The severed leaf claims the keyspace to the unbounded end, so there
        // is no key to descend on and no way back into the chain. The pass
        // cannot examine the remainder and must say so rather than return the
        // empty finding list that reads as a clean shard.
        var h = Build(
            new LeafSpec("a", null, "c", Routed: true, Next: "c"),
            new LeafSpec("c", "c", null, Routed: true, Next: null),
            new LeafSpec("e", "e", null, Routed: true, Next: null));

        var page = await InspectAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(Reasons(page),
                Does.Contain(OrphanedLeafAuditGapReason.ChainTruncatedUnrecoverable));
            Assert.That(page.Findings, Is.Empty);
        });

        var gap = page.Gaps.Single();
        Assert.Multiple(() =>
        {
            Assert.That(gap.LeafId, Is.EqualTo(h.Id("c").ToString()));
            Assert.That(gap.KeyHint, Is.Null);
        });
    }

    [Test]
    public async Task A_re_entry_that_lands_back_on_the_severed_leaf_is_refused_rather_than_looped()
    {
        // The severed leaf declares a high bound that routes straight back to
        // itself. Descending on it would re-enter where the walk already is and
        // spin the pass for its whole budget, so re-entry must advance or not
        // happen.
        var h = Build(
            new LeafSpec("a", null, "c", Routed: true, Next: "c"),
            new LeafSpec("c", "c", "d", Routed: true, Next: null),
            new LeafSpec("e", "e", null, Routed: true, Next: null));

        var page = await InspectAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(Reasons(page),
                Does.Contain(OrphanedLeafAuditGapReason.ChainTruncatedUnrecoverable));
            Assert.That(page.Gaps.Single().KeyHint, Is.EqualTo("d"));
            Assert.That(page.LeavesWalked, Is.LessThanOrEqualTo(4),
                "a refused re-entry must stop the walk, not spin it");
        });
    }

    // ========================================================================
    // Shard-level declines
    // ========================================================================

    [Test]
    public async Task A_shard_mid_split_declines_with_a_gap_instead_of_a_clean_page()
    {
        // On a 64-shard tree this was 64 chances to contribute a silent zero to
        // a tree-level report: the decline returned the same empty page shape a
        // genuinely clean shard returns, and its own doc said declining was
        // reported as completion.
        var h = HealthyChain();
        h.State.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = ShardSplitPhase.BeginShadowWrite,
            ShadowTargetShardIndex = 1,
            MovedSlots = [1],
            VirtualShardCount = 8,
        };

        var page = await InspectAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(Reasons(page),
                Does.Contain(OrphanedLeafAuditGapReason.ShardSplitInProgress));
            Assert.That(page.Findings, Is.Empty);
            Assert.That(page.LeavesWalked, Is.Zero);
        });
    }

    [Test]
    public async Task A_concurrent_pass_declines_with_a_gap_instead_of_a_clean_page()
    {
        // Same silent-zero shape as the split decline: a shard already
        // draining returned the empty page a clean shard returns, so a
        // tree-level report could be assembled entirely out of shards that
        // never looked.
        var h = HealthyChain();
        h.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.GateHit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Hold the first pass inside its first routing read so the second
        // observes the latch rather than racing it.
        var first = h.Grain.RepairOrphanedLeavesAsync(null, dryRun: true);
        await h.GateHit.Task;

        var second = await h.Grain.RepairOrphanedLeavesAsync(null, dryRun: true);

        h.Gate.SetResult();
        await first;

        Assert.Multiple(() =>
        {
            Assert.That(Reasons(second),
                Does.Contain(OrphanedLeafAuditGapReason.ShardPassAlreadyRunning));
            Assert.That(second.Findings, Is.Empty);
            Assert.That(second.LeavesWalked, Is.Zero);
        });
    }

    // ========================================================================
    // Undecidable leaves
    // ========================================================================

    [Test]
    public async Task A_mid_chain_leaf_with_no_low_bound_is_reported_rather_than_skipped_silently()
    {
        // The range-scan chain guard judges this same population the OTHER way:
        // a leaf declaring a trailing edge while leaving its leading edge unset
        // is claiming the keyspace from the unbounded end, and the scan treats
        // it as a regression worth warning about. The audit declined to judge
        // it - defensible - and then said nothing at all, which is the audit
        // disagreeing with the running system in the one direction that causes
        // no action.
        var h = Build(
            new LeafSpec("a", null, "c", Routed: true, Next: "u"),
            new LeafSpec("u", null, "d", Routed: false, Next: "e"),
            new LeafSpec("e", "e", null, Routed: true, Next: null));

        var page = await InspectAsync(h);

        Assert.That(Reasons(page),
            Does.Contain(OrphanedLeafAuditGapReason.LeafBoundsUndecidable));

        var gap = page.Gaps.Single(
            g => g.Reason == OrphanedLeafAuditGapReason.LeafBoundsUndecidable);
        Assert.Multiple(() =>
        {
            Assert.That(gap.LeafId, Is.EqualTo(h.Id("u").ToString()));
            Assert.That(gap.KeyHint, Is.EqualTo("d"));
        });
    }

    [Test]
    public async Task An_entry_leaf_that_is_itself_unreachable_is_reported_rather_than_passed_over()
    {
        // The walk only ever examines a leaf's SUCCESSOR, because an unsplice
        // swings a live predecessor's pointer and an entry leaf has no
        // predecessor within reach. A resume position landing on a leaf whose
        // declared low bound routes elsewhere is therefore an orphan the pass
        // can see and cannot act on - reported as a gap, not as a finding,
        // because a finding asserts a disposition the pass never reached.
        var h = Build(
            new LeafSpec("a", null, "c", Routed: true, Next: "c"),
            new LeafSpec("c", "b", "e", Routed: true, Next: "e",
                Separator: "c", SeparatorOverridden: true),
            new LeafSpec("e", "e", null, Routed: true, Next: null));

        var page = await InspectAsync(h, resume: "c");

        Assert.Multiple(() =>
        {
            Assert.That(Reasons(page),
                Does.Contain(OrphanedLeafAuditGapReason.EntryLeafUnreachable));
            Assert.That(page.Findings, Is.Empty,
                "an entry leaf has no reachable predecessor, so no disposition was reached");
        });

        var gap = page.Gaps.Single(
            g => g.Reason == OrphanedLeafAuditGapReason.EntryLeafUnreachable);
        Assert.That(gap.LeafId, Is.EqualTo(h.Id("c").ToString()));
    }

    // ========================================================================
    // The page and report contract
    // ========================================================================

    [Test]
    public void An_empty_page_is_a_complete_verdict_and_a_declined_one_is_not()
    {
        var empty = OrphanedLeafRepairPage.Empty;
        var declined = OrphanedLeafRepairPage.Declined(
            3, OrphanedLeafAuditGapReason.ShardSplitInProgress);

        Assert.Multiple(() =>
        {
            Assert.That(empty.Gaps, Is.Empty);
            Assert.That(declined.Gaps, Has.Count.EqualTo(1));
            Assert.That(declined.Gaps[0].ShardIndex, Is.EqualTo(3));
            Assert.That(declined.Gaps[0].Reason,
                Is.EqualTo(OrphanedLeafAuditGapReason.ShardSplitInProgress));
            Assert.That(declined.Findings, Is.Empty);
            Assert.That(declined.ResumeFromInclusive, Is.Null);
        });
    }

    [Test]
    public void A_report_is_only_a_complete_verdict_when_no_shard_left_a_gap()
    {
        var clean = new OrphanedLeafRepairReport
        {
            LeavesWalked = 10,
            Findings = [],
            Gaps = [],
        };

        var gapped = clean with
        {
            Gaps =
            [
                new OrphanedLeafAuditGap
                {
                    ShardIndex = 7,
                    Reason = OrphanedLeafAuditGapReason.ChainTruncatedUnrecoverable,
                },
            ],
        };

        Assert.Multiple(() =>
        {
            Assert.That(clean.VerdictComplete, Is.True);
            Assert.That(gapped.VerdictComplete, Is.False,
                "an empty finding list means nothing when part of the tree was never examined");
        });
    }
}
