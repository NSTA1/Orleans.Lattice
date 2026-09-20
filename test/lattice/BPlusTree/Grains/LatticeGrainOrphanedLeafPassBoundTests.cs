using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers the <b>tree-level driver</b> of the orphaned-leaf pass - the fan-out
/// in <c>LatticeGrain.OrphanRepair.cs</c> that pumps each shard's bounded
/// batches - against the defect in issue 3302.
/// <para>
/// The per-shard batch was already work-bounded and already returned a resume
/// key, which is why the issue's framing of the verb as "not chunked or
/// resumable" is wrong one level down. What was unbounded was this driver: it
/// walked every shard and every batch to completion inside a single
/// client-facing grain call. On a tree with 236 repairable orphans that call
/// outran the Orleans response deadline, so the operator was handed a
/// <c>TimeoutException</c> while the grain went on to complete all 236 repairs -
/// an operation reported as failed having entirely succeeded, with the report of
/// what it did discarded along with the exception. A subsequent audit is the
/// only thing that revealed the work had landed.
/// </para>
/// <para>
/// <b>Why a wall-clock bound rather than a leaf cap.</b> The two trees measured
/// in the field separate exactly one way. The one that returned walked 2443
/// leaves; the one that timed out walked <em>fewer</em> - 2121 - and took longer,
/// because cost is carried by per-key verification (19,638 keys across 236
/// repaired leaves, each a grain call) rather than by leaves traversed. A leaf
/// cap set anywhere that admitted the first tree would have admitted the second,
/// so it would not have fixed the reported defect at all.
/// </para>
/// <para>
/// The fixture drives that bound directly by setting
/// <see cref="LatticeOptions.BackgroundDrainMaxDuration"/>: one tick yields after
/// the first batch that can name a resume position, and a long span drives the
/// pass to completion. That keeps every assertion deterministic without any
/// dependence on how fast the machine happens to be.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeGrainOrphanedLeafPassBoundTests
{
    private const string TreeId = "orphan-pass-bound-tree";

    /// <summary>Yields at the first opportunity: the deadline is already past.</summary>
    private static LatticeOptions YieldImmediately() =>
        new() { BackgroundDrainMaxDuration = TimeSpan.FromTicks(1) };

    /// <summary>Never yields on the clock, so the pass runs to completion.</summary>
    private static LatticeOptions NeverYield() =>
        new() { BackgroundDrainMaxDuration = TimeSpan.FromHours(1) };

    /// <summary>
    /// Builds a grain over <paramref name="shardCount"/> distinct substituted
    /// shard roots, each addressed by the key the driver composes, so a test can
    /// assert which shards were visited and in what order.
    /// </summary>
    private static (LatticeGrain Grain, IReadOnlyList<IShardRootGrain> Shards) CreateGrain(
        LatticeOptions options,
        int shardCount = 1,
        int[]? slots = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>())
            .Returns(Task.FromResult<ShardMap?>(new ShardMap
            {
                Slots = slots ?? Enumerable.Range(0, shardCount).ToArray(),
                Version = 1,
            }));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = shardCount }));

        var shards = new IShardRootGrain[shardCount];
        for (var i = 0; i < shardCount; i++)
        {
            shards[i] = Substitute.For<IShardRootGrain>();
            var key = $"{TreeId}/{i}";
            grainFactory.GetGrain<IShardRootGrain>(key, Arg.Any<string>()).Returns(shards[i]);
        }

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);
        var grain = new LatticeGrain(
            context,
            grainFactory,
            optionsMonitor,
            optionsResolver,
            Substitute.For<IServiceProvider>(),
            NullLogger<LatticeGrain>.Instance);

        return (grain, shards);
    }

    /// <summary>
    /// Arranges a shard whose chain takes <paramref name="batches"/> batches to
    /// exhaust, recording the cursor each batch was issued with.
    /// </summary>
    private static List<string?> ArrangeChain(IShardRootGrain shard, int batches, int leavesPerBatch = 4)
    {
        var issued = new List<string?>();
        var served = 0;
        shard.RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                issued.Add(ci.Arg<string?>());
                served++;
                return new OrphanedLeafRepairPage
                {
                    LeavesWalked = leavesPerBatch,
                    ResumeFromInclusive = served < batches ? $"k{served}" : null,
                };
            });
        return issued;
    }

    // ------------------------------------------------------- the bound itself

    /// <summary>
    /// <b>The regression test for issue 3302.</b> A shard whose chain needs many
    /// batches must not be walked to the end inside one call once the budget is
    /// spent: the call returns a resume position instead. On the unfixed driver
    /// the loop ran to exhaustion regardless of the clock, which is precisely
    /// what outran the response deadline in the field.
    /// </summary>
    [Test]
    public async Task A_spent_budget_yields_a_resume_position_instead_of_draining_the_shard()
    {
        var (grain, shards) = CreateGrain(YieldImmediately());
        ArrangeChain(shards[0], batches: 50);

        var report = await grain.RepairOrphanedLeavesAsync(cancellationToken: CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                report.IsComplete,
                Is.False,
                "a batch that stopped on its budget must not claim the tree was examined end to end");
            Assert.That(report.ResumeFrom, Is.Not.Null, "a partial batch must name where to resume");
        });

        await shards[0].Received(1).RepairOrphanedLeavesAsync(
            Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// The negative control for the test above: with budget to spare the driver
    /// must still finish the job, so the bound cannot be "passing" by truncating
    /// every pass. This is also the arm that proves a complete pass reports
    /// itself complete.
    /// </summary>
    [Test]
    public async Task An_unspent_budget_drives_every_batch_and_reports_the_pass_complete()
    {
        var (grain, shards) = CreateGrain(NeverYield());
        var issued = ArrangeChain(shards[0], batches: 3);

        var report = await grain.RepairOrphanedLeavesAsync(cancellationToken: CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.ResumeFrom, Is.Null);
            Assert.That(report.IsComplete, Is.True);
            Assert.That(report.LeavesWalked, Is.EqualTo(12), "every batch's count must be accumulated");
            Assert.That(
                issued,
                Is.EqualTo(new string?[] { null, "k1", "k2" }),
                "each batch must be re-issued with the cursor the previous batch returned");
        });
    }

    /// <summary>
    /// Resuming must continue where the previous batch stopped, both in which
    /// shard and where inside it. A driver that took the token and started over
    /// would re-walk work already done and report the re-walk as the remainder.
    /// </summary>
    [Test]
    public async Task Resuming_continues_at_the_named_shard_and_key()
    {
        var (grain, shards) = CreateGrain(NeverYield(), shardCount: 3);
        var issued0 = ArrangeChain(shards[0], batches: 1);
        var issued1 = ArrangeChain(shards[1], batches: 1);
        var issued2 = ArrangeChain(shards[2], batches: 1);

        await grain.RepairOrphanedLeavesAsync(OrphanedLeafPassCursor.Encode(1, "mid-chain"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(issued0, Is.Empty, "a shard below the cursor was finished by an earlier batch");
            Assert.That(issued1, Is.EqualTo(new string?[] { "mid-chain" }),
                "the named shard must resume at the named key, not at its chain head");
            Assert.That(issued2, Is.EqualTo(new string?[] { null }),
                "shards above the cursor start at their chain heads");
        });
    }

    /// <summary>
    /// A pass driven batch by batch to completion must see every shard exactly
    /// once and reach the same end state as one unbounded call. This is the
    /// property that makes the bound a chunking of the work rather than a
    /// reduction of it.
    /// </summary>
    [Test]
    public async Task Driving_the_pass_to_completion_covers_every_shard_exactly_once()
    {
        var (grain, shards) = CreateGrain(YieldImmediately(), shardCount: 4);
        var issued = new List<string?>[4];
        for (var i = 0; i < 4; i++) issued[i] = ArrangeChain(shards[i], batches: 2);

        var calls = 0;
        var walked = 0;
        string? cursor = null;
        do
        {
            var report = await grain.RepairOrphanedLeavesAsync(cursor, CancellationToken.None);
            walked += report.LeavesWalked;
            cursor = report.ResumeFrom;
            calls++;
            Assert.That(calls, Is.LessThan(50), "the pass must converge, not loop");
        }
        while (cursor is not null);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.GreaterThan(1), "a yielding budget must actually have split the pass");
            Assert.That(walked, Is.EqualTo(4 * 2 * 4), "driving to completion must walk the whole tree");
            for (var i = 0; i < 4; i++)
            {
                Assert.That(issued[i], Is.EqualTo(new string?[] { null, "k1" }),
                    $"shard {i} must be walked exactly once, from its head, across the whole pass");
            }
        });
    }

    /// <summary>
    /// The last shard finishing must end the pass, not hand back a position past
    /// the end. Encoding one would cost the operator a round trip to be told
    /// there was nothing left and - worse - would report a tree that had been
    /// examined end to end as incomplete, so an empty finding list could never be
    /// read as the clean verdict it is.
    /// </summary>
    [Test]
    public async Task The_final_shard_completing_ends_the_pass_rather_than_yielding_past_the_end()
    {
        var (grain, shards) = CreateGrain(YieldImmediately(), shardCount: 2);
        ArrangeChain(shards[0], batches: 1);
        ArrangeChain(shards[1], batches: 1);

        var first = await grain.RepairOrphanedLeavesAsync(cancellationToken: CancellationToken.None);
        Assert.That(first.ResumeFrom, Is.Not.Null, "an exhausted budget with a shard left must yield");

        var second = await grain.RepairOrphanedLeavesAsync(first.ResumeFrom, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.ResumeFrom, Is.Null);
            Assert.That(second.IsComplete, Is.True);
        });
    }

    /// <summary>
    /// The cursor names a shard by <em>index</em>, so "skip everything below it"
    /// is only the same claim as "skip what is already done" when the walk visits
    /// indices in ascending order. <see cref="ShardMap.GetPhysicalShardIndices"/>
    /// returns them sorted on its bitmap fast path but not on the set-based
    /// fallback a hand-built or wire-corrupt map takes, and a pass that skipped a
    /// shard would report a clean tree that still had an orphan holding its WAL
    /// floor down.
    /// </summary>
    [Test]
    public async Task Shards_are_visited_in_ascending_index_order()
    {
        var (grain, shards) = CreateGrain(NeverYield(), shardCount: 3, slots: [2, 0, 1, 2, 0, 1]);
        var order = new List<int>();
        for (var i = 0; i < 3; i++)
        {
            var index = i;
            shards[i].RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
                .Returns(_ =>
                {
                    order.Add(index);
                    return new OrphanedLeafRepairPage { LeavesWalked = 1, ResumeFromInclusive = null };
                });
        }

        await grain.RepairOrphanedLeavesAsync(cancellationToken: CancellationToken.None);

        Assert.That(order, Is.EqualTo(new[] { 0, 1, 2 }),
            "resuming by shard index is only sound if the walk visits indices in ascending order");
    }

    // --------------------------------------------------------- report shaping

    /// <summary>
    /// The inspection verb must be recognisable as a dry run at every point in a
    /// resumed pass, and must be the verb that reaches the shard in dry-run mode.
    /// An operator whose second batch silently mutated would have no way to tell.
    /// </summary>
    [Test]
    public async Task The_inspection_verb_stays_a_dry_run_across_a_resumed_pass()
    {
        var (grain, shards) = CreateGrain(YieldImmediately(), shardCount: 2);
        ArrangeChain(shards[0], batches: 1);
        ArrangeChain(shards[1], batches: 1);

        var first = await grain.InspectOrphanedLeavesAsync(cancellationToken: CancellationToken.None);
        var second = await grain.InspectOrphanedLeavesAsync(first.ResumeFrom, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.DryRun, Is.True);
            Assert.That(second.DryRun, Is.True);
        });

        await shards[0].Received().RepairOrphanedLeavesAsync(Arg.Any<string?>(), true, Arg.Any<CancellationToken>());
        await shards[1].Received().RepairOrphanedLeavesAsync(Arg.Any<string?>(), true, Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Findings are per batch, so a partial batch's findings must be exactly the
    /// ones it actually saw. Reporting a superset would misattribute another
    /// batch's work; a subset would lose a leaf the operator needs to see.
    /// </summary>
    [Test]
    public async Task A_partial_batch_reports_only_the_findings_it_reached()
    {
        var (grain, shards) = CreateGrain(YieldImmediately(), shardCount: 2);
        shards[0].RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new OrphanedLeafRepairPage
            {
                LeavesWalked = 2,
                ResumeFromInclusive = null,
                Findings = [new OrphanedLeafFinding { ShardIndex = 0, LeafId = "leaf-a" }],
            });
        shards[1].RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new OrphanedLeafRepairPage
            {
                LeavesWalked = 2,
                ResumeFromInclusive = null,
                Findings = [new OrphanedLeafFinding { ShardIndex = 1, LeafId = "leaf-b" }],
            });

        var first = await grain.RepairOrphanedLeavesAsync(cancellationToken: CancellationToken.None);
        var second = await grain.RepairOrphanedLeavesAsync(first.ResumeFrom, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Findings.Select(f => f.LeafId), Is.EqualTo(new[] { "leaf-a" }));
            Assert.That(second.Findings.Select(f => f.LeafId), Is.EqualTo(new[] { "leaf-b" }));
        });
    }

    // ------------------------------------------------------- token validation

    /// <summary>
    /// A token this surface did not produce is rejected before any authorization
    /// round trip or shard call, so a mistyped resume never silently restarts the
    /// pass and never reaches a shard.
    /// </summary>
    [Test]
    public void A_foreign_resume_token_is_rejected_without_touching_a_shard()
    {
        var (grain, shards) = CreateGrain(NeverYield());

        Assert.That(
            async () => await grain.RepairOrphanedLeavesAsync("not-a-token", CancellationToken.None),
            Throws.ArgumentException);

        shards[0].DidNotReceive().RepairOrphanedLeavesAsync(
            Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Re-running a completed pass from the start must be safe - the idempotence
    /// the tool description promises an operator who saw a timeout. Every leaf is
    /// re-examined and the report is reached again on the same evidence; nothing
    /// in the driver carries state from the previous pass that a second one could
    /// double-count.
    /// </summary>
    [Test]
    public async Task Re_running_a_completed_pass_reaches_the_same_report()
    {
        var (grain, shards) = CreateGrain(NeverYield(), shardCount: 2);
        for (var i = 0; i < 2; i++)
        {
            shards[i].RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
                .Returns(new OrphanedLeafRepairPage { LeavesWalked = 3, ResumeFromInclusive = null });
        }

        var first = await grain.RepairOrphanedLeavesAsync(cancellationToken: CancellationToken.None);
        var again = await grain.RepairOrphanedLeavesAsync(cancellationToken: CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(again.LeavesWalked, Is.EqualTo(first.LeavesWalked));
            Assert.That(again.IsComplete, Is.True);
            Assert.That(again.Findings, Has.Count.EqualTo(first.Findings!.Count));
        });
    }
}
