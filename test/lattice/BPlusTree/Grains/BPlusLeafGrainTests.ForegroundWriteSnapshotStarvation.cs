using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Deterministic reproduction of the unbounded-WAL-retention defect: a leaf
/// holding foreground-written rows that has never durably checkpointed any WAL
/// partition is refused a snapshot forever, so its block pins never lift and its
/// tree's WAL is never trimmed.
/// <para>
/// The two halves of the machine disagree about what "empty" means.
/// <c>ResolveDurablePinForPartition</c> decides liveness from the projection
/// cache (it takes <c>partitionsWithLiveData</c> as a parameter distinct from
/// the checkpoint, precisely because the two can diverge) and retains a
/// <see cref="HybridLogicalClock.Zero"/> block pin for a partition whose only
/// durable copy is the WAL prefix. The snapshot capture path instead infers
/// emptiness from the checkpoint sentinel alone: its comment reads "the
/// 'nothing applied' sentinel (-1) means the leaf has not yet absorbed any WAL
/// entry into its projection; capturing an empty cache would create a snapshot
/// the activation path is required to ignore, so the work is pure overhead."
/// </para>
/// <para>
/// Those are different propositions. The projection checkpoint tracks WAL
/// <em>replay</em> position, not cache contents, so a leaf whose rows arrived as
/// foreground writes has a populated cache and a checkpoint of <c>-1</c>. Both
/// capture gates short-circuit on it: the advisory gate returns when no
/// partition reports <c>checkpoint &gt;= 0</c>, and the periodic recheck returns
/// because <c>-1 &gt; -1</c> is false. The block pin is therefore set by the
/// half that distinguishes cache liveness from the checkpoint, and can only be
/// released by the half that conflates them.
/// </para>
/// <para>
/// That voids the documented bounded-retention guarantee ("every blocked prefix
/// is covered by a durable snapshot within at most
/// <c>LeafSnapshotReClassifyEveryNCheckpoints</c> checkpoints"): the cadence is
/// denominated in checkpoints, and this leaf never takes one. Retention is not
/// slow, it is unreachable.
/// </para>
/// <para>
/// Half A responded by capturing such a leaf anyway. Issue #2725 then showed
/// that the capture could not have helped and this file's assertions had been
/// reading the wrong signal: a leaf that has never checkpointed can stamp no
/// coverage, <c>LeafSnapshotStorageGrain.HasCapturedPrefix</c> refuses exactly
/// that shape, and so <c>LoadAsync</c> reports the blob absent forever. Half A's
/// comment claimed "the durability of this leaf's rows is earned by writing the
/// blob"; it was not, because nothing would ever read the blob back. Asserting
/// <c>SaveAsync</c> against an NSubstitute stub could not detect that - the stub
/// has no load gate - which is why the round-trip proof now lives in
/// <c>BPlusLeafGrainTests.UnloadableSnapshotBlob.cs</c>, driven through the real
/// storage grain.
/// </para>
/// <para>
/// So the contract this file pins is now: such a leaf is DECLINED, under its own
/// reason <c>no_coverage_claim</c>, distinct from the <c>not_eligible</c> a
/// genuinely empty leaf earns. That distinction is what Half A was actually
/// reaching for and is the part worth keeping - "this leaf holds rows it cannot
/// yet claim coverage for" becomes a counted, legible population instead of a
/// silent write nothing can read. Its rows remain fully recoverable by WAL
/// replay, which is why declining is safe.
/// </para>
/// <para>
/// The pin assertions below are unchanged and remain the most load-bearing in
/// the file. The two retention planes are coupled by a documented handoff -
/// <c>ComputeMaterialiserOffsetFloorAsync</c> skips a <c>-1</c> pin precisely
/// because "WAL retention is already enforced by the HLC block-pin branch" - so
/// lifting the block on an un-replayed partition would drop both protections at
/// once. On an existing deployment that would fire across every previously
/// starved leaf simultaneously and trim a large prefix no consumer has read.
/// </para>
/// <para>
/// The empty-cache control must stay a no-op, and must keep declining for the
/// OTHER reason, so that the two declines cannot be confused for one another.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ForegroundStarvationTreeId = "tree-foreground-write-starvation";

    private static (BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        Func<IReadOnlyList<MaterialiserPinReport>?> LastFlush,
        ILeafSnapshotStorageGrain SnapshotStub)
        CreateNeverCheckpointedLeaf(int walPartitions, bool bornAtDefault = false, string? treeId = null)
    {
        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId ?? ForegroundStarvationTreeId;

        // The leaf under test has never absorbed a WAL entry on ANY partition:
        // the scalar slot and every per-partition slot sit at the "nothing
        // applied" sentinel. This is the state of a leaf whose materialiser is
        // lagging behind sustained foreground write load.
        //
        // Two ways to be never-checkpointed, and they are NOT interchangeable
        // (issue #2703). An explicit -1 is what the operator-driven projection
        // rebuild writes, and it is the only writer of a negative partition-0
        // scalar anywhere in src/lattice. Production reaches the same logical
        // state by a different route: the scalar is simply never assigned, so it
        // holds the CLR default and the serializer omits it. bornAtDefault
        // selects that second, far more common shape.
        if (bornAtDefault)
        {
            state.State.ProjectionCheckpointOffset = 0L;
            state.State.ProjectionCheckpointOffsetAssigned = null;
        }
        else
        {
            state.State.ProjectionCheckpointOffset = -1L;
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = walPartitions,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                LeafSnapshotReClassifyEveryNCheckpoints = 1,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (grain, state, () => captured, snapshotStub);
    }

    [Test]
    public async Task Foreground_written_leaf_with_no_checkpoint_is_never_snapshotted_so_its_wal_pin_is_permanent()
    {
        const int partitions = 8;
        var (grain, state, lastFlush, snapshotStub) = CreateNeverCheckpointedLeaf(partitions);
        var projection = AsProjection(grain);
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // A foreground write: the row lands in the projection cache and in the
        // WAL, and the leaf clock advances past Zero. It does NOT advance any
        // projection checkpoint, because the checkpoint tracks WAL replay
        // position and this row was never replayed - it was written.
        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: ForegroundStarvationTreeId));

        Assert.That(state.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero),
            "precondition: a foreground write was applied, so the cache is populated");
        for (var p = 0; p < partitions; p++)
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(p), Is.EqualTo(-1L),
                $"precondition: partition {p} has never durably checkpointed");
        }

        // The cadence capture is the ONLY documented path from a block pin to
        // coverage and trim. Run it repeatedly: the defect claim is that
        // retention is unreachable, not merely slow, so no number of captures
        // may change the outcome.
        for (var i = 0; i < 5; i++)
        {
            await grain.CaptureSnapshotAsync();
        }

        // ISSUE #2725. The capture is DECLINED, and that is the correct
        // outcome rather than a regression of Half A. A leaf with no
        // checkpoint on any partition can stamp no coverage, and
        // LeafSnapshotStorageGrain.HasCapturedPrefix refuses precisely that
        // shape, so a blob written here could never be read back - it would be
        // storage spent on something LoadAsync reports as absent, and which
        // ClearAsync (also gated on HasCapturedPrefix) would then refuse to
        // reclaim. The rows are not orphaned by declining: they remain in the
        // WAL, and WAL replay is what recovers this leaf, exactly as the pin
        // assertions below require.
        await snapshotStub.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(-1L),
            "a never-checkpointed partition must claim no offset, declined or not: coverage states what the "
            + "materialiser has consumed, and it has consumed nothing");

        // The guard that matters most. The two retention planes are coupled by
        // a documented handoff: ComputeMaterialiserOffsetFloorAsync SKIPS a -1
        // pin *because* the HLC block-pin branch is enforcing retention. So
        // advancing this pin while the offset is still -1 would remove BOTH
        // protections at once and authorise trimming a prefix the materialiser
        // has never replayed - silent loss of committed data, and on an upgrade
        // it would fire across every previously-starved leaf simultaneously.
        // This assertion exists to stop a future contributor "completing" the
        // fix by lifting the block here.
        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        var reports = lastFlush();
        Assert.That(reports, Is.Not.Null, "the deactivation hook must flush a durable frontier");
        Assert.That(reports![dataPartition].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            $"partition {dataPartition} holds rows whose only durable WAL claim is un-replayed, so its block "
            + "pin MUST be retained even after a successful capture; lifting it drops both retention planes");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(-1L));
    }

    [Test]
    public async Task Genuinely_empty_never_checkpointed_leaf_is_still_not_snapshotted()
    {
        // Narrowness control. A leaf with an EMPTY cache and no checkpoint has
        // nothing to snapshot, and capturing it would write a blob the
        // activation path is required to ignore. This must stay a no-op after
        // the fix, which is why the fix has to key on cache liveness rather
        // than simply removing the sentinel gate.
        const int partitions = 8;
        var (grain, _, _, snapshotStub) = CreateNeverCheckpointedLeaf(partitions);

        await grain.CaptureSnapshotAsync();

        await snapshotStub.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "an empty, never-checkpointed leaf has nothing to cover and must not write a snapshot");
    }

    /// <summary>
    /// ACCEPTANCE CRITERION 7. The two tests above reach the widened branch by
    /// seeding an explicit <c>-1</c>, which only the operator-driven projection
    /// rebuild ever writes. Acceptance criterion 8 is explicit that their green
    /// therefore does NOT evidence the production path, and this pair supplies
    /// what it asks for: the same branch driven by a leaf built the way
    /// production builds one, with partition 0 left at the CLR default and no
    /// presence marker.
    /// <para>
    /// This arm is the live-rows half. Since issue #2725 both arms of the pair
    /// DECLINE, so "did it save?" no longer discriminates between them and the
    /// discriminating signal is the decline REASON. That is a strictly stronger
    /// acceptance criterion than the old one, not a weaker one: the reason is
    /// emitted from inside the widened branch and nowhere else, and the two arms
    /// emit DIFFERENT reasons, so a single assertion now witnesses both that the
    /// branch executed and which side of its live-data test the leaf fell on.
    /// The previous formulation could not do that - it asserted a
    /// <c>SaveAsync</c> against a stub, which a pre-#2692 leaf reached too, just
    /// by the other route.
    /// </para>
    /// <para>
    /// The coverage-stamp assertions are retained unchanged. They remain the
    /// data-loss guard for the birth-zero ambiguity of issue #2703: reading the
    /// ambiguous <c>0</c> as real progress would make
    /// <c>anyPartitionCheckpointed</c> true, skip this branch entirely, and run
    /// <c>BuildCheckpointCoverage</c>, which stamps <c>offsets[0]</c> straight
    /// from the birth scalar and publishes an offset-0 trim entitlement for a
    /// partition that has consumed nothing.
    /// </para>
    /// </summary>
    [Test]
    public async Task Born_at_default_leaf_with_live_rows_is_declined_as_no_coverage_claim()
    {
        const int partitions = 8;
        var treeId = UniqueSnapshotCaptureTree();
        var (grain, state, _, snapshotStub) = CreateNeverCheckpointedLeaf(
            partitions, bornAtDefault: true, treeId: treeId);
        var projection = AsProjection(grain);
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.Zero,
                "precondition: the PRODUCTION birth shape, not the admin rebuild's explicit -1");
            Assert.That(state.State.ProjectionCheckpointOffsetAssigned, Is.Null,
                "precondition: no presence marker was ever written");
        });
        for (var p = 0; p < partitions; p++)
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(p), Is.EqualTo(-1L),
                $"precondition: partition {p} reports nothing applied, partition 0 included");
        }

        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: treeId));

        var reasons = CaptureSnapshotDeclineObservations(treeId, out var listener);
        using (listener)
        {
            await grain.CaptureSnapshotAsync();
        }

        await snapshotStub.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(reasons, Is.EquivalentTo(new[] { "no_coverage_claim" }),
                "THE acceptance assertion (issue #2725). no_coverage_claim is emitted from inside the "
                + "widened branch and from nowhere else, and only on its live-data side, so this single "
                + "reading witnesses that a leaf built the PRODUCTION way entered that branch and was "
                + "found to hold rows. not_eligible here would mean the live-data test misread a "
                + "populated cache as empty; an empty bag would mean the branch never ran at all");
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
                "THE data-loss assertion. Partition 0 never applied anything, so no offset floor may "
                + "be published for it. Reading its birth value as a real checkpoint stamps coverage 0, "
                + "which turns min(checkpoint, covered) from -1 into 0 and converts a correct block pin "
                + "into an unearned trim entitlement - silently, on upgrade, across every such leaf");
            Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(-1L),
                "and the partition holding the rows claims nothing either");
        });
    }

    /// <summary>
    /// ACCEPTANCE CRITERION 7, the other arm.
    /// <para>
    /// Pre-fix a birth leaf reported <c>0</c>, so <c>anyPartitionCheckpointed</c>
    /// was true on the first iteration, the widening was skipped entirely, and
    /// an EMPTY leaf went on to be captured as though it held a real checkpoint -
    /// writing a blob the activation path is then required to ignore. Post-fix
    /// partition 0 reports the sentinel, no partition is proven, control enters
    /// the widened branch, it finds no live data, and it returns without
    /// capturing.
    /// </para>
    /// <para>
    /// Paired with the arm above, the two bracket the branch from both sides
    /// and - since issue #2725 turned the live-rows arm into a decline too - do
    /// so by the reason they emit rather than by whether a save happened. Both
    /// decline; they must decline DIFFERENTLY. <c>not_eligible</c> here and
    /// <c>no_coverage_claim</c> there is what keeps "this leaf has nothing"
    /// distinguishable from "this leaf has rows it cannot claim coverage for",
    /// which is the whole operational value of the reason tag. If either arm
    /// ever emitted the other's reason the branch's live-data test would have
    /// inverted, and no assertion on <c>SaveAsync</c> would notice.
    /// </para>
    /// </summary>
    [Test]
    public async Task Born_at_default_leaf_with_an_empty_cache_is_declined_as_not_eligible()
    {
        const int partitions = 8;
        var treeId = UniqueSnapshotCaptureTree();
        var (grain, state, _, snapshotStub) = CreateNeverCheckpointedLeaf(
            partitions, bornAtDefault: true, treeId: treeId);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.Zero,
                "precondition: the PRODUCTION birth shape, not the admin rebuild's explicit -1");
            Assert.That(state.State.ProjectionCheckpointOffsetAssigned, Is.Null,
                "precondition: no presence marker was ever written");
        });
        Assert.That(grain.EntriesForTest, Is.Empty,
            "precondition: the cache is genuinely empty, so the widened branch has a reason to decline");

        var reasons = CaptureSnapshotDeclineObservations(treeId, out var listener);
        using (listener)
        {
            await grain.CaptureSnapshotAsync();
        }

        await snapshotStub.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(reasons, Is.EquivalentTo(new[] { "not_eligible" }),
                "an empty leaf must decline as not_eligible, NOT as no_coverage_claim. The two reasons "
                + "mean different things to an operator - nothing to store, versus rows whose only "
                + "durable copy is the WAL - and collapsing them would retire the starved-leaf signal");
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
                "a birth leaf with nothing in it must be declined by the widened branch. Reading its "
                + "partition 0 as checkpointed skips that branch altogether and captures an empty leaf, "
                + "which is how the gate was proven unreachable in production");
        });
    }
}
