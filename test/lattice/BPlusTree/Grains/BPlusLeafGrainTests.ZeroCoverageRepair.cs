using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// "Half B" of the unbounded-WAL-retention defect: the repair path that takes a
/// leaf OUT of the zero-coverage state, so retention is bounded rather than
/// merely reachable.
/// <para>
/// Half A made a never-checkpointed leaf capturable. It deliberately stopped
/// short of making retention bounded, because writing a blob earns durability,
/// not authority to trim. The residue Half A left is the population this file
/// covers: a leaf that HAS checkpointed a partition (so it does make an honest
/// offset claim) but holds no durable snapshot covering it. For that leaf
/// <c>ResolveDurablePinForPartition</c> computes
/// <c>Math.Min(checkpoint, covered)</c>, sees <c>covered == -1</c>, and returns
/// <c>(HybridLogicalClock.Zero, -1L)</c> - a block pin on the HLC plane AND the
/// sentinel that makes <c>ComputeMaterialiserOffsetFloorAsync</c> abstain on the
/// offset plane. One such leaf disables cursor trim for its ENTIRE tree.
/// </para>
/// <para>
/// The measured predicate on the affected deployment was mechanical, not
/// correlational: a tree trims WAL iff zero of its leaves lack a partition
/// checkpoint coverage stamp. Ten trees, no exceptions - <c>structural</c>
/// (unsplit, non-vector, 12.9 MB, quiescent, eight consecutive <c>idle</c> GC
/// passes) starved, while <c>xref</c> - same family, same leaf count, comparable
/// WAL - reclaimed 96% of its bytes under the identical scheduler.
/// </para>
/// <para>
/// Why the pre-existing drivers do not close this. There are exactly four
/// snapshot-capture drivers, and every one of them misses this population:
/// <list type="number">
/// <item>The activation advisory is gated on <c>_activationSnapshotPending</c>,
/// a fall-off-log PROXIMITY heuristic about the WAL tail. Under the
/// coverage-gated pin the tail stays low precisely BECAUSE the block is held, so
/// the advisory does not fire - the symptom suppresses its own remedy.</item>
/// <item>The deactivation hook only runs on graceful deactivation, which a
/// permanently-resident leaf never reaches.</item>
/// <item>The coverage-deficit escape requires
/// <c>_snapshotCoverageDeficitAtActivation</c>, latched only when rehydrating a
/// STALE snapshot - unreachable for a leaf that never had one.</item>
/// <item>The cadence recheck carries its own debounce,
/// <c>checkpoint &gt; coverage</c>, and its counter RESETS every activation. It
/// is also disabled outright by a tuning value of 0.</item>
/// </list>
/// So the repair has to be its own driver, and it has to sit ABOVE the cadence
/// gate - the same placement, for the same reason, that the pre-existing
/// coverage-deficit escape already occupies.
/// </para>
/// <para>
/// The acceptance bar these tests encode is deliberately stronger than "WAL
/// bytes fell on the rig". Every leaf reactivates on deploy, so a fix
/// guaranteeing nothing would be vindicated by exactly the measurement one would
/// naturally take, and a single newborn leaf would then re-strand the tree. The
/// load-bearing assertion is therefore
/// <see cref="Newly_checkpointed_leaf_gains_coverage_on_its_first_persist_without_deactivating"/>:
/// a leaf reaches coverage WITHOUT deactivating and WITHOUT reaching the
/// cadence.
/// </para>
/// <para>
/// What this fix must NOT do is equally load-bearing, and
/// <see cref="Repair_does_not_stamp_coverage_for_a_partition_that_never_checkpointed"/>
/// pins it. The offset plane does not merely ignore a <c>-1</c>; it ABSTAINS on
/// the stated assumption that the HLC block pin is enforcing retention. A
/// <c>-1</c> is either a never-checkpointed partition or a split sibling that
/// received its rows by in-memory handoff rather than WAL replay, and in both
/// cases it makes no honest offset claim. Stamping one would drop BOTH
/// protections at once and authorise trimming a prefix no consumer has read -
/// silent loss of committed data, firing across every previously-starved leaf
/// simultaneously on upgrade. The repair therefore captures; it never advances a
/// pin and never lifts a block.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string CoverageRepairTreeId = "tree-zero-coverage-repair";

    /// <summary>
    /// Builds a leaf in, or adjacent to, the zero-coverage state.
    /// </summary>
    /// <param name="persistedCheckpoint">
    /// The partition-0 checkpoint the leaf comes online holding. A value
    /// <c>&gt;= 0</c> is what makes the leaf's claim honest and therefore
    /// repairable; <c>-1</c> is Half A's population and must stay unstamped.
    /// </param>
    /// <param name="reClassifyEveryN">
    /// The cadence option. These tests pin it WELL ABOVE the number of persists
    /// they drive (or at 0, disabled) precisely so that any capture observed is
    /// attributable to the repair path and not to the cadence.
    /// </param>
    /// <param name="existingCoverage">
    /// Per-partition coverage to seed via a loaded snapshot blob, or
    /// <see langword="null"/> for the starved case (no blob at all).
    /// </param>
    /// <param name="saveFailure">
    /// When supplied, every snapshot save throws it - used to prove the repair
    /// budget is bounded and that exhaustion becomes a visible state rather than
    /// silence.
    /// </param>
    private static (BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        ILeafSnapshotStorageGrain SnapshotStub,
        List<LeafSnapshotBlob> Saved)
        CreateLeafForCoverageRepair(
            long persistedCheckpoint = 0L,
            int reClassifyEveryN = 1000,
            long[]? existingCoverage = null,
            Exception? saveFailure = null,
            int walPartitions = 1)
    {
        var saved = new List<LeafSnapshotBlob>();

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();

        LeafSnapshotBlob? existing = null;
        if (existingCoverage is not null)
        {
            existing = new LeafSnapshotBlob
            {
                SnapshotOffset = existingCoverage[0],
                Rows = new List<LeafSnapshotRow>(),
                CapturedAtTicks = DateTime.UtcNow.Ticks,
                SnapshotBytes = 0L,
                SnapshotOffsetsByPartition = existingCoverage,
            };
        }

        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(existing));

        if (saveFailure is null)
        {
            snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    saved.Add(ci.Arg<LeafSnapshotBlob>());
                    return Task.CompletedTask;
                });
        }
        else
        {
            snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                .Throws(_ =>
                {
                    saved.Add(new LeafSnapshotBlob { Rows = new List<LeafSnapshotRow>() });
                    return saveFailure;
                });
        }

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(persistedCheckpoint < 0 ? 0L : persistedCheckpoint));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        // TailReplay throughout: the fall-off-log advisory must NOT be the thing
        // that drives any capture observed here. That is the whole point - the
        // advisory is the driver that provably does not fire for this
        // population, so leaving it off isolates the repair path.
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.TailReplay));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(detector);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = CoverageRepairTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = walPartitions,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                LeafSnapshotReClassifyEveryNCheckpoints = reClassifyEveryN,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state, snapshotStub, saved);
    }

    private static void SeedRow(BPlusLeafGrain grain, string key = "k")
        => grain.EntriesForTest[key] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Zero,
        };

    /// <summary>
    /// THE load-bearing test. A leaf that takes its first checkpoint during an
    /// activation it never leaves must reach coverage on that persist.
    /// <para>
    /// The bar is deliberately "without deactivating" and "without reaching the
    /// cadence". A repair that only ran at activation would be vindicated by the
    /// obvious rig measurement - every leaf reactivates on deploy, coverage is
    /// stamped, bytes fall - while guaranteeing nothing, because the next
    /// newborn leaf re-strands the whole tree. The cadence here is 1000 against a
    /// single persist, so a pass cannot be explained by the cadence firing.
    /// </para>
    /// <para>
    /// RED pre-fix: the cadence debounce is <c>checkpoint &gt; coverage</c>,
    /// which is satisfied, but the cadence COUNTER is at 1 of 1000, so
    /// <c>MaybeRunPeriodicSnapshotRecheckAsync</c> returns before reaching
    /// capture and coverage stays at the sentinel.
    /// </para>
    /// </summary>
    [Test]
    public async Task Newly_checkpointed_leaf_gains_coverage_on_its_first_persist_without_deactivating()
    {
        // persistedCheckpoint -1: the leaf is born having absorbed nothing, so
        // it is NOT repairable at activation. It becomes repairable only by
        // taking its first checkpoint, mid-activation - the newborn/split-sibling
        // shape that a once-per-activation repair would miss entirely.
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, reClassifyEveryN: 1000);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        SeedRow(grain);

        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "precondition: the newborn leaf holds no coverage");
        Assert.That(saved, Is.Empty,
            "precondition: activation alone captured nothing (the advisory is TailReplay)");

        // ONE checkpoint persist. Not 1000, not a deactivation.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);

        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(7L),
            "the leaf now makes an honest offset claim on partition 0");
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(0L),
            "the repair must stamp real coverage on the first persist that creates the deficit - "
            + "waiting for the cadence or for a deactivation is what stranded the deployment");
        Assert.That(saved, Has.Count.EqualTo(1),
            "exactly one capture: the repair is self-extinguishing, not a per-persist capture");
    }

    /// <summary>
    /// The repair sits ABOVE the cadence gate, so a tuning value of <c>0</c>
    /// cannot disable it.
    /// <para>
    /// This is a deliberate, precedented spec change rather than an oversight.
    /// The pre-existing coverage-deficit escape in the same method already sits
    /// above the same gate for the identical reason, recorded there verbatim:
    /// with the cadence at 0 a starved leaf could NEVER escape, "so its WAL pin
    /// would never lift and its WAL would grow without bound - a disk-exhaustion
    /// failure mode reachable by setting a cadence value". A tuning knob that
    /// trades snapshot frequency for CPU must not also be able to trade away
    /// bounded disk.
    /// </para>
    /// <para>
    /// The cadence itself remains genuinely disabled at 0; that contract is
    /// still pinned by
    /// <c>Periodic_recheck_disabled_when_threshold_is_zero</c>, which was
    /// narrowed to a leaf that already holds coverage - the domain where it is
    /// still true - rather than edited to accept the new behaviour.
    /// </para>
    /// <para>RED pre-fix: <c>threshold &lt;= 0</c> returns immediately.</para>
    /// </summary>
    [Test]
    public async Task Repair_runs_even_when_the_periodic_cadence_is_disabled()
    {
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, reClassifyEveryN: 0);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        SeedRow(grain);

        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(3, CancellationToken.None);

        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(0L),
            "a cadence of 0 disables the periodic recheck; it must not also disable bounded WAL retention");
        Assert.That(saved, Has.Count.EqualTo(1));
    }

    /// <summary>
    /// The "went quiet" case: a tree that was hot, accumulated a large WAL, then
    /// stopped taking writes and never deactivates.
    /// <para>
    /// This is the population the persist-driven trigger provably cannot reach -
    /// no writes means no persists means no trigger - and it is not a
    /// hypothetical: it is the steady state of a corpus that finishes ingesting,
    /// which is exactly what the affected deployment does. Left uncovered it
    /// strands the tree's whole accumulated WAL permanently, not a bounded
    /// residue.
    /// </para>
    /// <para>
    /// The activation-side repair closes it, and the two triggers are jointly
    /// exhaustive: a leaf either ENTERS an activation already uncovered (caught
    /// here) or BECOMES uncovered during one by persisting a checkpoint (caught
    /// by the test above). There is no third way to occupy the state. Ordering is
    /// load-bearing and verified in source: step 0 rehydrates the snapshot before
    /// step 1.5 evaluates the predicate, so coverage is populated when it runs.
    /// </para>
    /// <para>
    /// RED pre-fix: the activation advisory is TailReplay so nothing captures,
    /// and with zero subsequent persists no other driver exists.
    /// </para>
    /// </summary>
    [Test]
    public async Task Repair_runs_at_activation_for_a_leaf_that_never_persists_again()
    {
        // Checkpoint 12 with no snapshot: the leaf comes online ALREADY in the
        // stranded state, holding an honest claim nothing covers.
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: 12L, reClassifyEveryN: 1000);
        SeedRow(grain);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // No SetCheckpointOffsetAsync. No deactivation. The tree is quiescent.
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(0L),
            "a tree that stopped writing must still release its accumulated WAL; the persist-driven "
            + "trigger cannot reach it, so the activation-side repair has to");
        Assert.That(saved, Has.Count.EqualTo(1));
    }

    /// <summary>
    /// The safety assertion, and the one that must never be "fixed" away.
    /// <para>
    /// A partition that has never checkpointed makes no honest offset claim.
    /// Stamping coverage for it would satisfy <c>Math.Min(checkpoint, covered)</c>
    /// on the HLC plane and simultaneously stop
    /// <c>ComputeMaterialiserOffsetFloorAsync</c> abstaining on the offset plane -
    /// dropping both retention protections at once for a prefix the materialiser
    /// has never replayed. This covers two real shapes at once: the mixed-version
    /// upgrade (a leaf carrying <c>-1</c> coverage written by the old version and
    /// read by the new one) and the split sibling that received its rows by
    /// in-memory handoff and legitimately has no WAL offset.
    /// </para>
    /// <para>
    /// Both directions of the upgrade are covered by construction: the stamp is
    /// derived from the live checkpoint rather than read from the blob, so an old
    /// blob cannot inject a claim, and a new blob read by an old version carries
    /// only offsets that were already true.
    /// </para>
    /// </summary>
    [Test]
    public async Task Repair_does_not_stamp_coverage_for_a_partition_that_never_checkpointed()
    {
        const int partitions = 4;
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, reClassifyEveryN: 1000, walPartitions: partitions);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        SeedRow(grain);

        // Partition 0 checkpoints. Partitions 1..3 never do - they hold rows
        // whose only durable claim is un-replayed WAL, or none at all.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(5, CancellationToken.None);

        Assert.That(saved, Is.Not.Empty, "the repair fired for the checkpointed partition");
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(0L),
            "partition 0 made an honest claim, so it earns coverage");

        for (var p = 1; p < partitions; p++)
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(p), Is.EqualTo(-1L),
                $"precondition: partition {p} never checkpointed");
            Assert.That(grain.DurableSnapshotCoverageForPartition(p), Is.EqualTo(-1L),
                $"partition {p} makes no offset claim, so the repair must NOT stamp coverage for it: "
                + "the offset plane ABSTAINS on -1 on the stated assumption that the HLC block pin is "
                + "enforcing retention, so stamping it drops both protections at once");
        }
    }

    /// <summary>
    /// THE UPGRADE-SAFETY CASE, CONSTRUCTED THE WAY PRODUCTION CONSTRUCTS IT.
    /// Every other test in this file seeds partition 0 at the <c>-1</c>
    /// sentinel, which is a state the field cannot actually reach: partition 0
    /// lives in <c>LeafNodeState.ProjectionCheckpointOffset</c>, which has no
    /// initializer and whose only negative writer anywhere in the library is the
    /// admin projection-rebuild path. A leaf is therefore born at <c>0</c>, not
    /// at <c>-1</c> (issue #2703), and the serializer omits default-valued
    /// members so the value is absent on disk entirely.
    /// <para>
    /// That makes <c>checkpoint(0) == 0</c> ambiguous between "never applied
    /// anything" and "genuinely checkpointed at offset 0", and reading it as the
    /// latter is a silent-data-loss path: the repair would stamp coverage
    /// <c>0</c> for a partition that never replayed, turning
    /// <c>min(checkpoint, covered)</c> from <c>-1</c> into <c>0</c> and
    /// converting a correct Zero block pin into an offset-0 trim entitlement
    /// nothing earned. It is the exact hazard this fix must not introduce,
    /// arriving through a type default rather than through advancing a pin.
    /// </para>
    /// <para>
    /// The second half of the test is what stops it passing vacuously: the same
    /// leaf, once it takes a REAL checkpoint, must be repaired normally. The
    /// guard defers the repair by at most one WAL entry; it does not disable it.
    /// </para>
    /// </summary>
    [Test]
    public async Task Repair_does_not_stamp_coverage_for_a_leaf_born_at_the_default_checkpoint()
    {
        var (grain, state, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: 0L, reClassifyEveryN: 1000);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
            "precondition: the leaf carries the production BIRTH value for partition 0 - the type "
            + "default, not the -1 sentinel, which is the shape the field actually produces");

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        SeedRow(grain);

        // Live rows, a checkpoint scalar reading 0, and no durable snapshot:
        // indistinguishable on the persisted state alone from a leaf genuinely
        // checkpointed at offset 0, so the repair must refuse to claim.
        Assert.That(saved, Is.Empty,
            "a leaf that has only ever reported the type default has not proven it applied anything, "
            + "so the repair must not capture a blob that would carry an offset-0 coverage claim");
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "no coverage may be stamped for an unproven partition: the offset plane ABSTAINS on -1 "
            + "on the stated assumption that the HLC block pin is enforcing retention, so stamping it "
            + "here would drop both retention protections at once on upgrade");

        // ...and the guard defers rather than disables. One real checkpoint is
        // positive evidence, and the repair must then behave exactly as it does
        // for any other stranded leaf.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(5, CancellationToken.None);

        Assert.That(saved, Is.Not.Empty,
            "once the partition proves it applied something, the repair must fire normally - the "
            + "born-at-default guard costs at most one WAL entry of deferral, it does not disable "
            + "the repair");
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(0L),
            "the proven partition earns its honest coverage stamp");
    }

    /// <summary>
    /// THE SECOND DOOR ONTO THE SAME HAZARD. The repair is not the only writer
    /// of a coverage stamp: <c>CaptureSnapshotAsync</c> builds its claim from
    /// the same checkpoint scalar, and reaches it from the deactivation and
    /// advisory drivers that do not consult the repair predicate at all. Closing
    /// only the repair's door would leave a born-at-default leaf publishing an
    /// unearned offset-0 claim the moment it deactivated gracefully.
    /// <para>
    /// The separation asserted here is the one Half A's gate exists to state,
    /// and it is only now reachable. Reading partition 0's birth value as a real
    /// checkpoint made <c>anyPartitionCheckpointed</c> unconditionally true in
    /// production, which short-circuited that gate's loop on its first iteration
    /// and made the whole widening dead code outside an admin rebuild. With the
    /// gate reading proven evidence instead, a leaf holding live rows and no
    /// proven checkpoint takes the widened branch as intended: it EARNS
    /// DURABILITY by writing the blob, and it does NOT earn trim authority,
    /// because a blob containing these rows says nothing about whether the
    /// materialiser has consumed the corresponding WAL entries.
    /// </para>
    /// </summary>
    [Test]
    public async Task Deactivation_capture_of_a_born_at_default_leaf_earns_durability_but_no_offset_claim()
    {
        // TWO partitions, and only partition 1 checkpoints. This is the shape
        // that actually reaches the second door, and it is the common production
        // one (WalPartitions defaults to 8). Because partition 1 is proven,
        // anyPartitionCheckpointed is true and Half A's widened branch is
        // correctly SKIPPED - so the gate is not what protects partition 0 here.
        // Control passes straight to BuildCheckpointCoverage, which stamps
        // offsets[0] from the very scalar whose birth value is ambiguous.
        //
        // A first attempt at this test deactivated a single-partition leaf that
        // had taken no checkpoint at all, and no blob was written: the
        // deactivation driver declines on its own gate long before reaching
        // CaptureSnapshotAsync. The door is only open once SOME partition has
        // advanced this activation, which is exactly what this setup arranges.
        const int partitions = 2;
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: 0L, reClassifyEveryN: 1000, walPartitions: partitions);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);
        SeedRow(grain, dataKey);
        using (LatticeApplyOffsetContext.BeginScope(dataPartition, 4))
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        }

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        Assert.That(saved, Is.Not.Empty,
            "precondition: the deactivation driver reached CaptureSnapshotAsync - without a blob this "
            + "test would assert nothing about the coverage stamp at all");
        Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(4L),
            "the proven partition earns its honest coverage stamp");
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "durability and trim authority are separate claims. Partition 0 never proved it applied "
            + "anything, so no offset floor may be published for it - stamping its birth value as 0 "
            + "would turn min(checkpoint, covered) from -1 into 0 and authorise trimming a prefix no "
            + "consumer has read");
    }
    /// <c>exists p: checkpoint(p) &gt;= 0 AND coverage(p) &lt; 0</c>, and the
    /// <c>checkpoint &gt;= 0</c> conjunct is what makes it self-extinguishing:
    /// <c>BuildCheckpointCoverage</c> derives each partition's coverage FROM its
    /// checkpoint, so a checkpointed partition necessarily lands <c>&gt;= 0</c>,
    /// and coverage is monotone-max so it can never fall back to the sentinel.
    /// <para>
    /// Drop that conjunct and a never-checkpointed partition would stamp itself
    /// <c>-1</c>, leaving the predicate true and re-firing the capture on every
    /// persist forever. This test is the guard against that regression: it
    /// asserts a BOUND on the number of captures, not merely that one happened.
    /// </para>
    /// </summary>
    [Test]
    public async Task Repair_stops_firing_once_coverage_is_stamped()
    {
        // FOUR partitions, of which only partition 0 ever checkpoints. This
        // shape is the whole point: with one partition the termination conjunct
        // is unobservable, because the single partition is checkpointed and
        // gains coverage either way. It is the partitions that NEVER checkpoint
        // that can never be satisfied, and so re-arm the predicate forever if
        // the conjunct is lost. Verified by mutation: deleting
        // `GetCurrentCheckpointForPartition(p) >= 0` leaves this test GREEN at
        // one partition and RED here.
        const int partitions = 4;
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, reClassifyEveryN: 1000, walPartitions: partitions);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        SeedRow(grain);

        for (var i = 1; i <= 25; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(0L));
        Assert.That(saved, Has.Count.EqualTo(1),
            "25 persists must drive exactly ONE repair capture: the predicate is self-extinguishing, so a "
            + "count that tracks the persist count means the termination conjunct has been lost and every "
            + "checkpoint now pays for a full snapshot");
    }

    /// <summary>
    /// The repair budget is bounded and its exhaustion is a VISIBLE state, never
    /// silence.
    /// <para>
    /// A leaf whose snapshot store is persistently failing would otherwise retry
    /// the capture on every single checkpoint persist for the life of the
    /// activation, converting a storage outage into an amplification loop against
    /// the store that is already failing. The budget caps it. Because the capture
    /// path swallows its exceptions to keep the activation alive, an exhausted
    /// budget is exactly the shape this bucket has spent the day cataloguing - a
    /// mechanism that stops answering and looks identical to one with nothing to
    /// say - so exhaustion increments its own counter outcome and logs once per
    /// activation with the leaf identity.
    /// </para>
    /// </summary>
    [Test]
    public async Task Repair_attempts_are_bounded_when_capture_keeps_failing()
    {
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: new InvalidOperationException("snapshot store is down"));

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        SeedRow(grain);

        for (var i = 1; i <= 40; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "precondition: every capture failed, so coverage never landed and the deficit persists");
        Assert.That(saved, Has.Count.LessThanOrEqualTo(8),
            "40 persists against a failing store must not drive 40 captures: an outage must not be "
            + "amplified into a retry storm against the store that is already failing");
        Assert.That(saved, Is.Not.Empty,
            "...but the budget must be spent, not zero - a repair that never attempts is not a repair");
    }
}
