using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the ungated activation-seed WAL trim pin (issue
/// 2150). <c>SeedDurableMaterialiserFrontierAsync</c> reported the RAW
/// <c>GetCurrentCheckpointForPartition(partition)</c> as the durable pin's
/// checkpoint offset, while both sibling report sites
/// (<c>ReportCursorIfActiveAsync</c> and
/// <c>FlushDurableMaterialiserFrontierAsync</c>) route the same report through
/// the coverage gate <c>ResolveDurablePinForPartition</c>. Nothing in the code
/// stated why the seed differed.
/// <para>
/// The derivation says the gate was missing rather than unnecessary, but NOT
/// for the reason the frontier dimension suggests. The seed's sole caller
/// (<c>OnActivateAsync</c>) invokes it only when
/// <c>Clock &lt;= HybridLogicalClock.Zero</c>, and every branch of the gate
/// returns either that clock or the literal Zero - so the seeded FRONTIER is
/// Zero either way and the gate is provably inert in that dimension. The
/// offset dimension is a different matter: <c>WalMaterialiserPinGrain.Merge</c>
/// merges frontier and offset INDEPENDENTLY and both monotonically (see
/// <c>WalMaterialiserPinGrainTests.ReportManyAsync_offset_never_regresses_on_a_stale_report</c>),
/// so a Zero frontier does not neutralise an over-reported offset - it is
/// recorded permanently and no later, correctly-gated report can lower it.
/// </para>
/// <para>
/// A leaf reaches this seam with a checkpoint at or past an offset its durable
/// snapshot does not cover while its clock is still Zero: the activation replay
/// bumps a partition's max-applied offset for entries it SKIPS because they
/// route to another leaf's key range, which never advances this leaf's clock.
/// Seeding the raw checkpoint then publishes an offset floor beyond durable
/// coverage, and that floor outlives the Zero block - once the leaf takes real
/// data and its first gated flush releases the frontier, the stale
/// over-advanced offset is still the merged maximum and the shared-shard WAL GC
/// is authorised to trim a prefix no cold rebuild can replay
/// (<see cref="LeafProjectionStaleException"/>).
/// </para>
/// <para>
/// Test classification: the first two tests are DISCRIMINATORS (red before the
/// fix, green after - they distinguish the gated site from the ungated one).
/// The last two are GUARDS (green both before and after) - they lock the
/// preconditions the site's correctness rests on, so a later edit that removes
/// the caller's Zero gate, or that over-blocks the ubiquitous never-checkpointed
/// seed, goes red.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string SeedPinTreeId = "tree-activation-seed-pin";

    private sealed record SeedPinNote(string ConsumerId, HybridLogicalClock Frontier, long Offset);

    /// <summary>
    /// Builds a leaf poised on the activation-seed path: a persisted
    /// per-partition checkpoint, a clock still at
    /// <see cref="HybridLogicalClock.Zero"/>, an empty per-activation cache, and
    /// a replay that finds nothing new (so <c>OnActivateAsync</c> falls through
    /// to the seed rather than to <c>ReportCursorIfActiveAsync</c>). Every
    /// <c>NoteDurableMaterialiserFrontier</c> call is captured, along with the
    /// awaited batched flush the non-seed path would take instead.
    /// </summary>
    private static (BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        List<SeedPinNote> Notes,
        Func<IReadOnlyList<MaterialiserPinReport>?> LastFlush)
        CreateActivationSeedLeaf(
            long persistedCheckpoint,
            LeafSnapshotBlob? snapshot = null,
            HybridLogicalClock? clock = null)
    {
        var notes = new List<SeedPinNote>();
        IReadOnlyList<MaterialiserPinReport>? flushed = null;

        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.When(r => r.NoteDurableMaterialiserFrontier(
                Arg.Any<string>(), Arg.Any<string>(),
                Arg.Any<HybridLogicalClock>(), Arg.Any<long>()))
            .Do(ci => notes.Add(new SeedPinNote(
                ci.ArgAt<string>(1), ci.ArgAt<HybridLogicalClock>(2), ci.ArgAt<long>(3))));
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => flushed = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(snapshot));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        // The WAL head sits at the persisted checkpoint and the slice read is
        // empty, so the activation replay applies nothing and `advanced` stays
        // false - the precondition for reaching the seed at all. A tail of 0
        // keeps the snapshot rehydrate on its decline path, which still records
        // durable coverage (that is the point: coverage is available here).
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Math.Max(0L, persistedCheckpoint)));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = SeedPinTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;
        if (clock is { } c)
            state.State.Clock = c;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                // Disable the periodic snapshot recheck so no capture can
                // advance coverage behind the assertion's back.
                LeafSnapshotReClassifyEveryNCheckpoints = 0,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (grain, state, notes, () => flushed);
    }

    private static LeafSnapshotBlob SeedPinSnapshotCovering(long coveredOffset) => new()
    {
        SnapshotOffset = coveredOffset,
        Rows = new List<LeafSnapshotRow>(),
        CapturedAtTicks = DateTime.UtcNow.Ticks,
        SnapshotBytes = 0L,
        SnapshotOffsetsByPartition = new[] { coveredOffset },
    };

    [Test]
    public async Task Activation_seed_pin_never_publishes_an_offset_beyond_durable_snapshot_coverage()
    {
        // DISCRIMINATOR. The leaf durably checkpointed partition 0 through
        // offset 5, but the only durable snapshot covers offset 3 - so offsets
        // 4 and 5 exist nowhere but the WAL. The clock is still Zero (the
        // checkpoint was advanced by replay over entries this leaf SKIPPED),
        // so activation takes the seed path.
        var (grain, state, notes, _) = CreateActivationSeedLeaf(
            persistedCheckpoint: 5, snapshot: SeedPinSnapshotCovering(3));

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(state.State.Clock, Is.EqualTo(HybridLogicalClock.Zero),
            "the scenario is only the seed path while the clock is still Zero");
        Assert.That(notes, Has.Count.EqualTo(1),
            "one durable pin note per WAL partition is seeded at activation");

        // RED (pre-fix):  offset 5 - the raw checkpoint, two offsets beyond
        //                 anything a durable snapshot holds. Because the pin
        //                 store merges offsets monotonically and independently
        //                 of the frontier, that 5 is permanent: the leaf's
        //                 later, correctly-gated flush of min(5, 3) = 3 cannot
        //                 pull it back down, so once the frontier is released
        //                 the WAL GC may trim offsets 4 and 5 and the next cold
        //                 rebuild falls off the log.
        // GREEN (post-fix): min(checkpoint, covered) = 3.
        Assert.That(notes[0].Offset, Is.EqualTo(3L),
            "the activation seed must publish min(checkpoint, snapshotCoverage), not the raw checkpoint; "
            + "the pin store's offset merge is monotonic and independent of the frontier, so an offset "
            + "seeded beyond durable coverage is recorded permanently and no later gated report can lower it");
        Assert.That(notes[0].Offset, Is.LessThanOrEqualTo(grain.DurableSnapshotCoverageForPartition(0)),
            "the pin offset is a WAL trim floor: it may never authorise trimming past what a durable "
            + "snapshot actually covers for the partition");
    }

    [Test]
    public async Task Activation_seed_pin_blocks_when_a_checkpointed_partition_has_no_snapshot_at_all()
    {
        // DISCRIMINATOR. The incident census shape: a durable checkpoint at
        // offset 5 and ZERO snapshots anywhere. The whole prefix [0, 5] is
        // recoverable only from the WAL, so the seed must contribute no offset
        // floor at all (the -1 sentinel) rather than authorising a trim to 5.
        var (grain, _, notes, _) = CreateActivationSeedLeaf(
            persistedCheckpoint: 5, snapshot: null);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(notes, Has.Count.EqualTo(1));
        Assert.That(notes[0].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            "a never-checkpointed leaf seeds the Zero block pin");

        // RED (pre-fix): 5. GREEN (post-fix): -1.
        Assert.That(notes[0].Offset, Is.EqualTo(-1L),
            "with no snapshot covering the checkpointed prefix, the seed must publish the -1 sentinel so "
            + "the GC's offset floor cannot advance past a prefix whose only durable copy is the WAL");
    }

    [Test]
    public async Task Activation_seed_pin_is_only_taken_while_the_leaf_clock_is_still_zero()
    {
        // GUARD (green before and after the fix). The seed's frontier dimension
        // is inert ONLY because its sole caller gates on
        // `Clock <= HybridLogicalClock.Zero`. That precondition is what lets the
        // site reason about offsets alone. Removing the caller's gate - or
        // adding a second, ungated call site - turns the seed into a
        // real-frontier publisher and breaks that reasoning, so pin it here: a
        // leaf whose clock has advanced must take the batched, gated flush
        // instead and must not seed at all.
        var (grain, _, notes, lastFlush) = CreateActivationSeedLeaf(
            persistedCheckpoint: 5,
            snapshot: SeedPinSnapshotCovering(3),
            clock: new HybridLogicalClock { WallClockTicks = 500 });

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(notes, Is.Empty,
            "a leaf whose clock has advanced past Zero must not take the activation-seed path; the seed "
            + "reasons about offsets alone precisely because its only caller guarantees a Zero clock");

        var reports = lastFlush();
        Assert.That(reports, Is.Not.Null,
            "the advanced-clock leaf publishes through the batched, coverage-gated flush instead");
        Assert.That(reports![0].Frontier, Is.GreaterThan(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task Activation_seed_pin_stays_live_for_a_leaf_that_never_checkpointed()
    {
        // GUARD (green before and after the fix). Liveness: the ubiquitous case
        // is a leaf that has applied nothing durably (checkpoint == -1). Gating
        // the seed must not turn that into anything different - it already
        // reports the Zero block frontier with the -1 sentinel offset, and it
        // must keep doing so. This is the test that goes red if a future edit
        // "hardens" the seed into blocking or offsetting something it should
        // not, which would wedge WAL trim for every fresh leaf in the tree.
        var (grain, _, notes, _) = CreateActivationSeedLeaf(
            persistedCheckpoint: -1, snapshot: null);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(notes, Has.Count.EqualTo(1));
        Assert.That(notes[0].Frontier, Is.EqualTo(HybridLogicalClock.Zero));
        Assert.That(notes[0].Offset, Is.EqualTo(-1L),
            "a leaf that has applied nothing durably seeds the Zero block pin with the -1 sentinel offset, "
            + "exactly as it did before the coverage gate was applied to this site");
    }
}
