using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The byte-overflow pre-split at the CAPTURE SEAM (issue #2733).
/// <para>
/// Issue #2481 built the self-repair half of the byte bound -
/// <c>TrySplitForByteOverflowAsync</c>, which divides a leaf that is oversized
/// ALREADY - and then wired it to exactly one of the seven routes into
/// <c>CaptureSnapshotCoreAsync</c>, behind that route's own predicate. The
/// route was <c>TryRepairZeroCoverageAsync</c> and the predicate was
/// <c>HasCheckpointedPartitionWithoutCoverage</c>, which requires a partition
/// that has checkpointed AND lacks coverage. A tree holding no
/// proven-checkpointed partition never satisfies it, so no leaf on such a tree
/// was divided on the byte bound at all, and the other six routes - the grain
/// seam, the activation advisory, the coverage-deficit escape, the cadence
/// recheck, the deactivation hook, and cold-replay banking - reached the
/// capture with no size guard whatsoever.
/// </para>
/// <para>
/// Ordering compounded it. The activation advisory runs at Step 1.5, ahead of
/// the repair at Step 1.5b, and the coverage-deficit escape runs ahead of the
/// repair inside <c>MaybeRunPeriodicSnapshotRecheckAsync</c>. So even on a tree
/// where the predicate DID hold, an earlier driver reached the unguarded
/// capture first. That is why the affected deployment showed leaves that had
/// split and were still stuck.
/// </para>
/// <para>
/// <b>Why these fixtures drive the public seam only.</b> The pre-existing
/// coverage in <c>BPlusLeafGrainSplitByteBoundTests</c> reaches
/// <c>TrySplitForByteOverflowAsync</c> through
/// <c>BindingFlags.NonPublic</c> + <c>MethodInfo.Invoke</c>. That proves the
/// helper works WHEN CALLED and says nothing about whether anything calls it,
/// so it stayed green for the entire life of the defect while no production
/// route could reach the code it covers. It is a general trap worth naming: a
/// test that reaches past the public seam by reflection verifies the unit and
/// silently exempts the wiring, and the two failures are indistinguishable from
/// the test report. Every fixture below therefore enters through
/// <see cref="BPlusLeafGrain.CaptureSnapshotAsync"/> or through activation, and
/// none of them reflects.
/// </para>
/// </summary>
public sealed class BPlusLeafGrainCaptureSeamByteOverflowTests
{
    private const string CaptureSeamTreeId = "tree-capture-seam-byte-overflow";

    /// <summary>
    /// Everything a capture-seam test needs to observe, which is deliberately
    /// more than "did it throw": the blobs that reached durable storage, and the
    /// entry batches that reached split siblings.
    /// </summary>
    private sealed record CaptureSeamHarness(
        BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        List<LeafSnapshotBlob> Saved,
        List<Dictionary<string, LwwValue<byte[]>>> SiblingBatches);

    /// <summary>
    /// Builds a leaf that is over the byte bound already, on a tree with NO
    /// proven-checkpointed partition.
    /// </summary>
    /// <param name="persistedCheckpoint">
    /// Defaults to <c>-1</c> on purpose. That is the population the old wiring
    /// could not reach: with no checkpointed partition,
    /// <c>HasCheckpointedPartitionWithoutCoverage</c> is false, the repair
    /// driver declines, and the only surviving routes into capture were the
    /// unguarded ones. A fixture seeded with a non-negative checkpoint would be
    /// repairable under the OLD wiring too and so could not distinguish the fix.
    /// </param>
    /// <param name="entries">Rows the leaf comes online already holding.</param>
    /// <param name="bytesEach">Value size per row.</param>
    private static CaptureSeamHarness CreateOversizedLeaf(
        long maxLeafBytes,
        int entries,
        int bytesEach,
        int maxLeafKeys = 128,
        long persistedCheckpoint = -1L,
        FallOffLogDecision decision = FallOffLogDecision.TailReplay,
        TimeSpan? materialiserCheckpointInterval = null,
        int reClassifyEveryNCheckpoints = 0)
    {
        materialiserCheckpointInterval ??= TimeSpan.Zero;
        var saved = new List<LeafSnapshotBlob>();
        var siblingBatches = new List<Dictionary<string, LwwValue<byte[]>>>();

        // Seed through the snapshot the leaf rehydrates at activation, NOT by
        // writing into the cache's underlying dictionary.
        //
        // Two reasons, and the second is the one that matters. First, it is the
        // production shape: a leaf that grew oversized under an older build and
        // then went quiet comes back exactly this way, by loading its own
        // snapshot, with no write involved. Second, EntriesForTest is
        // Cache.UnderlyingRows, so assigning into it bypasses the cache's
        // StateBytes accounting entirely - StateBytes stays at 0, the byte
        // predicate is false for any bound, and a fixture seeded that way would
        // pass or fail for reasons unrelated to the code under test.
        var rows = new List<LeafSnapshotRow>(entries);
        for (var i = 0; i < entries; i++)
        {
            rows.Add(new LeafSnapshotRow(
                $"k{i:D4}",
                new LwwValue<byte[]> { Value = Payload(bytesEach), Timestamp = HybridLogicalClock.Zero }));
        }

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 0L,
                Rows = rows,
                CapturedAtTicks = DateTime.UtcNow.Ticks,
                SnapshotBytes = (long)entries * bytesEach,
                SnapshotOffsetsByPartition = [0L],
            }));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                saved.Add(ci.Arg<LeafSnapshotBlob>());
                return Task.CompletedTask;
            });

        // A split hands entries to a real sibling reference, so the stub needs a
        // grain context; a bare auto-stub fails GetGrainId. Recording the batch
        // is what lets a test measure how the division actually landed rather
        // than merely that one happened.
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(ci =>
            {
                siblingBatches.Add(
                    new Dictionary<string, LwwValue<byte[]>>(
                        ci.Arg<Dictionary<string, LwwValue<byte[]>>>()));
                return Task.CompletedTask;
            });
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(persistedCheckpoint < 0 ? 0L : persistedCheckpoint));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coord.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([]));

        // Defaults to TailReplay so that no capture observed in the other
        // fixtures can be attributed to the activation advisory. The activation
        // fixture overrides it to SnapshotPending, which is the decision that
        // arms _activationSnapshotPending and drives the "proactive snapshot
        // capture" the production logs show retrying against these leaves.
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(decision));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(detector);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = CaptureSeamTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeafBytes = maxLeafBytes,
                WalPartitions = 1,
                MaterialiserCheckpointInterval = materialiserCheckpointInterval.Value,

                // Disabled by default, so no capture observed in the other
                // fixtures can be attributed to the cadence recheck rather than
                // to the seam. The cadence fixture overrides it to 1, which is
                // the driver the production logs attribute 126 of the failing
                // captures to.
                LeafSnapshotReClassifyEveryNCheckpoints = reClassifyEveryNCheckpoints,
            },
            maxLeafKeys: maxLeafKeys,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return new CaptureSeamHarness(grain, state, saved, siblingBatches);
    }

    private static byte[] Payload(int size) => Encoding.UTF8.GetBytes(new string('x', size));

    /// <summary>
    /// THE acceptance test for issue #2733, and the one that is RED on the
    /// pre-fix implementation.
    /// <para>
    /// A leaf that is over the byte bound at process start, on a tree with no
    /// proven-checkpointed partition, receiving NO writes, must still be divided
    /// before its payload is materialised - with no forced snapshot, no pin
    /// reset, no re-index, and no configuration change. The only action taken
    /// here is the one the runtime already takes on its own: a snapshot capture.
    /// </para>
    /// <para>
    /// RED pre-fix for a mechanical reason, not a marginal one: the only call to
    /// <c>TrySplitForByteOverflowAsync</c> sat inside
    /// <c>TryRepairZeroCoverageAsync</c>, whose
    /// <c>HasCheckpointedPartitionWithoutCoverage</c> gate is false for this
    /// leaf, so the division was unreachable from <c>CaptureSnapshotAsync</c> by
    /// any path at all.
    /// </para>
    /// </summary>
    [Test]
    public async Task Capture_divides_a_leaf_that_is_already_over_the_byte_bound()
    {
        // 16 KiB against a 4 KiB bound: 4x over, and well inside the key bound,
        // so the key-count arm cannot be what divides it.
        var h = CreateOversizedLeaf(maxLeafBytes: 4096, entries: 16, bytesEach: 1024);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);
        var before = h.Grain.EntriesForTest.Count;

        await h.Grain.CaptureSnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                h.SiblingBatches, Is.Not.Empty,
                "the capture seam did not divide an already-oversized leaf: no entries were "
                + "handed to a sibling. This is issue #2733 - the pre-split is reachable from "
                + "only one of seven capture routes, behind a predicate this tree cannot satisfy.");
            Assert.That(
                h.Grain.EntriesForTest.Count, Is.LessThan(before),
                "the donor kept every entry, so no division actually landed.");
        });
    }

    /// <summary>
    /// The durability half of the same criterion: the leaf does not merely
    /// divide, it reaches a durable snapshot, and the blob it persists is
    /// UNDER the byte bound.
    /// <para>
    /// Asserted on <c>SnapshotBytes</c>, which the capture stamps from
    /// <c>Cache.StateBytes</c>, rather than on <c>Rows</c> - a modern blob
    /// carries its rows in <c>EncodedRows</c> and leaves <c>Rows</c> empty, so a
    /// row-count assertion would pass vacuously against an empty list and prove
    /// nothing. <c>SnapshotBytes</c> is also the quantity that actually matters:
    /// it is the size of the payload the serializer has to materialise
    /// contiguously, which is what threw <see cref="OutOfMemoryException"/>.
    /// </para>
    /// <para>
    /// Separate from the division assertion because they fail for different
    /// reasons. A blob whose bytes still equal the pre-capture total would mean
    /// the division ran AFTER the rows were copied, which is the ordering
    /// perturbation arm; the pre-split has to precede the row copy or it saves
    /// nothing, since the oversized payload is materialised either way.
    /// </para>
    /// </summary>
    [Test]
    public async Task Capture_persists_a_blob_under_the_byte_bound_for_an_oversized_leaf()
    {
        var h = CreateOversizedLeaf(maxLeafBytes: 4096, entries: 16, bytesEach: 1024);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        await h.Grain.CaptureSnapshotAsync();

        Assert.That(h.Saved, Is.Not.Empty, "the oversized leaf reached no durable snapshot at all.");
        var blob = h.Saved[^1];
        Assert.That(
            blob.SnapshotBytes, Is.LessThanOrEqualTo(4096L),
            $"the persisted blob is {blob.SnapshotBytes} bytes against a 4096-byte bound. Either no "
            + "division ran, or it ran AFTER the rows were copied - in which case the oversized "
            + "payload was materialised anyway and the pre-split saved nothing.");
    }

    /// <summary>
    /// The irreducible case, which must degrade rather than spin.
    /// <para>
    /// <c>IsLeafOverCapacity</c> carries a <c>Cache.Count &gt; 1</c> conjunct,
    /// so a leaf holding ONE entry larger than the bound cannot be divided by
    /// this mechanism at all - there is no pivot. The seam must still capture,
    /// and must not loop attempting a division that cannot make progress. This
    /// is the population the serializer change in
    /// <c>LatticeGrainStorageSerializer</c> addresses instead, and it is why
    /// that change is not redundant with this one.
    /// </para>
    /// </summary>
    [Test]
    public async Task Capture_of_an_irreducible_single_entry_leaf_does_not_divide_and_still_persists()
    {
        var h = CreateOversizedLeaf(maxLeafBytes: 1024, entries: 1, bytesEach: 8192);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        await h.Grain.CaptureSnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                h.SiblingBatches, Is.Empty,
                "a single-entry leaf has no pivot and must not be divided.");
            Assert.That(
                h.Grain.EntriesForTest.Count, Is.EqualTo(1),
                "the irreducible entry was lost.");
            Assert.That(
                h.Saved, Is.Not.Empty,
                "an irreducible leaf must still reach a durable snapshot; refusing to capture "
                + "would pin its tree's WAL trim floor at zero forever.");
        });
    }

    /// <summary>
    /// <b>Driver coverage, part two: the periodic cadence recheck.</b>
    /// <para>
    /// Attribution of the live failures on <c>repo-context-vector-index</c>
    /// splits almost evenly between two drivers over the same hour: 130 through
    /// the activation/proactive advisory, 126 through
    /// <c>MaybeRunPeriodicSnapshotRecheckAsync</c>, and ZERO through any
    /// coverage-repair message. Those counts are comparable, not one dominant,
    /// so a guard that covered only the advisory would leave about half the
    /// failing captures untouched and the deployment would still not reclaim
    /// its WAL.
    /// </para>
    /// <para>
    /// This fixture is the reason the guard was placed at
    /// <c>CaptureSnapshotCoreAsync</c> rather than at either driver. That
    /// method is the single seam all seven capture routes pass through, so
    /// covering the second driver required no second call site - and a driver
    /// added later inherits the guard by construction rather than by anyone
    /// remembering to wire it. The cadence recheck is driven here through a
    /// real checkpoint persist, the same trigger production uses; nothing calls
    /// <c>CaptureSnapshotAsync</c>.
    /// </para>
    /// </summary>
    [Test]
    public async Task Oversized_leaf_divides_through_the_cadence_recheck_with_no_external_call()
    {
        var h = CreateOversizedLeaf(
            maxLeafBytes: 1024,
            entries: 64,
            bytesEach: 1024,
            reClassifyEveryNCheckpoints: 1);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        Assert.That(
            h.SiblingBatches, Is.Empty,
            "POSITIVE CONTROL: the leaf was already divided before the cadence recheck ran, "
            + "so the assertion below would not be attributable to that driver.");

        // The production trigger: a durable checkpoint flush. The recheck runs
        // off the back of it, reaches the seam, and the seam divides.
        await ((ILeafProjection)h.Grain).SetCheckpointOffsetAsync(64L);

        Assert.Multiple(() =>
        {
            Assert.That(
                h.SiblingBatches, Is.Not.Empty,
                "the cadence recheck - the driver carrying roughly half the live failures - "
                + "reached the capture without passing the byte-overflow guard.");
            Assert.That(
                h.Saved, Is.Not.Empty,
                "the cadence recheck divided the leaf but never persisted a snapshot, so "
                + "coverage still never advances and the block pin is never released.");
        });
    }

    /// <summary>
    /// The deactivation exclusion, and the reason it costs nothing.
    /// <para>
    /// The seam declines to divide when the caller's token is already
    /// cancelled - a leaf on a deactivation deadline (issue #1965). Starting a
    /// multi-persist division that cannot finish before the deadline would
    /// leave a half-migrated split behind, which is a worse outcome than an
    /// oversized leaf.
    /// </para>
    /// <para>
    /// <b>Both halves are asserted, and the second is the point.</b> Declining
    /// is only acceptable because it is a DEFERRAL rather than a refusal: the
    /// next capture re-enters the same seam and divides then, with no operator
    /// action. A test that asserted only "did not divide" would equally pass an
    /// implementation that had quietly given up on the leaf forever, which is
    /// precisely the self-healing failure this issue exists to remove. The
    /// exclusion is therefore pinned together with its escape.
    /// </para>
    /// <para>
    /// Driven through <c>IGrainBase.OnDeactivateAsync</c>, the real Orleans
    /// lifecycle seam that supplies the deactivation token, rather than by
    /// reflecting into the private capture. That distinction is load-bearing
    /// here: reaching a production helper by reflection is exactly how the
    /// pre-existing byte-bound fixture stayed green for the whole life of this
    /// defect while the only call site was unreachable.
    /// </para>
    /// <para>
    /// <b>The deadline expires DURING the teardown, not before it, and that is
    /// the only shape that tests anything.</b> An already-cancelled token is
    /// the obvious way to write this test and it is worthless: the hook's very
    /// first step, <c>ILeafProjection.FlushCheckpointAsync</c>, opens with
    /// <c>ThrowIfCancellationRequested</c>, so the capture seam is never
    /// reached and both assertions below hold for any implementation of the
    /// guard. Measured, not assumed - written that way first, it stayed green
    /// with the guard deleted. The token is therefore cancelled from the state
    /// store's write callback, which fires inside that first flush, reproducing
    /// the real #1965 shape: a deactivation deadline that expires partway
    /// through teardown, after the hook has committed to running.
    /// </para>
    /// </summary>
    [Test]
    public async Task Capture_on_an_expired_deactivation_deadline_defers_the_division_to_the_next_capture()
    {
        // A non-zero materialiser interval is what leaves the checkpoint advance
        // PENDING rather than flushing it inline. The deactivation hook's own
        // FlushCheckpointAsync then performs the write, which is what fires the
        // state store's OnWriteState callback below - the only point inside the
        // teardown at which this test can expire the deadline.
        var h = CreateOversizedLeaf(
            maxLeafBytes: 1024,
            entries: 64,
            bytesEach: 1024,
            materialiserCheckpointInterval: TimeSpan.FromHours(1));
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        // Reaching the seam from the deactivation hook is not automatic, and a
        // test that skips this step passes vacuously.
        // TryCaptureSnapshotOnDeactivateAsync applies the #1535 no-loss gate
        // first and returns without capturing unless this activation produced
        // cache-backed coverage. Queue a real checkpoint advance through the
        // projection seam so the hook's own FlushCheckpointAsync latches
        // _checkpointAdvancedThisActivation and the capture is actually
        // attempted - otherwise the assertions below hold no matter what the
        // pre-split guard does.
        await ((ILeafProjection)h.Grain).SetCheckpointOffsetAsync(64L);

        using var deadline = new CancellationTokenSource();
        h.State.OnWriteState = _ => deadline.Cancel();

        await ((IGrainBase)h.Grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            deadline.Token);

        Assert.That(
            h.Saved, Is.Not.Empty,
            "POSITIVE CONTROL: the deactivation hook did not capture at all, so the assertion "
            + "below would hold for any implementation of the pre-split guard. Either the "
            + "#1535 no-loss gate declined (re-check that the queued checkpoint advance above "
            + "still latches _checkpointAdvancedThisActivation) or the token was already "
            + "cancelled on entry and FlushCheckpointAsync threw before the seam.");

        Assert.That(
            h.SiblingBatches, Is.Empty,
            "a leaf past its deactivation deadline must not start a multi-persist division "
            + "it cannot finish; a half-migrated split is worse than an oversized leaf.");

        // The deferral must not be a refusal. No reactivation, no operator
        // action, no configuration change - just the next capture.
        h.State.OnWriteState = null;
        await h.Grain.CaptureSnapshotAsync();

        Assert.That(
            h.SiblingBatches, Is.Not.Empty,
            "the deactivation exclusion silently became permanent: the leaf was never divided "
            + "on a later, uncancelled capture, so the exclusion is a refusal and not a deferral.");
    }

    /// <summary>
    /// A leaf already under the bound must not be divided by the seam. The guard
    /// is a repair for an oversized leaf, not a policy applied to every capture.
    /// </summary>
    [Test]
    public async Task Capture_does_not_divide_a_leaf_that_is_under_the_byte_bound()
    {
        var h = CreateOversizedLeaf(maxLeafBytes: 1024 * 1024, entries: 16, bytesEach: 1024);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        await h.Grain.CaptureSnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.SiblingBatches, Is.Empty, "an in-bound leaf was divided.");
            Assert.That(h.Grain.EntriesForTest.Count, Is.EqualTo(16));
        });
    }

    /// <summary>
    /// The bound being disabled (<c>MaxLeafBytes = 0</c>) must disable the seam
    /// guard too, not merely the write-path predicate. A configuration that
    /// switches a feature off has to switch off every arm of it.
    /// </summary>
    [Test]
    public async Task Capture_does_not_divide_when_the_byte_bound_is_disabled()
    {
        var h = CreateOversizedLeaf(maxLeafBytes: 0, entries: 16, bytesEach: 1024);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        await h.Grain.CaptureSnapshotAsync();

        Assert.That(h.SiblingBatches, Is.Empty, "the byte bound is disabled but the seam divided anyway.");
    }

    /// <summary>
    /// The activation route, which is the one that matters for the stated
    /// acceptance criterion: no operator action at all, not even a capture call.
    /// <para>
    /// A leaf that comes online oversized must divide during its own activation.
    /// This is the difference between "recovers when someone pokes it" and
    /// "recovers on its own", and only the second bounds WAL retention on a
    /// deployment nobody is touching.
    /// </para>
    /// <para>
    /// The advisory is armed with <c>SnapshotPending</c> because that is the
    /// decision that sets <c>_activationSnapshotPending</c> and so drives the
    /// Step 1.5 capture - the "proactive snapshot capture for leaf ... failed"
    /// the production logs show retrying against this exact population. That
    /// route is one of the six that carried NO size guard before this change,
    /// and it runs AHEAD of the repair driver that carried the only one, which
    /// is why a leaf could split and stay stuck.
    /// </para>
    /// </summary>
    [Test]
    public async Task Oversized_leaf_divides_during_activation_with_no_external_call()
    {
        // Oversized BEFORE activation, via the snapshot it rehydrates: precisely
        // the population that receives no writes and that no write-path
        // predicate can ever reach.
        var h = CreateOversizedLeaf(
            maxLeafBytes: 4096,
            entries: 16,
            bytesEach: 1024,
            decision: FallOffLogDecision.SnapshotPending);

        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        Assert.That(
            h.SiblingBatches, Is.Not.Empty,
            "a leaf that came online over the byte bound was not divided by its own activation, "
            + "so it needs an operator action to recover. That is the criterion this issue exists "
            + "to meet.");
    }

    /// <summary>
    /// Convergence, stated as a measured property rather than an assumption.
    /// <para>
    /// <c>SplitAsync</c> pivots on the COUNT median and the loop re-checks only
    /// the donor, so a division leaves the donor under bound while a sibling can
    /// still be over it. Healing is therefore generational: each capture drives
    /// the leaf it is capturing under bound, and an oversized sibling converges
    /// on its own subsequent capture. This test pins the property that actually
    /// bounds retention - the CAPTURED leaf always ends under bound - so that a
    /// future change to the pivot cannot silently regress it.
    /// </para>
    /// </summary>
    [Test]
    public async Task Capture_drives_the_captured_leaf_under_the_byte_bound_in_one_pass()
    {
        // 16x over the bound, to exercise more than a single halving.
        var h = CreateOversizedLeaf(maxLeafBytes: 4096, entries: 64, bytesEach: 1024);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        await h.Grain.CaptureSnapshotAsync();

        var remaining = h.Grain.EntriesForTest.Values.Sum(v => (long)(v.Value?.Length ?? 0));
        Assert.That(
            remaining, Is.LessThanOrEqualTo(4096),
            $"the captured leaf is still over the byte bound after a full pass ({remaining} bytes). "
            + "The division loop is bounded at MaxByteOverflowSplitsPerPass, so a leaf more than "
            + "2^8 times over bound would legitimately need a second pass - but 16x must converge "
            + "in one.");
    }

    /// <summary>
    /// Re-entry across captures, which is the convergence guarantee this change
    /// actually makes, stated as a test rather than an assumption.
    /// <para>
    /// The division loop is capped at <c>MaxByteOverflowSplitsPerPass</c> (8),
    /// and <c>SplitAsync</c> pivots on the COUNT median, so one pass divides the
    /// captured leaf by at most 2^8. A leaf further over the bound than that
    /// legitimately needs more than one pass. Of the two remedies - loop
    /// unboundedly until every side is under bound, or guarantee the next
    /// capture re-enters the split - this change takes the second, because the
    /// pre-split now sits at the single seam EVERY capture route passes
    /// through, so re-entry is structural rather than something an additional
    /// driver has to arrange. An unbounded loop would also do arbitrarily many
    /// persists inside one grain turn, which is what the per-pass cap exists to
    /// prevent.
    /// </para>
    /// <para>
    /// 512x over the bound here, so the first pass provably cannot finish the
    /// job: 8 halvings take 512 entries to 2, which is still over. The second
    /// capture must make further progress, and that is the property pinned.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_leaf_too_far_over_bound_for_one_pass_converges_on_the_next_capture()
    {
        // maxLeafKeys well above the entry count so the key-count arm plays no
        // part; this must be the byte arm alone.
        var h = CreateOversizedLeaf(
            maxLeafBytes: 1024, entries: 512, bytesEach: 1024, maxLeafKeys: 100_000);
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);

        await h.Grain.CaptureSnapshotAsync();
        var afterFirst = h.Grain.EntriesForTest.Count;

        await h.Grain.CaptureSnapshotAsync();
        var afterSecond = h.Grain.EntriesForTest.Count;

        Assert.Multiple(() =>
        {
            Assert.That(
                afterFirst, Is.LessThan(512).And.GreaterThan(1),
                $"the first pass left {afterFirst} entries. It is capped at 8 halvings, so from "
                + "512 it should reach 2 - neither finishing the job nor failing to start it.");
            Assert.That(
                afterSecond, Is.LessThan(afterFirst),
                "the second capture made no further progress, so a leaf beyond one pass's reach "
                + "never converges. The pre-split has to be re-entered by the NEXT capture, which "
                + "is the whole reason it belongs at the shared seam rather than on one driver.");
        });
    }
}
