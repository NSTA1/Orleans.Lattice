using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Reflection;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Bounded leaf division (issue #2771): an oversized leaf divides without
/// materialising itself first.
/// <para>
/// Division was the only operation that could make an oversized leaf smaller,
/// and it was the operation most certain to fail on one. Both halves of it
/// reached for an accessor that hydrates the whole leaf as a precondition -
/// <c>Cache.Keys</c> to choose the median, then <c>Cache.EnumerateRows()</c> to
/// read the rows being moved - so the probability that a division succeeded
/// fell as the leaf grew, which is the exact inverse of the relationship the
/// operation needs. Production showed the consequence directly: of 179
/// proactive capture failures, 76 threw out of <c>HydrateBlock</c> and 26 out
/// of <c>CompleteSplitAsync</c>, every one an <see cref="OutOfMemoryException"/>.
/// </para>
/// <para>
/// The fixtures below assert the repaired shape as a property rather than as a
/// threshold: the peak resident row count during a division is bounded by the
/// batch, so it does not grow with the leaf. A leaf four times larger divides
/// at the same peak, which is what makes this a fix rather than a larger
/// allowance - no constant here is tuned to any particular host's memory.
/// </para>
/// </summary>
public sealed class BPlusLeafGrainBoundedSplitTransferTests
{
    /// <summary>Rows are 256 payload bytes each, so the corpus size is predictable.</summary>
    private static byte[] Payload(int i)
    {
        var bytes = new byte[256];
        for (var b = 0; b < bytes.Length; b++)
        {
            bytes[b] = (byte)((i + b) & 0xFF);
        }

        return bytes;
    }

    private static string Key(int i) => $"k{i:D6}";

    private static LeafSnapshotRow[] Rows(int rowCount)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            rows[i] = new LeafSnapshotRow(
                Key(i),
                LwwValue<byte[]>.Create(
                    Payload(i),
                    new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i }));
        }

        return rows;
    }

    /// <summary>
    /// Records what the donor handed to the sibling, and - the measurement that
    /// matters - how many rows were resident in the donor's cache at the moment
    /// of each hand-off. That is the peak this fix exists to bound.
    /// </summary>
    private sealed class TransferRecorder
    {
        public List<Dictionary<string, LwwValue<byte[]>>> Batches { get; } = [];

        public List<int> ResidentRowsAtEachBatch { get; } = [];

        public int PeakResidentRows => ResidentRowsAtEachBatch.Count == 0
            ? 0
            : ResidentRowsAtEachBatch.Max();

        public IEnumerable<string> AllTransferredKeys => Batches.SelectMany(b => b.Keys);

        /// <summary>Batch index to fail on, or -1 to never fail.</summary>
        public int FailOnBatchIndex { get; set; } = -1;
    }

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, TransferRecorder Recorder, ILeafSnapshotStorageGrain Snapshots)
        CreateDividableLeaf(
            long maxLeafBytes = 64L * 1024,
            long residentBudgetBytes = 16L * 1024,
            int maxLeafKeys = 1_000_000,
            LeafSnapshotRow[]? snapshotRows = null)
    {
        var recorder = new TransferRecorder();

        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(snapshotRows is null
                ? null
                : new LeafSnapshotBlob
                {
                    SnapshotOffset = 25L,
                    EncodedRows = LeafSnapshotCodec.Encode(snapshotRows),
                    SnapshotOffsetsByPartition = [25L],
                }));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-bounded-split";

        // Deliberately NOT seeding state.State.ProjectionHash. A leaf that
        // rehydrates from a snapshot always has a null hash, because
        // TryRehydrateFromSnapshotAsync nulls it on purpose to force the
        // canonical full-walk recompute the chained internal-node fold
        // depends on. Seeding one here would model a leaf that cannot occur
        // in the population under test, and would hide the recompute - which
        // is itself a whole-leaf materialiser on the split path.

        var resolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                MaxLeafBytes = maxLeafBytes,
                LeafPartialHydrationEnabled = true,
                LeafHydrationResidentBytes = residentBudgetBytes,
            },
            maxLeafKeys: maxLeafKeys,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            resolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        // Wired after the grain exists so the callback can observe its cache.
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(call =>
            {
                var batch = call.Arg<Dictionary<string, LwwValue<byte[]>>>();
                var index = recorder.Batches.Count;
                recorder.Batches.Add(new Dictionary<string, LwwValue<byte[]>>(batch));
                recorder.ResidentRowsAtEachBatch.Add(grain.CacheForTest.HydratedRowCount);

                if (index == recorder.FailOnBatchIndex)
                {
                    throw new OutOfMemoryException("simulated interruption mid-transfer");
                }

                return Task.FromResult<SplitResult?>(null);
            });

        return (grain, state, recorder, snapshotStub);
    }

    private static async Task<(BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, TransferRecorder Recorder, ILeafSnapshotStorageGrain Snapshots)>
        RehydratedDividableLeafAsync(
            int rowCount,
            long maxLeafBytes = 64L * 1024,
            long residentBudgetBytes = 16L * 1024)
    {
        var created = CreateDividableLeaf(
            maxLeafBytes, residentBudgetBytes, snapshotRows: Rows(rowCount));

        Assert.That(
            await created.Grain.TryRehydrateFromSnapshotAsync(CancellationToken.None),
            Is.True,
            "the leaf must come online from its snapshot without decoding it");
        Assert.That(
            created.Grain.CacheForTest.HydratedRowCount,
            Is.Zero,
            "activation must not materialise a row - otherwise the arm below measures nothing");

        return created;
    }

    private static Task<SplitResult> InvokeSplit(BPlusLeafGrain grain)
    {
        var method = typeof(BPlusLeafGrain).GetMethod(
            "SplitAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(
            method,
            Is.Not.Null,
            "SplitAsync not found - was it renamed? It is the division entry point issue #2771 repairs.");
        return (Task<SplitResult>)method!.Invoke(grain, [])!;
    }

    // ---------------------------------------------------------------
    // Arm 0 - the accessors themselves, isolated from the split.
    // ---------------------------------------------------------------

    [Test]
    public async Task The_structural_pivot_is_available_without_materialising_a_row()
    {
        var (grain, _, _, _) = await RehydratedDividableLeafAsync(rowCount: 2048);
        var cache = grain.CacheForTest;

        Assert.That(
            cache.TryGetBisectingKeyWithoutHydrating(out var pivot),
            Is.True,
            "a lazily hydrated leaf must be able to place its cut from the frame's ordinal index; "
            + "a refusal here sends the split back to Cache.Keys and hydrates the whole leaf");
        Assert.That(pivot, Is.EqualTo(Key(1024)));
        Assert.That(
            cache.HydratedRowCount,
            Is.Zero,
            "placing the cut must not materialise a single row");
    }

    [Test]
    public async Task Transfer_boundaries_are_available_without_materialising_a_row()
    {
        var (grain, _, _, _) = await RehydratedDividableLeafAsync(rowCount: 2048);
        var cache = grain.CacheForTest;

        Assert.That(cache.TryGetBisectingKeyWithoutHydrating(out var pivot), Is.True);
        var boundaries = cache.GetTransferBatchBoundariesWithoutHydrating(pivot, 16L * 1024);

        Assert.That(
            boundaries, Is.Not.Empty,
            "half of a 2048-row leaf must not fit one 16 KiB batch - an empty result here means "
            + "the transfer degenerates to the single whole-half pass this fix exists to remove");
        Assert.That(boundaries, Is.Ordered.Using<string>(StringComparer.Ordinal));
        Assert.That(
            cache.HydratedRowCount,
            Is.Zero,
            "planning the transfer must not materialise a single row");
    }

    // A guard is only a guard if something fails when it is removed. Under
    // today's code the refusal path below is unreachable from the grain: the
    // frame is strictly ascending, so whenever block 0 is materialised its
    // rows necessarily sort below the frame's median. Reverting the guard
    // therefore reddened nothing, which by the perturbation discipline makes
    // it a dead clause to be deleted rather than shipped.
    //
    // It is kept because the state it refuses is reachable at the cache's own
    // surface, and specified here so it is enforced rather than assumed. If a
    // later change ever lets a materialised row be dropped after its block
    // hydrated, the ascending-frame proof stops holding and an unguarded
    // bisect would return a pivot with nothing below it - migrating every
    // entry and leaving an empty donor, the non-terminating shape
    // IsLeafOverCapacity's Count > 1 conjunct exists to exclude.
    [Test]
    public async Task A_pivot_with_nothing_below_it_is_refused_rather_than_returned()
    {
        var (grain, _, _, _) = await RehydratedDividableLeafAsync(rowCount: 2048);
        var cache = grain.CacheForTest;

        // Materialise block 0, then drop exactly the rows it brought in. That
        // leaves the frame reporting block 0 as hydrated while no materialised
        // row sorts below the median - the one state in which the ascending
        // frame no longer proves interiority.
        foreach (var _ in cache.EnumerateRange(Key(0), Key(1)))
        {
        }

        Assert.That(
            cache.HydratedRowCount,
            Is.GreaterThan(0),
            "the arm must actually materialise block 0, or it constructs nothing");

        var block0Keys = new List<string>();
        foreach (var row in cache.EnumerateRange(Key(0), Key(32)))
        {
            block0Keys.Add(row.Key);
        }

        foreach (var key in block0Keys)
        {
            cache.Remove(key);
        }

        Assert.That(
            cache.HydratedRowCount,
            Is.Zero,
            "the arm must leave no materialised row below the median, or it is not exercising "
            + "the refusal path");

        Assert.That(
            cache.TryGetBisectingKeyWithoutHydrating(out _),
            Is.False,
            "a bisecting key with nothing proven below it must be refused, so the caller falls "
            + "back to the ordered view instead of dividing into an empty donor");
    }

    // ---------------------------------------------------------------
    // Arm 1 - the load-bearing property.
    // ---------------------------------------------------------------

    [Test]
    public async Task A_leaf_larger_than_the_hydration_budget_completes_a_division_without_ever_fully_hydrating()
    {
        // 2048 rows x 256 payload bytes is far beyond the 16 KiB residency
        // figure the batch is sized against, so a division that insisted on
        // materialising the leaf first would have to materialise all of it.
        var (grain, _, recorder, _) = await RehydratedDividableLeafAsync(rowCount: 2048);
        var cache = grain.CacheForTest;

        var result = await InvokeSplit(grain);

        Assert.That(result, Is.Not.Null, "the division must complete");
        Assert.That(recorder.Batches, Is.Not.Empty, "rows must actually have moved to the sibling");

        // The acceptance property as issue #2771 words it: no full hydration
        // occurred. HasPendingHydration going false would mean the source had
        // been drained and detached - precisely the whole-leaf materialisation
        // that was making the largest leaves undividable.
        Assert.That(
            cache.HasPendingHydration,
            Is.True,
            "dividing the leaf must not have drained its hydration source - the left half was "
            + "never read, so it must still be lazily backed. Residency per batch was: "
            + string.Join(",", recorder.ResidentRowsAtEachBatch)
            + " across " + recorder.Batches.Count + " batches");
        Assert.That(
            cache.HydratedRowCount,
            Is.LessThan(2048),
            "the whole leaf must never have been resident at once");
    }

    [Test]
    public async Task Peak_residency_during_a_division_does_not_grow_with_the_leaf()
    {
        // The property that makes this a fix rather than a larger allowance.
        // Hold the batch budget fixed and quadruple the leaf: if the peak
        // tracked the leaf, an oversized leaf would still be undividable and
        // this change would merely have moved the threshold.
        var (smallGrain, _, smallRecorder, _) = await RehydratedDividableLeafAsync(rowCount: 512);
        var (largeGrain, _, largeRecorder, _) = await RehydratedDividableLeafAsync(rowCount: 2048);

        await InvokeSplit(smallGrain);
        await InvokeSplit(largeGrain);

        Assert.That(smallRecorder.PeakResidentRows, Is.GreaterThan(0));
        Assert.That(
            largeRecorder.PeakResidentRows,
            Is.EqualTo(smallRecorder.PeakResidentRows),
            "a four-times-larger leaf must divide at the same peak residency; if this grows with "
            + "the leaf then the largest leaves are still undividable and issue #2771 is not fixed");

        // And the larger leaf must genuinely have been moved in more batches -
        // otherwise the equality above would hold vacuously because neither
        // leaf was batched at all.
        Assert.That(
            largeRecorder.Batches.Count,
            Is.GreaterThan(smallRecorder.Batches.Count),
            "the larger leaf must have been transferred in more batches, not in a larger one");
    }

    [Test]
    public async Task Every_row_at_or_above_the_pivot_is_transferred_exactly_once()
    {
        var (grain, _, recorder, _) = await RehydratedDividableLeafAsync(rowCount: 1024);

        var result = await InvokeSplit(grain);

        var transferred = recorder.AllTransferredKeys.ToList();
        Assert.That(
            transferred.Count,
            Is.EqualTo(transferred.Distinct().Count()),
            "batching must not hand the same key to the sibling twice");
        Assert.That(
            transferred,
            Is.All.GreaterThanOrEqualTo(result.PromotedKey),
            "no key below the pivot may be transferred");
        Assert.That(
            transferred.Count + grain.CacheForTest.Count,
            Is.EqualTo(1024),
            "the donor's residue plus the transferred set must be the original leaf - no loss, "
            + "no duplication");
    }

    // ---------------------------------------------------------------
    // Arm 2 - the control. The already-resident population must be untouched.
    // ---------------------------------------------------------------

    [Test]
    public async Task A_fully_resident_leaf_divides_exactly_as_it_did_before()
    {
        // No snapshot is attached, so the structural pivot cannot be taken and
        // the ordered-key fallback runs. This is the population that already
        // worked, and it must be bit-for-bit unchanged.
        var (grain, _, recorder, _) = CreateDividableLeaf(maxLeafBytes: 1L << 40);

        for (var i = 0; i < 64; i++)
        {
            await grain.SetAsync(Key(i), Payload(i));
        }

        var result = await InvokeSplit(grain);

        Assert.That(result, Is.Not.Null);
        Assert.That(
            recorder.Batches.Count,
            Is.EqualTo(1),
            "a leaf that is already wholly resident has nothing to bound, so it must still move "
            + "in a single pass - the batching must not impose a cost on the population that "
            + "never had the problem");
        Assert.That(recorder.AllTransferredKeys.Count(), Is.EqualTo(32));
        Assert.That(result.PromotedKey, Is.EqualTo(Key(32)));
    }

    // ---------------------------------------------------------------
    // Arm 3 - interruption. The intermediate state must not be durable.
    // ---------------------------------------------------------------

    [Test]
    public async Task An_interruption_mid_transfer_leaves_the_donors_durable_state_whole()
    {
        // Batching makes a partially-transferred leaf reachable, so the state
        // it leaves behind has to be pinned rather than argued about. The
        // invariant is that the donor never PERSISTS a partial transfer: rows
        // are removed from the in-memory cache batch by batch, but nothing
        // writes them out until the division completes. A donor that dies here
        // therefore reloads holding every row it started with, which is what
        // keeps the intermediate state unobservable - the parent has not been
        // told the sibling exists, so every read still routes to the donor and
        // the donor still has everything.
        var (grain, state, recorder, snapshots) = await RehydratedDividableLeafAsync(rowCount: 1024);
        recorder.FailOnBatchIndex = 2;

        Assert.That(
            async () => await InvokeSplit(grain),
            Throws.InstanceOf<OutOfMemoryException>(),
            "the arm must actually interrupt the transfer, or it proves nothing");

        Assert.That(
            recorder.Batches.Count,
            Is.EqualTo(3),
            "the interruption must land mid-transfer, with batches both before and after it");

        // The load-bearing assertion. A leaf's durable row image is its
        // snapshot blob, not its LeafNodeState - so the only way a partial
        // transfer could become durable is a capture taken mid-division. None
        // is taken, so the donor reloads holding every row it started with.
        await snapshots.DidNotReceive().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        // And the resume intent IS durable, which is what lets the interrupted
        // division be finished rather than restarted: the pivot was persisted
        // before the first row moved, so recovery re-derives the same cut.
        Assert.That(
            state.State.SplitState,
            Is.EqualTo(SplitState.SplitInProgress),
            "the donor must reload knowing a division was underway");
        Assert.That(
            state.State.SplitKey,
            Is.Not.Null,
            "the pivot must be durable before any row moves, so a resumed division cuts in the "
            + "same place as the interrupted one");
    }

    [Test]
    public async Task A_resumed_division_after_an_interruption_loses_and_duplicates_nothing()
    {
        var (grain, _, recorder, _) = await RehydratedDividableLeafAsync(rowCount: 1024);
        recorder.FailOnBatchIndex = 2;

        Assert.That(async () => await InvokeSplit(grain), Throws.InstanceOf<OutOfMemoryException>());

        var transferredBeforeInterruption = recorder.AllTransferredKeys.Distinct().ToHashSet();
        Assert.That(transferredBeforeInterruption, Is.Not.Empty);

        // Resume. The donor's own residue is the cursor: rows already migrated
        // and removed are simply not seen again, and any row migrated but not
        // yet removed is re-sent into an idempotent last-writer-wins merge.
        recorder.FailOnBatchIndex = -1;
        await InvokeSplit(grain);

        var union = recorder.AllTransferredKeys.Distinct().ToHashSet();
        union.UnionWith(grain.CacheForTest.Keys);

        Assert.That(
            union.Count,
            Is.EqualTo(1024),
            "every key must survive the interruption, on one side or the other");
    }

    // ---------------------------------------------------------------
    // Arm 5 - the rented invariant.
    //
    // The crash-safety argument for batching does not stand on its own. It
    // rents an ordering maintained in code this change does not touch: the
    // parent is not told the sibling exists until the division completes, so
    // while a division is in flight every read still routes to the donor,
    // which still holds every row. That is what makes a partially-transferred
    // donor unobservable rather than merely undurable.
    //
    // Pinned here rather than argued in a PR body, because an invariant that
    // only a review comment depends on is an unenforced assumption. If the
    // publication ordering is ever changed by someone who has not read that
    // argument, this arm fails and names the argument in its message - which
    // is the only way the dependency is discoverable from the failure alone.
    // ---------------------------------------------------------------

    [Test]
    public async Task The_parent_is_not_told_the_sibling_exists_until_every_row_has_moved()
    {
        var (grain, _, recorder, _) = await RehydratedDividableLeafAsync(rowCount: 1024);
        recorder.FailOnBatchIndex = 2;

        // SplitResult is the sole channel by which the parent learns a sibling
        // exists: it carries PromotedKey and NewSiblingId, and the caller
        // inserts the child into the parent from them. An interrupted division
        // produces none, so the parent is never told.
        Assert.That(
            async () => await InvokeSplit(grain),
            Throws.InstanceOf<OutOfMemoryException>(),
            "an interrupted division must not return a SplitResult - that return value is what "
            + "tells the parent the sibling exists, and the bounded-transfer fix's crash-safety "
            + "argument rents the fact that it is produced only after every row has moved");

        // Vacuity control (R2). Without this the arm would pass even if the
        // transfer had never begun, which is the state it is meant to exclude:
        // the point is that rows really were mid-flight when the parent was
        // still uninformed, not that nothing happened.
        Assert.That(
            recorder.Batches.Count,
            Is.GreaterThan(0),
            "the arm must interrupt a division that had genuinely started moving rows, or it "
            + "proves nothing about ordering");
        Assert.That(
            recorder.AllTransferredKeys.Any(),
            Is.True,
            "the sibling must actually hold migrated rows at the moment the parent is still "
            + "uninformed - that asymmetry is the invariant under test");

        // Positive control. A zero above means something only if the same
        // channel is demonstrably non-empty when the division does complete.
        var (completing, _, completingRecorder, _) = await RehydratedDividableLeafAsync(rowCount: 1024);
        completingRecorder.FailOnBatchIndex = -1;
        var result = await InvokeSplit(completing);

        Assert.That(
            result.NewSiblingId,
            Is.Not.Null,
            "a completed division must tell the parent the sibling exists, or the assertion above "
            + "is vacuous rather than load-bearing");
        Assert.That(
            result.PromotedKey,
            Is.Not.Null.And.Not.Empty,
            "a completed division must promote its pivot to the parent");
    }
}
