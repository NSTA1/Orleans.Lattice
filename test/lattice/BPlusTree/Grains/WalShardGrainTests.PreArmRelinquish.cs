using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for #5840 review S4 and a5e2e2ef's conditions R1-R4: in pre-arm the shard root
/// fences a split's donor at link time, raising the leaf's entry above the live donor's
/// epoch. The live donor relinquishes its epoch and claims at its counted split close, so it
/// keeps serving: its next appends go pre-arm, its next arm re-claims through the probed
/// claim, and one allowance per close turns a refusal at its current epoch - an arm that
/// raced the link - into the same re-claim instead of a supersession. A duplicate counted
/// no close and still supersedes; a re-claim whose leaf state moved on goes stale.
/// </summary>
public partial class WalShardGrainTests
{
    private static Task<(long Head, long Generation)?[]> ArmAtAsync(DonorActivation activation, WalShardGrain wal, int partition = 0) =>
        activation.Fence.ArmAsync(_ => wal, [(partition, WalRoutingInitiator.None, 0L)], CancellationToken.None);

    /// <summary>A routed write under a shard root's generation, through the production dispatch.</summary>
    private static Task<long> RoutedWriteAsync(WalShardGrain wal, DonorActivation activation, string key = "own") =>
        WalFencedAppendDispatch.AppendPointAsync(
            wal,
            MakeEntry(key),
            batched: false,
            partition: 0,
            activation.Fence,
            WalRoutingStamp.ForShardRoot(0, 1) with { AdmissionMark = activation.Fence.HandOffCloses + 1 },
            waitForDeadline: false,
            CancellationToken.None).AsTask();

    /// <summary>The live donor's split in pre-arm: its narrowing is durable and it counts the close.</summary>
    private static async Task CloseSplitPreArmAsync(EnumerableMemoryGrainStorage storage, DonorActivation live)
    {
        await live.PersistAsync(storage, highKeyExclusive: "m");
        live.Fence.CountSplitClose();
    }

    /// <summary>The shard root's link-time fence of the donor (pre-arm).</summary>
    private static Task<long[]?> LinkFenceAsync(EnumerableMemoryGrainStorage storage, WalShardGrain wal) =>
        LeafSplitDuplicateFence.FenceAsync(storage, _ => wal, 1, FenceLeaf, null, null, CancellationToken.None);

    [Test]
    public async Task A_pre_arm_donor_keeps_stamping_after_the_link_fence()
    {
        // S4 progress: an armed live donor splits and the shard root fences it at the link.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        await ArmAtAsync(live, wal);
        await RoutedWriteAsync(wal, live, "before");
        await CloseSplitPreArmAsync(storage, live);
        await LinkFenceAsync(storage, wal);

        var preArm = await RoutedWriteAsync(wal, live, "between");
        var rearm = await ArmAtAsync(live, wal);
        var stamped = await RoutedWriteAsync(wal, live, "after");

        Assert.Multiple(() =>
        {
            Assert.That(live.Superseded, Is.Empty, "the live donor superseded itself at the link fence");
            Assert.That(live.Fence.IsStale, Is.False);
            Assert.That(preArm, Is.GreaterThanOrEqualTo(0));
            Assert.That(rearm, Has.Length.EqualTo(1).And.All.Not.Null, "the live donor could not re-arm after the link fence");
            Assert.That(live.Fence.Epoch, Is.EqualTo(3), "the re-arm did not claim above the link fence's raise");
            Assert.That(live.Fence.TryGetArmingView(0, out _, out _), Is.True);
            Assert.That(stamped, Is.GreaterThan(preArm));
        });
    }

    [Test]
    public async Task RelinquishKeepsCachedRow()
    {
        // R1: the counted close drops the cached row and epoch, so the next arm reads the row
        // afresh. Kept, the arm would present epoch 1, be refused, and bump on the cached ETag
        // the link fence changed.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        await ArmAtAsync(live, wal);
        await CloseSplitPreArmAsync(storage, live);
        var epochAfterClose = live.Fence.Epoch;
        await LinkFenceAsync(storage, wal);

        Exception? fault = null;
        try
        {
            await ArmAtAsync(live, wal);
        }
        catch (Exception ex)
        {
            fault = ex;
        }

        Assert.Multiple(() =>
        {
            Assert.That(epochAfterClose, Is.Zero, "the counted close kept the activation's epoch");
            Assert.That(fault, Is.Null, "the first arm after the link fence was refused");
            Assert.That(live.Fence.Epoch, Is.EqualTo(3));
            Assert.That(live.Superseded, Is.Empty);
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task RelinquishLateClaimReinstalls(bool bumpInFlight)
    {
        // R2: a bump, or a fresh row claim, in flight at the counted close installs nothing
        // when it lands; the next round re-claims through the probed claim instead.
        var gated = new GatedEpochRowStorage();
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        var donor = new DonorActivation(storage, live.Etag, preArm: true, fenceStorage: gated);
        gated.Inner = storage;
        if (bumpInFlight)
        {
            await ArmAtAsync(donor, wal);
            await wal.ClaimAppendEpochAsync(FenceLeaf, 5, CancellationToken.None);
        }

        gated.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arm = ArmAtAsync(donor, wal, partition: 1);
        await WaitUntilAsync(() => gated.PendingWrites == 1);
        await CloseSplitPreArmAsync(storage, donor);
        gated.Gate.SetResult();
        Exception? fault = null;
        try
        {
            await arm;
        }
        catch (Exception ex)
        {
            fault = ex;
        }

        var installed = donor.Fence.Epoch;
        var rearm = await ArmAtAsync(donor, wal, partition: 1);
        var expected = bumpInFlight ? 7 : 2;

        Assert.Multiple(() =>
        {
            Assert.That(fault is null or Orleans.Lattice.LeafRetiredException { ActivationSuperseded: true }, Is.True, $"unexpected fault: {fault}");
            Assert.That(donor.Fence.IsStale, Is.False, "the discarded claim superseded the live donor");
            Assert.That(rearm, Has.All.Not.Null);
            Assert.That(installed is 0 || installed == expected, Is.True,
                $"the {(bumpInFlight ? "bump" : "row claim")} in flight at the close installed epoch {installed}");
            Assert.That(donor.Fence.Epoch, Is.EqualTo(expected), "the next arm did not re-claim through the probed claim");
            Assert.That(donor.Superseded, Is.Empty);
        });
    }

    [Test]
    public async Task A_stale_stamp_above_the_relinquished_epoch_still_supersedes()
    {
        // R3: a stamp at or below the relinquished epoch was this activation's own and is
        // self-fenced (an older one may still be in flight after a bump); any other refusal
        // at no current epoch still supersedes.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf);
        row.State.Epoch = 2;
        await WalAppendFenceRows.WriteAsync(storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(FenceLeaf), row);
        await ArmAtAsync(live, wal);
        Assert.That(live.Fence.Epoch, Is.EqualTo(3), "precondition");
        await CloseSplitPreArmAsync(storage, live);

        var atRelinquished = live.Fence.OnStale(WalAppendStamp.ForLeaf(FenceLeaf, 3));
        live.Fence.OnStale(WalAppendStamp.ForLeaf(FenceLeaf, 2));
        var staleAfterOwn = live.Fence.IsStale;
        live.Fence.OnStale(WalAppendStamp.ForLeaf(FenceLeaf, 4));

        Assert.Multiple(() =>
        {
            Assert.That(atRelinquished.ActivationSuperseded, Is.True, "refused with the shape every seam retries");
            Assert.That(staleAfterOwn, Is.False, "a stamp at or below the relinquished epoch superseded the activation");
            Assert.That(live.Fence.IsStale, Is.True, "a stamp above the relinquished epoch, at no current epoch, was self-fenced");
            Assert.That(live.Superseded, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_claim_discarded_after_a_relinquish_that_overtook_its_row_read_is_still_recorded()
    {
        // R5: the relinquish ran after the claim read the row but before it recorded the epoch
        // it writes. The discard records that epoch in the same section, so a stamp at it is
        // still the activation's own; and a later relinquish never lowers the record.
        var gated = new GatedEpochRowStorage();
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        var donor = new DonorActivation(storage, live.Etag, preArm: true, fenceStorage: gated);
        gated.Inner = storage;

        gated.ReadGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arm = ArmAtAsync(donor, wal);
        await WaitUntilAsync(() => gated.PendingReads == 1);
        await CloseSplitPreArmAsync(storage, donor);
        gated.ReadGate.SetResult();
        gated.ReadGate = null;
        var discarded = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => arm);
        donor.Fence.CountSplitClose();

        donor.Fence.OnStale(WalAppendStamp.ForLeaf(FenceLeaf, 1));

        Assert.Multiple(() =>
        {
            Assert.That(discarded!.ActivationSuperseded, Is.True, "precondition: the claim was discarded with the self-fence shape");
            Assert.That(donor.Fence.IsStale, Is.False, "the discarded claim's epoch was not recorded as relinquished, or a later relinquish lowered the record");
            Assert.That(donor.Superseded, Is.Empty);
        });
    }

    [Test]
    public async Task A_discarded_claim_whose_leaf_state_moved_on_still_supersedes()
    {
        // R6: discarding a claim in flight at the counted close never swallows a #5841
        // detection: its probe found the leaf state moved on, so the activation is stale.
        var gated = new GatedEpochRowStorage();
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        var donor = new DonorActivation(storage, live.Etag, preArm: true, fenceStorage: gated);
        gated.Inner = storage;

        gated.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arm = ArmAtAsync(donor, wal);
        await WaitUntilAsync(() => gated.PendingWrites == 1);
        donor.Fence.CountSplitClose();
        var moved = new GrainState<LeafNodeState>(new LeafNodeState { TreeId = TreeId }) { ETag = donor.Etag, RecordExists = true };
        await storage.WriteStateAsync(LeafStateCurrencyProbe.LeafStateName, DonorStateId, moved);
        gated.Gate.SetResult();

        Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => arm);
        Assert.Multiple(async () =>
        {
            Assert.That(donor.Fence.IsStale, Is.True, "the discard swallowed the probe's stale verdict");
            Assert.That(donor.Superseded, Has.Count.EqualTo(1));
            Assert.That(await DurableEntryAsync(storage, FenceLeaf), Is.Zero, "the stale claim claimed at a partition");
        });
    }

    [Test]
    public async Task A_leaf_that_never_persisted_probes_as_current_and_claims()
    {
        // A leaf with no state row yet (a null ETag in storage and in the activation) is the
        // version the activation holds: its claim must not read it as moved on.
        var storage = new EnumerableMemoryGrainStorage();
        var wal = await CreateFencedGrainAsync(storage);
        var fresh = new DonorActivation(storage, etag: null, preArm: false);

        var stamp = await fresh.Fence.StampForAsync(wal, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stamp?.Epoch, Is.EqualTo(1), "a never-persisted leaf's claim was refused");
            Assert.That(fresh.Fence.IsStale, Is.False);
            Assert.That(fresh.Superseded, Is.Empty);
        });
    }

    [Test]
    public async Task A_floor_refusal_of_a_write_stamped_before_the_relinquish_self_fences_and_never_bumps()
    {
        // R7 (676f389a): the write's claim round ran under epoch 1, the counted close
        // relinquished it, and the round's refusal then lands. It self-fences for its seam to
        // re-admit; it neither bumps the row on the relinquished epoch nor claims afresh in
        // the same round.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: false);
        await live.Fence.StampForAsync(wal, 0, CancellationToken.None);
        var refusal = new TaskCompletionSource<WalAppendFenceClaimResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var floored = Substitute.For<IWalShardGrain>();
        floored.ClaimAppendEpochAsync(FenceLeaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(call => call.ArgAt<long>(1) == 1 ? refusal.Task : Task.FromResult(WalAppendFenceClaimResult.Accept(0)));

        var write = live.Fence.StampForAsync(floored, 1, CancellationToken.None).AsTask();
        await live.PersistAsync(storage, highKeyExclusive: "m");
        live.Fence.CountSplitClose();
        refusal.SetResult(WalAppendFenceClaimResult.Refuse(5));

        var fenced = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => write);
        Assert.Multiple(async () =>
        {
            Assert.That(fenced!.ActivationSuperseded, Is.True, "refused with the shape every seam retries");
            Assert.That(live.Fence.IsStale, Is.False);
            Assert.That(live.Superseded, Is.Empty);
            Assert.That((await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf)).State.Epoch, Is.EqualTo(1),
                "the refusal of a round under the relinquished epoch bumped the row or claimed afresh");
        });
    }

    [Test]
    public async Task A_floor_refusal_at_the_current_epoch_above_the_relinquished_one_still_bumps()
    {
        // R8, the other side: after relinquishing epoch 1 the activation re-claims afresh at 2;
        // a refusal of a round under that current epoch still bumps, as before S4.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: false);
        await live.Fence.StampForAsync(wal, 0, CancellationToken.None);
        await live.PersistAsync(storage, highKeyExclusive: "m");
        live.Fence.CountSplitClose();
        var reclaimed = await live.Fence.StampForAsync(wal, 0, CancellationToken.None);
        await wal.ClaimAppendEpochAsync(FenceLeaf, 5, CancellationToken.None);

        var bumped = await live.Fence.StampForAsync(wal, 1, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(reclaimed?.Epoch, Is.EqualTo(2), "precondition: a fresh claim above the relinquished epoch");
            Assert.That(bumped?.Epoch, Is.EqualTo(6), "a refusal at the current epoch did not bump");
            Assert.That((await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf)).State.Epoch, Is.EqualTo(6));
            Assert.That(live.Fence.IsStale, Is.False);
            Assert.That(live.Superseded, Is.Empty);
        });
    }

    [Test]
    public async Task RelinquishedAppendRestamped()
    {
        // An append stamped at the epoch the counted close relinquished, dispatched after the
        // link fence, is refused allocating nothing and self-fenced for its seam to re-admit,
        // never appended under that epoch.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        await ArmAtAsync(live, wal);
        var stamp = await live.Fence.StampForAsync(wal, 0, CancellationToken.None);
        await CloseSplitPreArmAsync(storage, live);
        await LinkFenceAsync(storage, wal);
        var before = await wal.GetNextSequenceAsync(CancellationToken.None);

        var refused = await wal.AppendStampedAsync(MakeEntry("in-flight"), stamp!.Value, CancellationToken.None);
        var fenced = live.Fence.OnStale(stamp.Value);

        Assert.Multiple(async () =>
        {
            Assert.That(stamp.Value.IsPreArm, Is.False, "precondition: the append was stamped under the armed epoch");
            Assert.That(refused.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleEpoch));
            Assert.That(await wal.GetNextSequenceAsync(CancellationToken.None), Is.EqualTo(before), "the relinquished-epoch append was allocated");
            Assert.That(fenced.ActivationSuperseded, Is.True);
            Assert.That(live.Fence.IsStale, Is.False, "the relinquished-epoch refusal superseded the live donor");
            Assert.That(live.Superseded, Is.Empty);
        });
    }

    [Test]
    public async Task The_reclaim_allowance_is_one_per_close_and_never_accumulates()
    {
        // R4: two counted closes leave one allowance; a refusal at the current epoch spends
        // it, and the next such refusal supersedes.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        await ArmAtAsync(live, wal);
        await CloseSplitPreArmAsync(storage, live);
        live.Fence.CountSplitClose();
        var allowance = live.Fence.ReclaimAllowanceForTesting;

        await ArmAtAsync(live, wal);
        var first = live.Fence.Epoch;
        live.Fence.OnStale(WalAppendStamp.ForLeaf(FenceLeaf, first));
        var staleAfterFirst = live.Fence.IsStale;
        await ArmAtAsync(live, wal);
        live.Fence.OnStale(WalAppendStamp.ForLeaf(FenceLeaf, live.Fence.Epoch));

        Assert.Multiple(() =>
        {
            Assert.That(allowance, Is.EqualTo(1), "the allowance accumulated across closes");
            Assert.That(staleAfterFirst, Is.False, "the allowance did not cover a refusal at the current epoch");
            Assert.That(live.Fence.IsStale, Is.True, "a second refusal at the current epoch was covered by a spent allowance");
        });
    }

    [Test]
    public async Task A_claim_row_conflict_never_spends_the_allowance()
    {
        // R4: the allowance covers a bump's conflict, never a fresh claim's, which still
        // means another activation claimed between its read and its write.
        var gated = new GatedEpochRowStorage();
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        var donor = new DonorActivation(storage, live.Etag, preArm: true, fenceStorage: gated);
        gated.Inner = storage;
        await ArmAtAsync(donor, wal);
        await CloseSplitPreArmAsync(storage, donor);

        gated.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arm = ArmAtAsync(donor, wal);
        await WaitUntilAsync(() => gated.PendingWrites == 1);
        var other = await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf);
        other.State.Epoch++;
        await WalAppendFenceRows.WriteAsync(storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(FenceLeaf), other);
        gated.Gate.SetResult();

        Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => arm);
        Assert.Multiple(() =>
        {
            Assert.That(donor.Fence.IsStale, Is.True, "a fresh claim's conflict spent the allowance instead of superseding");
            Assert.That(donor.Superseded, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task StaleEpochReclaimSkipsProbe()
    {
        // 676f389a's HandOffBoundStaleEpochReclaimSkipsProbe: the allowance's re-claim is the
        // probed claim. Here the donor re-armed between its close and the link fence, and
        // its leaf state moved on before the fence: the refusal at its current epoch is
        // self-fenced, and the re-claim that follows goes stale and claims nowhere.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        await ArmAtAsync(live, wal);
        await CloseSplitPreArmAsync(storage, live);
        await ArmAtAsync(live, wal);
        var moved = new GrainState<LeafNodeState>(new LeafNodeState { TreeId = TreeId }) { ETag = live.Etag, RecordExists = true };
        await storage.WriteStateAsync(LeafStateCurrencyProbe.LeafStateName, DonorStateId, moved);
        await LinkFenceAsync(storage, wal);
        var entry = await DurableEntryAsync(storage, FenceLeaf);

        var refused = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => RoutedWriteAsync(wal, live));
        var staleAfterRefusal = live.Fence.IsStale;
        var reclaim = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => ArmAtAsync(live, wal));

        Assert.Multiple(async () =>
        {
            Assert.That(refused!.ActivationSuperseded, Is.True);
            Assert.That(staleAfterRefusal, Is.False, "precondition: the refusal at the current epoch spent the allowance");
            Assert.That(reclaim!.ActivationSuperseded, Is.True);
            Assert.That(live.Fence.IsStale, Is.True, "the allowance's re-claim did not run the leaf-state probe");
            Assert.That(await DurableEntryAsync(storage, FenceLeaf), Is.EqualTo(entry), "a stale re-claim claimed at the partition");
        });
    }
}
