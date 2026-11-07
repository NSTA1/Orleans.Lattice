using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for a leaf activation's split fence (epic #5605, member 2; #5841, rule A):
/// a split close raises the leaf's epoch row above the value it reads fresh, claims the
/// raised epoch at every partition and returns the heads, and the live activation adopts
/// the raised row, epoch and claims; an append stamped below the activation's own current
/// epoch is self-fenced - refused for its caller to re-admit, without the activation
/// deactivating (#5643).
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    [Test]
    public async Task A_split_fence_raises_the_epoch_and_claims_it_at_every_partition()
    {
        var h = new Harness();
        h.Shard.ClaimAppendEpochAsync(Leaf, 2, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(17)));
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.Fence.StampForAsync(h.Shard, partition: 2, CancellationToken.None);

        var heads = await h.Fence.FenceSplitAsync(_ => h.Shard, partitionCount: 3, CancellationToken.None);
        var rowEpoch = await h.RowEpochAsync();
        var adopted = h.Fence.StampForAsync(h.Shard, partition: 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(rowEpoch, Is.EqualTo(2), "the fence persists the row plus one");
            Assert.That(h.Fence.Epoch, Is.EqualTo(2), "the live activation adopts the raised epoch");
            Assert.That(heads, Is.EqualTo(new long[] { 17, 17, 17 }),
                "the raised epoch is claimed at every partition, so a duplicate is refused even where this activation never wrote");
            Assert.That(adopted.IsCompletedSuccessfully && adopted.Result?.Epoch == 2, Is.True, "the fence's claims are adopted");
            Assert.That(h.Superseded, Is.Empty);
        });
        await h.Shard.Received(3).ClaimAppendEpochAsync(Leaf, 2, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_split_fence_when_no_activation_ever_claimed_returns_no_heads_and_counts_the_close()
    {
        var h = new Harness();
        var closes = h.Fence.HandOffCloses;

        var heads = await h.Fence.FenceSplitAsync(_ => h.Shard, partitionCount: 2, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(heads, Is.Null, "no activation claimed, so every append was unstamped, which the watermark reports");
            Assert.That(h.Fence.HandOffCloses, Is.EqualTo(closes + 1), "a write admitted before the narrowing must still self-fence");
            Assert.That(await h.RowEpochAsync(), Is.Zero, "a fence with nothing to fence wrote the row");
        });
        await h.Shard.DidNotReceiveWithAnyArgs().ClaimAppendEpochAsync(default, default, default);
    }

    [Test]
    public async Task A_split_fence_after_another_activation_claimed_raises_above_it_rather_than_superseding()
    {
        // #5841: the split runs on the live donor, whose narrowing is durable; a claim by
        // another activation is a duplicate's, which the fence outranks, not a supersession.
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.ClaimByAnotherActivationAsync();

        var heads = await h.Fence.FenceSplitAsync(_ => h.Shard, partitionCount: 1, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(heads, Is.Not.Null);
            Assert.That(await h.RowEpochAsync(), Is.EqualTo(3), "the fence read the row fresh and raised above the duplicate's claim");
            Assert.That(h.Fence.Epoch, Is.EqualTo(3));
            Assert.That(h.Superseded, Is.Empty);
        });
    }

    [Test]
    public async Task An_append_stamped_below_the_activations_own_epoch_is_self_fenced_and_does_not_deactivate()
    {
        // #5643: a stale stamp below the activation's own current epoch was fenced by
        // the activation itself (a hand-off close, or a bump after a floor refusal).
        var h = new Harness();
        var old = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.Fence.FenceSplitAsync(_ => h.Shard, partitionCount: 1, CancellationToken.None);

        var refusal = h.Fence.OnStale(old!.Value);
        var next = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(refusal.ActivationSuperseded, Is.True,
                "refused with the 10.2.0-retriable shape, so every seam re-admits the write under the current epoch");
            Assert.That(h.Superseded, Is.Empty, "the activation is not superseded and keeps serving");
            Assert.That(h.Fence.IsStale, Is.False);
            Assert.That(next?.Epoch, Is.EqualTo(2));
        });
    }

    /// <summary>
    /// A storage that captures a write's state and ETag when the write is issued, as a
    /// real provider serializes them before its I/O, and holds every write of the
    /// leaf's epoch row behind <see cref="Gate"/> once armed.
    /// </summary>
    private sealed class CallTimeEtagStorage(EnumerableMemoryGrainStorage inner) : Orleans.Storage.IGrainStorage
    {
        private int _pending;

        public TaskCompletionSource? Gate { get; set; }

        public int PendingWrites => Volatile.Read(ref _pending);

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ReadStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ClearStateAsync(stateName, grainId, grainState);

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (Gate is not { } gate || grainState is not IGrainState<Orleans.Lattice.BPlusTree.State.LeafAppendEpochState> row)
            {
                await inner.WriteStateAsync(stateName, grainId, grainState);
                return;
            }

            var issued = new GrainState<Orleans.Lattice.BPlusTree.State.LeafAppendEpochState>
            {
                State = new Orleans.Lattice.BPlusTree.State.LeafAppendEpochState { Epoch = row.State.Epoch },
                ETag = row.ETag,
                RecordExists = row.RecordExists,
            };
            Interlocked.Increment(ref _pending);
            try
            {
                await gate.Task;
                await inner.WriteStateAsync(stateName, grainId, issued);
                row.ETag = issued.ETag;
                row.RecordExists = issued.RecordExists;
            }
            finally
            {
                Interlocked.Decrement(ref _pending);
            }
        }
    }

    [Test]
    public async Task A_floor_refusal_concurrent_with_a_split_fence_joins_its_bump_and_does_not_supersede()
    {
        // #5662: the close's row write and a floor-refusal bump must not both write the
        // activation's row, or one conflicts on the activation's own ETag and reads as
        // a supersession (the #5643 class, from the other side).
        var storage = new CallTimeEtagStorage(new EnumerableMemoryGrainStorage());
        var superseded = new List<long>();
        var fence = new LeafAppendEpochFence(
            Leaf,
            new SettableGate(open: true),
            storage,
            superseded.Add,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree"),
            LatticeTenantLabel.ForTree("tree"));
        var claimed = Substitute.For<IWalShardGrain>();
        claimed.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(11)));
        var floored = Substitute.For<IWalShardGrain>();
        floored.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(call.ArgAt<long>(1) <= 5
                ? WalAppendFenceClaimResult.Refuse(5)
                : WalAppendFenceClaimResult.Accept(23)));
        await fence.StampForAsync(claimed, partition: 0, CancellationToken.None);

        storage.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var close = fence.FenceSplitAsync(p => p == 0 ? claimed : floored, partitionCount: 2, CancellationToken.None);
        await WaitForAsync(() => storage.PendingWrites >= 1);
        var refused = fence.StampForAsync(floored, partition: 1, CancellationToken.None).AsTask();
        await WaitForAsync(() => storage.PendingWrites >= 2, TimeSpan.FromMilliseconds(250));
        var overlapped = storage.PendingWrites;
        storage.Gate.SetResult();

        Exception? closeFault = null;
        long[]? heads = null;
        try
        {
            heads = await close;
        }
        catch (Exception ex)
        {
            closeFault = ex;
        }

        WalAppendStamp? stamp = null;
        Exception? stampFault = null;
        try
        {
            stamp = await refused;
        }
        catch (Exception ex)
        {
            stampFault = ex;
        }

        storage.Gate = null;
        var rowEpoch = (await WalAppendFenceRows.ReadLeafEpochAsync(storage, Leaf)).State.Epoch;
        Assert.Multiple(() =>
        {
            Assert.That(overlapped, Is.EqualTo(1), "the refusal joins the close's bump instead of writing the row beside it");
            Assert.That(superseded, Is.Empty, "neither write conflicts on the activation's own ETag");
            Assert.That(fence.IsStale, Is.False);
            Assert.That(closeFault, Is.Null);
            Assert.That(stampFault, Is.Null);
            Assert.That(heads, Is.Not.Null);
            Assert.That(stamp?.Epoch, Is.EqualTo(6), "the refusal claims again above the floor, once the close's raise has landed");
            Assert.That(rowEpoch, Is.EqualTo(fence.Epoch), "the durable row is the epoch the activation holds");
        });
    }

    private static async Task WaitForAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5);
        }
    }

    [Test]
    public async Task A_write_admitted_before_a_split_fence_and_dispatched_after_it_is_self_fenced_not_appended()
    {
        // #5664 (a): the stamp is decided at admission. A write admitted before the
        // donor's split narrowed must not reach the WAL under the close's raised epoch,
        // where it would be allocated above the heads the close records.
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        var admittedBefore = WalRoutingStamp.None with { AdmissionMark = h.Fence.HandOffCloses + 1 };
        await h.Fence.FenceSplitAsync(_ => h.Shard, partitionCount: 1, CancellationToken.None);
        var admittedAfter = WalRoutingStamp.None with { AdmissionMark = h.Fence.HandOffCloses + 1 };
        h.Shard.AppendStampedAsync(Arg.Any<WalRecord>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalStampedAppendResult(9, -1, WalAppendFenceOutcome.Admitted)));

        var refused = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(async () =>
            await WalFencedAppendDispatch.AppendPointAsync(
                h.Shard, new WalRecord(), batched: false, partition: 0, h.Fence, admittedBefore, waitForDeadline: false, CancellationToken.None));
        await h.Shard.DidNotReceiveWithAnyArgs().AppendStampedAsync(default!, default, default);
        var appended = await WalFencedAppendDispatch.AppendPointAsync(
            h.Shard, new WalRecord(), batched: false, partition: 0, h.Fence, admittedAfter, waitForDeadline: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(refused!.ActivationSuperseded, Is.True, "refused with the shape every seam re-admits");
            Assert.That(h.Fence.IsStale, Is.False, "a self-fence, not a supersession");
            Assert.That(h.Superseded, Is.Empty);
            Assert.That(appended, Is.EqualTo(9), "the re-admitted write appends under the raised epoch");
            Assert.That(h.Fence.OnAdmitted(0), Is.Null, "a write whose admission was not captured is stamped at dispatch");
        });
    }

    [Test]
    public async Task A_split_fence_claims_a_partition_whose_claim_a_wal_shard_restart_reset()
    {
        // #5664 (b): the activation stamped appends at partition 1, then the partition
        // restarted and an unloaded rejection reset the claim. The fence must still
        // record partition 1's real head, never -1.
        var h = new Harness();
        var second = Substitute.For<IWalShardGrain>();
        h.Shard.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(17)));
        second.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(29)));
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        var stamped = await h.Fence.StampForAsync(second, partition: 1, CancellationToken.None);
        h.Fence.OnUnloaded(1, stamped!.Value);

        var heads = await h.Fence.FenceSplitAsync(p => p == 0 ? h.Shard : second, partitionCount: 3, CancellationToken.None);

        Assert.That(heads, Is.EqualTo(new long[] { 17, 29, 29 }),
            "every partition is claimed under the raised epoch, including one whose claim was reset");
        await second.Received(2).ClaimAppendEpochAsync(Leaf, 2, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_split_fence_with_the_gate_view_closed_after_stamping_leaves_the_marker_open()
    {
        // #5664 (c): a stamped append may still be in flight, and the close cannot claim,
        // so it must not return unfenced heads for the caller to close the marker on.
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        h.Fence.OnClaimsExhausted();

        Assert.ThrowsAsync<InvalidOperationException>(
            () => h.Fence.FenceSplitAsync(_ => h.Shard, partitionCount: 1, CancellationToken.None));

        var neverStamped = new Harness();
        neverStamped.Gate.IsOpen = false;
        Assert.That(
            await neverStamped.Fence.FenceSplitAsync(_ => neverStamped.Shard, partitionCount: 1, CancellationToken.None),
            Is.Null,
            "only an activation that never stamped closes on unfenced heads");
        Assert.That(h.Superseded, Is.Empty);
    }

    [Test]
    public async Task A_current_epoch_append_rejected_as_stale_still_supersedes()
    {
        var h = new Harness();
        var current = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        h.Fence.OnStale(current!.Value);

        Assert.Multiple(() =>
        {
            Assert.That(h.Fence.IsStale, Is.True, "a genuine supersession meets the current epoch");
            Assert.That(h.Superseded, Has.Count.EqualTo(1));
        });
    }
}
