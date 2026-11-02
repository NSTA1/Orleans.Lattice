using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for member 2's additions to a leaf activation's append fence (epic
/// #5605; design owner's answer Q1): a hand-off close raises the activation's own
/// epoch, conditionally on its own row, and re-claims it at every partition it had
/// claimed, returning the heads; an append stamped below the activation's own
/// current epoch is self-fenced - refused for its caller to re-admit, without the
/// activation deactivating (#5643).
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    [Test]
    public async Task A_hand_off_close_raises_the_epoch_and_re_claims_every_claimed_partition()
    {
        var h = new Harness();
        h.Shard.ClaimAppendEpochAsync(Leaf, 2, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(17)));
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.Fence.StampForAsync(h.Shard, partition: 2, CancellationToken.None);

        var heads = await h.Fence.BumpAndReclaimAsync(_ => h.Shard, partitionCount: 3, CancellationToken.None);
        var rowEpoch = await h.RowEpochAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rowEpoch, Is.EqualTo(2), "the bump persists the row plus one before the epoch moves");
            Assert.That(h.Fence.Epoch, Is.EqualTo(2));
            Assert.That(heads, Is.EqualTo(new long[] { 17, -1, 17 }),
                "every partition it had claimed is re-claimed; one it never claimed holds none of its stamped appends");
            Assert.That(h.Superseded, Is.Empty);
        });
        await h.Shard.Received(2).ClaimAppendEpochAsync(Leaf, 2, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_hand_off_close_with_nothing_stamped_returns_no_heads()
    {
        var h = new Harness();

        var heads = await h.Fence.BumpAndReclaimAsync(_ => h.Shard, partitionCount: 2, CancellationToken.None);

        Assert.That(heads, Is.Null, "an activation that never claimed appended unstamped, which the watermark reports");
    }

    [Test]
    public async Task A_hand_off_close_after_another_activation_claimed_supersedes_rather_than_fences()
    {
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.ClaimByAnotherActivationAsync();

        var ex = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(
            () => h.Fence.BumpAndReclaimAsync(_ => h.Shard, partitionCount: 1, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ActivationSuperseded, Is.True);
            Assert.That(h.Superseded, Has.Count.EqualTo(1), "the bump is conditional on the row this activation last wrote");
        });
    }

    [Test]
    public async Task An_append_stamped_below_the_activations_own_epoch_is_self_fenced_and_does_not_deactivate()
    {
        // #5643: a stale stamp below the activation's own current epoch was fenced by
        // the activation itself (a hand-off close, or a bump after a floor refusal).
        var h = new Harness();
        var old = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.Fence.BumpAndReclaimAsync(_ => h.Shard, partitionCount: 1, CancellationToken.None);

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
    public async Task A_floor_refusal_concurrent_with_a_hand_off_close_joins_its_bump_and_does_not_supersede()
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
        var close = fence.BumpAndReclaimAsync(p => p == 0 ? claimed : floored, partitionCount: 2, CancellationToken.None);
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
