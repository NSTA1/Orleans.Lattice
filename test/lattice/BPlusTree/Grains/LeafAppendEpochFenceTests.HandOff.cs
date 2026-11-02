using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

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
