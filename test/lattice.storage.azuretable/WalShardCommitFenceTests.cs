using Azure;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Unit tests for <see cref="WalShardCommitFence"/> (issue #5810), and the invariant it
/// holds with the shard's <see cref="WalShardOverlapGuard"/>: while the fence holds an
/// ETag and the guard is bounded, the guard's bound covers the <c>TAIL</c> value that
/// ETag was observed with, so no claim the guard admits can start at or below it.
/// </summary>
[TestFixture]
public sealed class WalShardCommitFenceTests
{
    [Test]
    public void No_claim_is_admitted_while_the_fence_holds_no_etag()
    {
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        guard.RaiseBound(3L);

        Assert.Multiple(() =>
        {
            Assert.That(fence.TryClaimAboveWritten(guard, 4L, 5L, out _), Is.False);
            Assert.That(fence.TryGetGeneration(out _), Is.False);
        });
    }

    [Test]
    public void A_fill_raises_a_bounded_guard_to_the_observed_tail()
    {
        // The guard was bounded at 3 by an earlier read; another writer has since
        // committed up to 9. A claim at 4 under the fence filled at 9 would commit
        // conditionally on an ETag that already covers offsets 4..9.
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        guard.RaiseBound(3L);

        fence.Fill(guard, new ETag("e9"), tail: 9L, writtenUpperBound: null);

        Assert.Multiple(() =>
        {
            Assert.That(fence.TryClaimAboveWritten(guard, 4L, 5L, out _), Is.False, "a claim at or below the observed TAIL was admitted");
            Assert.That(fence.TryClaimAboveWritten(guard, 10L, 11L, out _), Is.True);
        });
    }

    [Test]
    public void A_read_of_tail_alone_leaves_an_unbounded_guard_unbounded()
    {
        // Bounding the guard needs the batch partitions above TAIL too; only the
        // overlap guard's own bound read supplies them.
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();

        fence.Fill(guard, new ETag("e9"), tail: 9L, writtenUpperBound: null);

        Assert.That(guard.IsBounded, Is.False);
    }

    [Test]
    public void A_bound_read_older_than_the_held_etag_is_raised_to_the_fenced_tail()
    {
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        fence.Fill(guard, new ETag("e9"), tail: 9L, writtenUpperBound: null);

        // A bound read that started before the fill completes after it with an older TAIL.
        var generation = fence.Fill(guard, new ETag("e3"), tail: 3L, writtenUpperBound: 3L);

        Assert.Multiple(() =>
        {
            Assert.That(fence.TryGetCondition(generation, out var etag, out var tail), Is.True);
            Assert.That(etag, Is.EqualTo(new ETag("e9")), "a read replaced the held ETag");
            Assert.That(tail, Is.EqualTo(9L));
            Assert.That(fence.TryClaimAboveWritten(guard, 4L, 5L, out _), Is.False, "the guard's bound is below the fenced TAIL");
        });
    }

    [Test]
    public void A_claim_carries_the_generation_it_was_admitted_under()
    {
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        var filled = fence.Fill(guard, new ETag("e3"), tail: 3L, writtenUpperBound: 3L);

        Assert.Multiple(() =>
        {
            Assert.That(fence.TryClaimAboveWritten(guard, 4L, 5L, out var generation), Is.True);
            Assert.That(generation, Is.EqualTo(filled));
        });
    }

    [Test]
    public void Invalidation_fences_every_earlier_generation()
    {
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        var stale = fence.Fill(guard, new ETag("e3"), tail: 3L, writtenUpperBound: 3L);

        fence.Invalidate();
        var current = fence.Fill(guard, new ETag("e7"), tail: 7L, writtenUpperBound: 7L);

        Assert.Multiple(() =>
        {
            Assert.That(current, Is.Not.EqualTo(stale));
            Assert.That(fence.TryGetCondition(stale, out _, out _), Is.False);
            Assert.That(fence.TryGetCondition(current, out _, out _), Is.True);
        });
    }

    [Test]
    public void Advance_records_the_landed_etag_only_for_the_current_generation()
    {
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        var stale = fence.Fill(guard, new ETag("e3"), tail: 3L, writtenUpperBound: 3L);
        fence.Invalidate();
        var current = fence.Fill(guard, new ETag("e7"), tail: 7L, writtenUpperBound: 7L);

        fence.Advance(stale, new ETag("late"), tail: 5L);
        fence.Advance(current, new ETag("e9"), tail: 9L);

        Assert.Multiple(() =>
        {
            Assert.That(fence.TryGetCondition(current, out var etag, out var tail), Is.True);
            Assert.That(etag, Is.EqualTo(new ETag("e9")));
            Assert.That(tail, Is.EqualTo(9L));
        });
    }

    [Test]
    public void Reset_fences_earlier_claims_and_resets_the_guard()
    {
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        var stale = fence.Fill(guard, new ETag("e3"), tail: 3L, writtenUpperBound: 3L);

        fence.Reset(guard, writtenUpperBound: null);

        Assert.Multiple(() =>
        {
            Assert.That(fence.TryGetGeneration(out var generation), Is.False);
            Assert.That(generation, Is.Not.EqualTo(stale));
            Assert.That(guard.IsBounded, Is.False);
        });
    }
}
