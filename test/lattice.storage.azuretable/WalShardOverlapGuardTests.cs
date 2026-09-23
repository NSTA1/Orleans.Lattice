namespace Orleans.Lattice.Storage.AzureTable.Tests;

[TestFixture]
public sealed class WalShardOverlapGuardTests
{
    [Test]
    public void An_unbounded_guard_never_takes_the_fast_path()
    {
        var guard = new WalShardOverlapGuard();

        Assert.Multiple(() =>
        {
            Assert.That(guard.IsBounded, Is.False);
            Assert.That(guard.TryClaimAboveWritten(0L, 3L), Is.False);
        });
    }

    [Test]
    public void The_fast_path_admits_only_batches_above_the_written_bound()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(2L);

        Assert.Multiple(() =>
        {
            Assert.That(guard.TryClaimAboveWritten(2L, 4L), Is.False, "A batch starting at the bound may overlap.");
            Assert.That(guard.TryClaimAboveWritten(3L, 4L), Is.True);
            Assert.That(guard.TryClaimAboveWritten(4L, 6L), Is.False, "An admitted batch raises the bound to its end.");
            Assert.That(guard.TryClaimAboveWritten(5L, 6L), Is.True);
        });
    }

    [Test]
    public void Claim_rejects_overlap_with_an_in_motion_batch_at_a_different_start()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(-1L);
        Assert.That(guard.TryClaimAboveWritten(0L, 2L), Is.True);

        var outcome = guard.Claim(2L, 3L, out var conflictingStart);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(WalShardOverlapGuard.ClaimOutcome.Conflict));
            Assert.That(conflictingStart, Is.EqualTo(0L));
        });
    }

    [Test]
    public void Claim_leaves_a_same_start_re_append_to_the_storage_check()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(-1L);
        Assert.That(guard.TryClaimAboveWritten(0L, 2L), Is.True);

        Assert.That(
            guard.Claim(0L, 2L, out _),
            Is.EqualTo(WalShardOverlapGuard.ClaimOutcome.NeedsStorageCheck));
    }

    [Test]
    public void A_released_batch_no_longer_conflicts_but_still_routes_through_storage()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(-1L);
        Assert.That(guard.TryClaimAboveWritten(0L, 2L), Is.True);

        guard.Release(0L, 2L);

        Assert.That(
            guard.Claim(2L, 3L, out _),
            Is.EqualTo(WalShardOverlapGuard.ClaimOutcome.NeedsStorageCheck));
    }

    [Test]
    public void Claim_above_the_bound_needs_no_storage_check()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(4L);

        Assert.That(
            guard.Claim(5L, 6L, out _),
            Is.EqualTo(WalShardOverlapGuard.ClaimOutcome.AboveWritten));
    }

    [Test]
    public void RaiseBound_never_lowers_the_bound()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(9L);
        guard.RaiseBound(3L);

        Assert.That(guard.TryClaimAboveWritten(5L, 6L), Is.False);
    }

    [Test]
    public void Reset_to_a_bound_keeps_in_motion_batches_covered()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(-1L);
        Assert.That(guard.TryClaimAboveWritten(0L, 9L), Is.True);

        guard.Reset(5L);

        Assert.Multiple(() =>
        {
            Assert.That(guard.IsBounded, Is.True);
            Assert.That(guard.TryClaimAboveWritten(6L, 7L), Is.False, "The in-motion batch still ends at 9.");
            Assert.That(guard.TryClaimAboveWritten(10L, 11L), Is.True);
        });
    }

    [Test]
    public void Reset_lowers_the_bound_once_nothing_is_in_motion()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(9L);

        guard.Reset(1L);

        Assert.That(guard.TryClaimAboveWritten(2L, 3L), Is.True);
    }

    [Test]
    public void Reset_without_a_bound_forces_the_next_append_to_read_it()
    {
        var guard = new WalShardOverlapGuard();
        guard.RaiseBound(1L);

        guard.Reset(null);

        Assert.Multiple(() =>
        {
            Assert.That(guard.IsBounded, Is.False);
            Assert.That(guard.TryClaimAboveWritten(2L, 3L), Is.False);
        });
    }
}
