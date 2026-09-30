using Microsoft.Extensions.Time.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantPolicyEpochLedger{TSubscriber}"/>, the protocol
/// behind the tenant-policy epoch grain (issue #4030). Every wait runs on a
/// <see cref="FakeTimeProvider"/>, so a lease lapses only when the test advances the
/// clock; nothing sleeps or polls.
/// </summary>
[TestFixture]
public sealed class TenantPolicyEpochLedgerTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(10);

    private static (TenantPolicyEpochLedger<string> Ledger, FakeTimeProvider Time) PastGrace()
    {
        var time = new FakeTimeProvider();
        var ledger = new TenantPolicyEpochLedger<string>(Lease, time, Guid.NewGuid());
        time.Advance(Lease * 2);
        return (ledger, time);
    }

    private static Func<string, TenantPolicyEpoch, Task> Recording(List<(string, TenantPolicyEpoch)> log) =>
        (subscriber, epoch) =>
        {
            log.Add((subscriber, epoch));
            return Task.CompletedTask;
        };

    [Test]
    public void Constructor_null_time_provider_throws()
    {
        Assert.That(() => new TenantPolicyEpochLedger<string>(Lease, null!, Guid.NewGuid()), Throws.ArgumentNullException);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Constructor_non_positive_lease_throws(int seconds)
    {
        Assert.That(
            () => new TenantPolicyEpochLedger<string>(TimeSpan.FromSeconds(seconds), new FakeTimeProvider(), Guid.NewGuid()),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Constructor_starts_at_version_zero_of_its_incarnation_with_derived_timings()
    {
        var incarnation = Guid.NewGuid();

        var ledger = new TenantPolicyEpochLedger<string>(Lease, new FakeTimeProvider(), incarnation);

        Assert.Multiple(() =>
        {
            Assert.That(ledger.Current, Is.EqualTo(new TenantPolicyEpoch(incarnation, 0)));
            Assert.That(ledger.LeaseDuration, Is.EqualTo(Lease));
            Assert.That(ledger.AckTimeout, Is.EqualTo(TimeSpan.FromSeconds(2)));
            Assert.That(ledger.Margin, Is.EqualTo(TimeSpan.FromSeconds(1)));
            Assert.That(ledger.SubscriberCount, Is.Zero);
        });
    }

    [Test]
    public void Lease_null_subscriber_throws()
    {
        var (ledger, _) = PastGrace();

        Assert.That(() => ledger.Lease(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Lease_returns_the_current_epoch_and_duration_and_subscribes()
    {
        var (ledger, _) = PastGrace();

        var lease = ledger.Lease("silo-1");

        Assert.Multiple(() =>
        {
            Assert.That(lease.Epoch, Is.EqualTo(ledger.Current));
            Assert.That(lease.Duration, Is.EqualTo(Lease));
            Assert.That(ledger.SubscriberCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void AdvanceAsync_null_notify_throws()
    {
        var (ledger, _) = PastGrace();

        Assert.That(async () => await ledger.AdvanceAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task AdvanceAsync_bumps_the_version_and_pushes_it_to_every_leased_subscriber()
    {
        var (ledger, _) = PastGrace();
        ledger.Lease("silo-1");
        ledger.Lease("silo-2");
        var log = new List<(string, TenantPolicyEpoch)>();

        var epoch = await ledger.AdvanceAsync(Recording(log));

        Assert.Multiple(() =>
        {
            Assert.That(epoch.Version, Is.EqualTo(1));
            Assert.That(ledger.Current, Is.EqualTo(epoch));
            Assert.That(log, Is.EquivalentTo(new[] { ("silo-1", epoch), ("silo-2", epoch) }));
        });
    }

    [Test]
    public async Task AdvanceAsync_does_not_push_to_or_wait_for_an_expired_lease_and_drops_it()
    {
        var (ledger, time) = PastGrace();
        ledger.Lease("silo-1");
        time.Advance(Lease);
        var log = new List<(string, TenantPolicyEpoch)>();

        var advance = ledger.AdvanceAsync(Recording(log));

        Assert.That(advance.IsCompletedSuccessfully, Is.True, "an expired lease holds no authority to wait out");
        Assert.That(log, Is.Empty);
        Assert.That(ledger.SubscriberCount, Is.Zero);
        await advance;
    }

    [Test]
    public async Task AdvanceAsync_waits_out_the_lease_of_a_subscriber_that_does_not_acknowledge()
    {
        var (ledger, time) = PastGrace();
        ledger.Lease("silo-1");

        var advance = ledger.AdvanceAsync((_, _) => Task.FromException(new TimeoutException()));

        Assert.That(advance.IsCompleted, Is.False);
        time.Advance(Lease + ledger.Margin - TimeSpan.FromTicks(1));
        Assert.That(advance.IsCompleted, Is.False, "not until the lease plus the margin has passed");
        time.Advance(TimeSpan.FromTicks(1));
        await advance;
        Assert.That(ledger.SubscriberCount, Is.Zero, "the unacknowledging subscriber is dropped");
    }

    [Test]
    public async Task AdvanceAsync_treats_an_acknowledgement_slower_than_the_timeout_as_none()
    {
        var (ledger, time) = PastGrace();
        ledger.Lease("silo-1");
        var hung = new TaskCompletionSource();

        var advance = ledger.AdvanceAsync((_, _) => hung.Task);

        time.Advance(ledger.AckTimeout);
        Assert.That(advance.IsCompleted, Is.False, "a timed-out acknowledgement falls back to waiting out the lease");
        time.Advance(Lease + ledger.Margin);
        await advance;
    }

    [Test]
    public async Task AdvanceAsync_keeps_a_subscriber_that_renewed_while_its_lease_was_waited_out()
    {
        var (ledger, time) = PastGrace();
        ledger.Lease("silo-1");
        var advance = ledger.AdvanceAsync((_, _) => Task.FromException(new TimeoutException()));

        time.Advance(TimeSpan.FromSeconds(1));
        var renewal = ledger.Lease("silo-1");
        time.Advance(Lease + ledger.Margin);
        await advance;

        Assert.Multiple(() =>
        {
            Assert.That(renewal.Epoch.Version, Is.EqualTo(1), "a renewal after the advance carries the new epoch");
            Assert.That(ledger.SubscriberCount, Is.EqualTo(1), "a subscriber that renewed is not dropped");
        });
    }

    [Test]
    public void Lease_never_shortens_a_recorded_deadline()
    {
        var (ledger, time) = PastGrace();
        ledger.Lease("silo-1");
        time.Advance(TimeSpan.FromSeconds(5));
        ledger.Lease("silo-1");

        var advance = ledger.AdvanceAsync((_, _) => Task.FromException(new TimeoutException()));
        time.Advance(Lease + ledger.Margin - TimeSpan.FromSeconds(1));

        Assert.That(advance.IsCompleted, Is.False, "the later deadline from the renewal is the one waited out");
    }

    [Test]
    public async Task AdvanceAsync_on_a_fresh_incarnation_waits_out_one_lease_plus_margin()
    {
        var time = new FakeTimeProvider();
        var ledger = new TenantPolicyEpochLedger<string>(Lease, time, Guid.NewGuid());

        var advance = ledger.AdvanceAsync((_, _) => Task.CompletedTask);

        Assert.That(advance.IsCompleted, Is.False, "leases granted by the previous incarnation may still be live");
        time.Advance(Lease + ledger.Margin - TimeSpan.FromTicks(1));
        Assert.That(advance.IsCompleted, Is.False);
        time.Advance(TimeSpan.FromTicks(1));
        Assert.That((await advance).Version, Is.EqualTo(1));
    }

    [Test]
    public void AdvanceAsync_caller_cancellation_propagates_from_the_wait()
    {
        var (ledger, _) = PastGrace();
        ledger.Lease("silo-1");
        using var cts = new CancellationTokenSource();
        var advance = ledger.AdvanceAsync((_, _) => Task.FromException(new TimeoutException()), cts.Token);

        cts.Cancel();

        Assert.That(async () => await advance, Throws.InstanceOf<OperationCanceledException>());
        Assert.That(ledger.Current.Version, Is.EqualTo(1), "the epoch has advanced even though the wait was cancelled");
    }

    [Test]
    public void AdvanceAsync_caller_cancellation_propagates_from_the_acknowledgement()
    {
        var (ledger, _) = PastGrace();
        ledger.Lease("silo-1");
        using var cts = new CancellationTokenSource();
        var hung = new TaskCompletionSource();
        var advance = ledger.AdvanceAsync((_, _) => hung.Task, cts.Token);

        cts.Cancel();

        Assert.That(async () => await advance, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task AdvanceAsync_successive_advances_produce_strictly_increasing_versions()
    {
        var (ledger, _) = PastGrace();

        var first = await ledger.AdvanceAsync((_, _) => Task.CompletedTask);
        var second = await ledger.AdvanceAsync((_, _) => Task.CompletedTask);

        Assert.That(second.Supersedes(first), Is.True);
        Assert.That(second.Version, Is.EqualTo(first.Version + 1));
    }
}
