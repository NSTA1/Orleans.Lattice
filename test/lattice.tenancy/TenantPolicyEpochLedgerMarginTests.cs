using Microsoft.Extensions.Time.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for the clock-rate margin of
/// <see cref="TenantPolicyEpochLedger{TSubscriber}.AdvanceAsync"/>. The ledger waits
/// an unacknowledged lease out until its recorded deadline plus
/// <see cref="TenantPolicyEpochLedger{TSubscriber}.Margin"/>, because a silo whose
/// clock runs slower than the grain's can still hold its lease past the recorded
/// deadline. A lease that the ledger finds already past its recorded deadline, but
/// still inside that margin, is the same case and must not be dropped unseen: the
/// write would complete while the slow silo still trusts its pre-write snapshot.
/// The silo side is the real <see cref="TenantSnapshotCurrency"/> on its own,
/// slower, fake clock.
/// </summary>
[TestFixture]
public sealed class TenantPolicyEpochLedgerMarginTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(10);

    // The silo's clock runs at 95% of the grain's rate, inside the 10% margin.
    private const double SiloClockRate = 0.95;

    private static (TenantPolicyEpochLedger<string> Ledger, FakeTimeProvider GrainTime, FakeTimeProvider SiloTime) PastGrace()
    {
        var grainTime = new FakeTimeProvider();
        var ledger = new TenantPolicyEpochLedger<string>(Lease, grainTime, Guid.NewGuid());
        grainTime.Advance(Lease * 2);
        return (ledger, grainTime, new FakeTimeProvider());
    }

    private static void Elapse(FakeTimeProvider grainTime, FakeTimeProvider siloTime, TimeSpan real)
    {
        grainTime.Advance(real);
        siloTime.Advance(real * SiloClockRate);
    }

    [Test]
    public async Task AdvanceAsync_pushes_to_a_lease_past_its_deadline_but_inside_the_margin()
    {
        var (ledger, grainTime, _) = PastGrace();
        ledger.Lease("silo-1");
        grainTime.Advance(Lease + (ledger.Margin / 2));
        var notified = new List<string>();

        var epoch = await ledger.AdvanceAsync((subscriber, _) =>
        {
            notified.Add(subscriber);
            return Task.CompletedTask;
        });

        Assert.That(notified, Is.EqualTo(new[] { "silo-1" }), "a silo on a slower clock may still hold this lease");
        Assert.That(epoch.Version, Is.EqualTo(1));
    }

    [Test]
    public async Task AdvanceAsync_does_not_complete_while_a_slower_silo_still_trusts_its_lease()
    {
        var (ledger, grainTime, siloTime) = PastGrace();
        var currency = new TenantSnapshotCurrency(siloTime);
        var requestedAt = siloTime.GetTimestamp();
        currency.ApplyLease(ledger.Lease("silo-1"), requestedAt);
        var builtFor = currency.Generation;

        Elapse(grainTime, siloTime, Lease + (ledger.Margin / 2));
        Assert.That(currency.IsCurrent(builtFor), Is.True, "precondition: the slower silo still trusts its pre-write snapshot");

        var advance = ledger.AdvanceAsync((_, _) => Task.FromException(new TimeoutException()));

        Assert.That(advance.IsCompleted, Is.False, "the write must not complete while the silo is still authoritative");
        Elapse(grainTime, siloTime, ledger.Margin / 2);
        await advance;
        Assert.That(currency.IsCurrent(builtFor), Is.False, "once the advance completes the unreached silo's lease has lapsed");
    }
}
