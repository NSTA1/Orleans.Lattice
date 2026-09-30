using System.Collections.Immutable;
using System.Net;
using Microsoft.Extensions.Time.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantSnapshotCurrency"/>, the cross-silo currency
/// state every per-silo tenant-registry snapshot composes (issues #4030, #4051,
/// #4052), and for the epoch grain's fresh-incarnation early grace release
/// (<see cref="TenantPolicyEpochGrain.EveryLiveSiloHasLeased"/> and
/// <see cref="TenantPolicyEpochLedger{TSubscriber}.ReleaseGrace"/>). Fake clocks
/// only; nothing sleeps.
/// </summary>
[TestFixture]
public sealed class TenantSnapshotCurrencyTests
{
    private static readonly Guid Incarnation = Guid.NewGuid();
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(10);

    private static TenantPolicyEpochLease LeaseAt(long version, Guid? incarnation = null) =>
        new(new TenantPolicyEpoch(incarnation ?? Incarnation, version), Lease);

    [Test]
    public void Constructor_null_time_provider_throws()
    {
        Assert.That(() => new TenantSnapshotCurrency(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Fresh_currency_is_not_current_and_exposes_its_clock()
    {
        var time = new FakeTimeProvider();
        var currency = new TenantSnapshotCurrency(time);

        Assert.Multiple(() =>
        {
            Assert.That(currency.Time, Is.SameAs(time));
            Assert.That(currency.Generation, Is.Zero);
            Assert.That(currency.IsCurrent(0), Is.False, "no lease yet");
            Assert.That(currency.LeaseEstablished.IsCompleted, Is.False);
        });
    }

    [Test]
    public void ApplyLease_first_lease_supersedes_and_makes_the_current_generation_current()
    {
        var time = new FakeTimeProvider();
        var currency = new TenantSnapshotCurrency(time);

        var superseded = currency.ApplyLease(LeaseAt(0), time.GetTimestamp());

        Assert.Multiple(() =>
        {
            Assert.That(superseded, Is.True, "the first epoch always supersedes the default one");
            Assert.That(currency.Generation, Is.EqualTo(1));
            Assert.That(currency.IsCurrent(1), Is.True);
            Assert.That(currency.IsCurrent(0), Is.False, "a snapshot built before the lease is out of date");
            Assert.That(currency.LeaseEstablished.IsCompletedSuccessfully, Is.True);
        });
    }

    [Test]
    public void ApplyLease_renewal_with_the_same_epoch_does_not_supersede()
    {
        var time = new FakeTimeProvider();
        var currency = new TenantSnapshotCurrency(time);
        currency.ApplyLease(LeaseAt(0), time.GetTimestamp());

        Assert.That(currency.ApplyLease(LeaseAt(0), time.GetTimestamp()), Is.False);
        Assert.That(currency.Generation, Is.EqualTo(1));
    }

    [Test]
    public void Lease_lapses_one_lease_after_the_request_and_is_never_shortened()
    {
        var time = new FakeTimeProvider();
        var currency = new TenantSnapshotCurrency(time);
        var early = time.GetTimestamp();
        time.Advance(TimeSpan.FromSeconds(4));
        currency.ApplyLease(LeaseAt(0), time.GetTimestamp());
        currency.ApplyLease(LeaseAt(0), early);

        time.Advance(Lease - TimeSpan.FromTicks(1));
        Assert.That(currency.IsCurrent(1), Is.True, "the later deadline stands");
        time.Advance(TimeSpan.FromTicks(1));
        Assert.That(currency.IsCurrent(1), Is.False);
    }

    [Test]
    public void Observe_newer_version_or_incarnation_supersedes_and_older_does_not()
    {
        var currency = new TenantSnapshotCurrency(new FakeTimeProvider());

        Assert.Multiple(() =>
        {
            Assert.That(currency.Observe(new TenantPolicyEpoch(Incarnation, 2)), Is.True);
            Assert.That(currency.Observe(new TenantPolicyEpoch(Incarnation, 2)), Is.False);
            Assert.That(currency.Observe(new TenantPolicyEpoch(Incarnation, 1)), Is.False);
            Assert.That(currency.Observe(new TenantPolicyEpoch(Guid.NewGuid(), 0)), Is.True);
            Assert.That(currency.Generation, Is.EqualTo(2));
        });
    }

    [Test]
    public void Invalidate_bumps_the_generation()
    {
        var time = new FakeTimeProvider();
        var currency = new TenantSnapshotCurrency(time);
        currency.ApplyLease(LeaseAt(0), time.GetTimestamp());

        currency.Invalidate();

        Assert.Multiple(() =>
        {
            Assert.That(currency.Generation, Is.EqualTo(2));
            Assert.That(currency.IsCurrent(1), Is.False);
            Assert.That(currency.IsCurrent(2), Is.True);
        });
    }

    [Test]
    public void System_clock_lease_is_current_within_its_duration_and_expires_early_by_the_allowance()
    {
        var currency = new TenantSnapshotCurrency(TimeProvider.System);
        var shortLease = new TenantPolicyEpochLease(
            new TenantPolicyEpoch(Incarnation, 0),
            TimeSpan.FromMilliseconds(TenantSnapshotCurrency.CoarseClockAllowanceMilliseconds));
        currency.ApplyLease(shortLease, TimeProvider.System.GetTimestamp());
        Assert.That(currency.IsCurrent(1), Is.False, "a lease no longer than the allowance is never current");

        currency.ApplyLease(new TenantPolicyEpochLease(new TenantPolicyEpoch(Incarnation, 0), TimeSpan.FromHours(1)), TimeProvider.System.GetTimestamp());
        Assert.That(currency.IsCurrent(1), Is.True);
    }

    // ---- fresh-incarnation early grace release ------------------------------

    private static SiloAddress Silo(int port) => SiloAddress.New(new IPEndPoint(IPAddress.Loopback, port), 1);

    private static ClusterMembershipSnapshot Membership(params (int Port, SiloStatus Status)[] members) =>
        new(
            members.ToImmutableDictionary(m => Silo(m.Port), m => new ClusterMember(Silo(m.Port), m.Status, $"silo-{m.Port}")),
            new MembershipVersion(1));

    [Test]
    public void EveryLiveSiloHasLeased_is_true_only_when_every_non_dead_silo_leased()
    {
        var snapshot = Membership((1, SiloStatus.Active), (2, SiloStatus.Joining), (3, SiloStatus.Dead));

        Assert.Multiple(() =>
        {
            Assert.That(TenantPolicyEpochGrain.EveryLiveSiloHasLeased(snapshot, new HashSet<SiloAddress> { Silo(1), Silo(2) }), Is.True,
                "a dead silo cannot hold authority");
            Assert.That(TenantPolicyEpochGrain.EveryLiveSiloHasLeased(snapshot, new HashSet<SiloAddress> { Silo(1) }), Is.False,
                "a joining silo may hold a lease from the previous activation");
        });
    }

    [Test]
    public void EveryLiveSiloHasLeased_empty_membership_proves_nothing()
    {
        Assert.That(TenantPolicyEpochGrain.EveryLiveSiloHasLeased(Membership(), new HashSet<SiloAddress>()), Is.False);
    }

    [Test]
    public void EveryLiveSiloHasLeased_null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantPolicyEpochGrain.EveryLiveSiloHasLeased(null!, new HashSet<SiloAddress>()), Throws.ArgumentNullException);
            Assert.That(() => TenantPolicyEpochGrain.EveryLiveSiloHasLeased(Membership((1, SiloStatus.Active)), null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task ReleaseGrace_lets_a_fresh_incarnations_advance_complete_before_one_lease()
    {
        var time = new FakeTimeProvider();
        var ledger = new TenantPolicyEpochLedger<string>(Lease, time, Guid.NewGuid());
        var advance = ledger.AdvanceAsync((_, _) => Task.CompletedTask);
        Assert.That(advance.IsCompleted, Is.False, "precondition: the grace holds the advance");

        ledger.ReleaseGrace();
        ledger.ReleaseGrace();

        Assert.That((await advance).Version, Is.EqualTo(1));
        Assert.That(ledger.IsGraceReleased, Is.True);
    }

    [Test]
    public void ReleaseGrace_before_an_advance_skips_the_grace_entirely()
    {
        var ledger = new TenantPolicyEpochLedger<string>(Lease, new FakeTimeProvider(), Guid.NewGuid());
        Assert.That(ledger.IsGraceReleased, Is.False);

        ledger.ReleaseGrace();

        Assert.That(ledger.AdvanceAsync((_, _) => Task.CompletedTask).IsCompletedSuccessfully, Is.True);
    }
}
