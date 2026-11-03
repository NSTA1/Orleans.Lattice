using System.Collections.Immutable;
using System.Net;
using Microsoft.Extensions.Time.Testing;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// When a fresh <see cref="AppRegistryEpochGrain"/> activation may end its grace early (#5713). A silo
/// declared dead is not assumed to have stopped: one falsely declared dead may still evaluate on a lease
/// from the previous activation, so its death releases the grace only once this activation has seen it
/// dead for a whole lease plus the margin.
/// </summary>
[TestFixture]
public sealed class AppRegistryEpochGrainGraceTests
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(11);

    private FakeTimeProvider _time = null!;
    private Dictionary<SiloAddress, long> _deadSince = null!;

    [SetUp]
    public void SetUp()
    {
        _time = new FakeTimeProvider();
        _deadSince = [];
    }

    [Test]
    public void Every_silo_leased_releases_the_grace()
    {
        var snapshot = Membership((1, SiloStatus.Active), (2, SiloStatus.Joining));

        Assert.That(CanRelease(snapshot, Silo(1), Silo(2)), Is.True);
    }

    [Test]
    public void A_live_silo_that_has_not_leased_holds_the_grace()
    {
        var snapshot = Membership((1, SiloStatus.Active), (2, SiloStatus.Active));

        Assert.That(CanRelease(snapshot, Silo(1)), Is.False);
    }

    [Test]
    public void A_silo_newly_seen_dead_holds_the_grace_until_it_has_been_dead_for_a_lease_plus_the_margin()
    {
        var snapshot = Membership((1, SiloStatus.Active), (2, SiloStatus.Dead));

        Assert.That(CanRelease(snapshot, Silo(1)), Is.False, "a silo falsely declared dead may still hold a previous lease");
        _time.Advance(Window - TimeSpan.FromTicks(1));
        Assert.That(CanRelease(snapshot, Silo(1)), Is.False);
        _time.Advance(TimeSpan.FromTicks(1));
        Assert.That(CanRelease(snapshot, Silo(1)), Is.True, "any lease it could hold has expired");
    }

    [Test]
    public void A_dead_silo_that_leased_from_this_activation_does_not_hold_the_grace()
    {
        var snapshot = Membership((1, SiloStatus.Active), (2, SiloStatus.Dead));

        Assert.That(CanRelease(snapshot, Silo(1), Silo(2)), Is.True);
    }

    [Test]
    public void Every_death_is_recorded_even_when_another_silo_already_holds_the_grace()
    {
        var snapshot = Membership((1, SiloStatus.Active), (2, SiloStatus.Dead));

        Assert.That(CanRelease(snapshot), Is.False);
        Assert.That(_deadSince, Contains.Key(Silo(2)));
    }

    [Test]
    public void An_empty_membership_proves_nothing()
    {
        Assert.That(CanRelease(Membership()), Is.False);
    }

    private bool CanRelease(ClusterMembershipSnapshot snapshot, params SiloAddress[] leased) =>
        AppRegistryEpochGrain.CanReleaseGrace(snapshot, leased.ToHashSet(), _deadSince, _time.GetTimestamp(), _time, Window);

    private static SiloAddress Silo(int port) => SiloAddress.New(new IPEndPoint(IPAddress.Loopback, port), 1);

    private static ClusterMembershipSnapshot Membership(params (int Port, SiloStatus Status)[] members) =>
        new(
            members.ToImmutableDictionary(m => Silo(m.Port), m => new ClusterMember(Silo(m.Port), m.Status, $"silo-{m.Port}")),
            new MembershipVersion(1));
}
