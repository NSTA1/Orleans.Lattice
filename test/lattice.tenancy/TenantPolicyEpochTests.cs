using Microsoft.Extensions.Time.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the small value types of the tenant-policy epoch protocol:
/// <see cref="TenantPolicyEpoch"/>, <see cref="TenantPolicyEpochLease"/> and
/// <see cref="TenantPolicyTimestamps"/>.
/// </summary>
[TestFixture]
public sealed class TenantPolicyEpochTests
{
    private static readonly Guid Incarnation = Guid.NewGuid();

    [Test]
    public void Supersedes_a_later_version_of_the_same_incarnation()
    {
        Assert.That(new TenantPolicyEpoch(Incarnation, 2).Supersedes(new TenantPolicyEpoch(Incarnation, 1)), Is.True);
    }

    [TestCase(1)]
    [TestCase(0)]
    public void Does_not_supersede_an_equal_or_later_version_of_the_same_incarnation(long version)
    {
        Assert.That(new TenantPolicyEpoch(Incarnation, version).Supersedes(new TenantPolicyEpoch(Incarnation, 1)), Is.False);
    }

    [Test]
    public void Supersedes_any_version_of_a_different_incarnation()
    {
        Assert.That(new TenantPolicyEpoch(Guid.NewGuid(), 0).Supersedes(new TenantPolicyEpoch(Incarnation, 99)), Is.True);
    }

    [Test]
    public void Any_real_epoch_supersedes_the_default_one()
    {
        Assert.That(new TenantPolicyEpoch(Incarnation, 0).Supersedes(default), Is.True);
    }

    [Test]
    public void Lease_carries_its_epoch_and_duration()
    {
        var epoch = new TenantPolicyEpoch(Incarnation, 3);

        var lease = new TenantPolicyEpochLease(epoch, TimeSpan.FromSeconds(4));

        Assert.Multiple(() =>
        {
            Assert.That(lease.Epoch, Is.EqualTo(epoch));
            Assert.That(lease.Duration, Is.EqualTo(TimeSpan.FromSeconds(4)));
        });
    }

    [Test]
    public void Timestamps_Add_advances_on_the_providers_timestamp_scale()
    {
        var time = new FakeTimeProvider();
        var start = time.GetTimestamp();

        var later = TenantPolicyTimestamps.Add(time, start, TimeSpan.FromSeconds(2));

        Assert.That(time.GetElapsedTime(start, later), Is.EqualTo(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void Timestamps_Add_saturates_instead_of_overflowing()
    {
        var time = new FakeTimeProvider();

        Assert.That(TenantPolicyTimestamps.Add(time, long.MaxValue - 5, TimeSpan.FromDays(1)), Is.EqualTo(long.MaxValue));
    }
}
