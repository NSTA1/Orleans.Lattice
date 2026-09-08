using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="LatticeView.ComputeCacheExpiry"/>: the read-handle
/// cache-expiry stamp must saturate rather than overflow the
/// <c>DateTime + TimeSpan</c> operator for an extreme-but-permitted
/// <c>LatticeViewOptions.ReadHandleCacheTtl</c> (validated only as strictly
/// positive), so resolving the active tree never throws on a view read.
/// </summary>
[TestFixture]
public sealed class LatticeViewReadHandleExpiryTests
{
    [Test]
    public void ComputeCacheExpiry_adds_a_normal_ttl()
    {
        var now = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ttl = TimeSpan.FromSeconds(1);

        Assert.That(LatticeView.ComputeCacheExpiry(now, ttl), Is.EqualTo(now + ttl));
    }

    [Test]
    public void ComputeCacheExpiry_saturates_instead_of_throwing_on_overflow()
    {
        var now = DateTime.UtcNow;

        // Pre-fix, DateTime.UtcNow + TimeSpan.MaxValue throws
        // ArgumentOutOfRangeException on every read; saturating keeps the handle
        // cached ("effectively forever") instead of failing the resolve.
        Assert.That(() => LatticeView.ComputeCacheExpiry(now, TimeSpan.MaxValue), Throws.Nothing);
        Assert.That(LatticeView.ComputeCacheExpiry(now, TimeSpan.MaxValue).Ticks,
            Is.EqualTo(DateTime.MaxValue.Ticks));
        Assert.That(LatticeView.ComputeCacheExpiry(now, TimeSpan.MaxValue),
            Is.GreaterThan(now), "an extreme TTL must leave the handle cached, not expired");
    }
}
