using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Additional unit tests for <see cref="BlobCacheEntryExpiration"/> covering the
/// absolute-instant (non-relative) branch of <see cref="BlobCacheEntryExpiration.Compute"/>
/// and the non-positive sliding-window guard, complementing
/// <see cref="BlobCacheEntryExpirationTests"/>.
/// </summary>
[TestFixture]
public sealed class BlobCacheEntryExpirationAdditionalTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Test]
    public void Compute_absolute_instant_in_the_future_sets_the_cap()
    {
        var absolute = Now.AddHours(2);
        var options = new DistributedCacheEntryOptions { AbsoluteExpiration = absolute };

        var values = BlobCacheEntryExpiration.Compute(options, Now);

        Assert.Multiple(() =>
        {
            Assert.That(values.Absolute, Is.EqualTo(absolute));
            Assert.That(values.Effective, Is.EqualTo(absolute));
            Assert.That(values.Sliding, Is.Null);
        });
    }

    [Test]
    public void Compute_absolute_instant_exactly_now_throws()
    {
        var options = new DistributedCacheEntryOptions { AbsoluteExpiration = Now };

        Assert.Throws<ArgumentOutOfRangeException>(() => BlobCacheEntryExpiration.Compute(options, Now));
    }

    [Test]
    public void Compute_absolute_instant_caps_a_longer_sliding_window()
    {
        var absolute = Now.AddMinutes(5);
        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpiration = absolute,
            SlidingExpiration = TimeSpan.FromMinutes(30),
        };

        var values = BlobCacheEntryExpiration.Compute(options, Now);

        Assert.That(values.Effective, Is.EqualTo(absolute));
    }
}
