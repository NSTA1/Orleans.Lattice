using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Unit tests for <see cref="BlobCacheEntryExpiration"/>: the pure expiry
/// arithmetic that turns <see cref="DistributedCacheEntryOptions"/> into stored
/// metadata, reads it back, tests expiry, and computes the sliding-window slide.
/// A fixed clock keeps every case deterministic.
/// </summary>
[TestFixture]
public sealed class BlobCacheEntryExpirationTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Test]
    public void Compute_no_options_yields_no_effective_expiry()
    {
        var values = BlobCacheEntryExpiration.Compute(new DistributedCacheEntryOptions(), Now);

        Assert.Multiple(() =>
        {
            Assert.That(values.Absolute, Is.Null);
            Assert.That(values.Sliding, Is.Null);
            Assert.That(values.Effective, Is.Null);
        });
    }

    [Test]
    public void Compute_absolute_relative_to_now_sets_the_cap()
    {
        var options = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(30) };

        var values = BlobCacheEntryExpiration.Compute(options, Now);

        Assert.Multiple(() =>
        {
            Assert.That(values.Absolute, Is.EqualTo(Now.AddMinutes(30)));
            Assert.That(values.Effective, Is.EqualTo(Now.AddMinutes(30)));
        });
    }

    [Test]
    public void Compute_throws_when_absolute_expiration_is_in_the_past()
    {
        var options = new DistributedCacheEntryOptions { AbsoluteExpiration = Now.AddMinutes(-1) };

        Assert.Throws<ArgumentOutOfRangeException>(() => BlobCacheEntryExpiration.Compute(options, Now));
    }

    [Test]
    public void Compute_sliding_only_sets_effective_to_now_plus_window()
    {
        var options = new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(10) };

        var values = BlobCacheEntryExpiration.Compute(options, Now);

        Assert.Multiple(() =>
        {
            Assert.That(values.Sliding, Is.EqualTo(TimeSpan.FromMinutes(10)));
            Assert.That(values.Absolute, Is.Null);
            Assert.That(values.Effective, Is.EqualTo(Now.AddMinutes(10)));
        });
    }

    [Test]
    public void Compute_sliding_is_capped_at_the_absolute_expiration()
    {
        var options = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(30),
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),
        };

        var values = BlobCacheEntryExpiration.Compute(options, Now);

        Assert.That(values.Effective, Is.EqualTo(Now.AddMinutes(5)));
    }

    [Test]
    public void ToMetadata_then_FromMetadata_round_trips_all_components()
    {
        var options = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(10),
            AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1),
        };
        var original = BlobCacheEntryExpiration.Compute(options, Now);

        var restored = BlobCacheEntryExpiration.FromMetadata(BlobCacheEntryExpiration.ToMetadata(original));

        Assert.Multiple(() =>
        {
            Assert.That(restored.Absolute, Is.EqualTo(original.Absolute));
            Assert.That(restored.Sliding, Is.EqualTo(original.Sliding));
            Assert.That(restored.Effective, Is.EqualTo(original.Effective));
        });
    }

    [Test]
    public void ToMetadata_writes_no_keys_for_a_never_expiring_entry()
    {
        var metadata = BlobCacheEntryExpiration.ToMetadata(default);

        Assert.That(metadata, Is.Empty);
    }

    [Test]
    public void FromMetadata_null_reads_back_as_never_expires()
    {
        var values = BlobCacheEntryExpiration.FromMetadata(null);

        Assert.That(values.Effective, Is.Null);
    }

    [Test]
    public void FromMetadata_ignores_unparsable_values()
    {
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["expiry"] = "not-a-number",
            ["sldexp"] = "-5",
        };

        var values = BlobCacheEntryExpiration.FromMetadata(metadata);

        Assert.Multiple(() =>
        {
            Assert.That(values.Effective, Is.Null);
            Assert.That(values.Sliding, Is.Null);
        });
    }

    [Test]
    public void IsExpired_is_true_once_the_clock_reaches_the_effective_instant()
    {
        var values = new BlobCacheEntryExpiration.Values(null, null, Now);

        Assert.Multiple(() =>
        {
            Assert.That(BlobCacheEntryExpiration.IsExpired(values, Now.AddTicks(-1)), Is.False);
            Assert.That(BlobCacheEntryExpiration.IsExpired(values, Now), Is.True);
            Assert.That(BlobCacheEntryExpiration.IsExpired(values, Now.AddSeconds(1)), Is.True);
        });
    }

    [Test]
    public void IsExpired_is_false_for_a_never_expiring_entry()
    {
        Assert.That(BlobCacheEntryExpiration.IsExpired(default, Now), Is.False);
    }

    [Test]
    public void Slide_returns_null_for_a_non_sliding_entry()
    {
        var values = new BlobCacheEntryExpiration.Values(Now.AddMinutes(10), null, Now.AddMinutes(10));

        Assert.That(BlobCacheEntryExpiration.Slide(values, Now.AddMinutes(1)), Is.Null);
    }

    [Test]
    public void Slide_advances_the_window_on_read()
    {
        var window = TimeSpan.FromMinutes(10);
        var values = new BlobCacheEntryExpiration.Values(null, window, Now + window);
        var readAt = Now.AddMinutes(3);

        var slid = BlobCacheEntryExpiration.Slide(values, readAt);

        Assert.That(slid, Is.EqualTo(readAt + window));
    }

    [Test]
    public void Slide_is_capped_at_the_absolute_expiration()
    {
        var absolute = Now.AddMinutes(12);
        var values = new BlobCacheEntryExpiration.Values(absolute, TimeSpan.FromMinutes(10), Now.AddMinutes(10));
        var readAt = Now.AddMinutes(9);

        var slid = BlobCacheEntryExpiration.Slide(values, readAt);

        Assert.That(slid, Is.EqualTo(absolute));
    }

    [Test]
    public void Slide_returns_null_when_the_window_would_not_advance()
    {
        var window = TimeSpan.FromMinutes(10);
        var values = new BlobCacheEntryExpiration.Values(null, window, Now + window);

        // Reading at the write instant would compute the same effective expiry,
        // so nothing should be rewritten.
        Assert.That(BlobCacheEntryExpiration.Slide(values, Now), Is.Null);
    }

    [Test]
    public void Slide_returns_null_when_already_at_the_absolute_cap()
    {
        var absolute = Now.AddMinutes(5);
        var values = new BlobCacheEntryExpiration.Values(absolute, TimeSpan.FromMinutes(10), absolute);
        var readAt = Now.AddMinutes(3);

        Assert.That(BlobCacheEntryExpiration.Slide(values, readAt), Is.Null);
    }

    [Test]
    public void Slide_returns_null_for_a_sliding_entry_that_never_expires()
    {
        // A sliding window with no stored effective instant never expires on its
        // own: IsExpired treats a null effective instant as not-expired, and
        // FromMetadata degrades a partial or corrupt blob (a sliding window
        // present, the effective instant missing or unparsable) to exactly this
        // shape. A read must therefore not rewrite it to a finite expiry, which
        // would silently shorten a never-expiring entry - the same
        // null-effective-first guard IsExpired already applies.
        var values = new BlobCacheEntryExpiration.Values(null, TimeSpan.FromMinutes(10), null);

        Assert.Multiple(() =>
        {
            Assert.That(BlobCacheEntryExpiration.IsExpired(values, Now.AddYears(1)), Is.False);
            Assert.That(BlobCacheEntryExpiration.Slide(values, Now.AddMinutes(1)), Is.Null);
        });
    }
}
