using System.Globalization;
using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Boundary and rejection cases for <see cref="BlobCacheEntryExpiration"/> that the
/// happy-path fixtures do not reach: each individual reason a stored metadata
/// component is discarded as unusable, and the sliding-slide arm where an absolute
/// cap exists but does not bind.
/// <para>
/// These matter because <see cref="BlobCacheEntryExpiration.FromMetadata"/> is the
/// trust boundary between the cache and blob metadata that a tenant, an operator, or
/// a half-finished write can influence. Its contract is that anything unusable
/// degrades to "never expires" rather than throwing or producing a nonsense instant,
/// and that contract only holds if every discard reason is exercised.
/// </para>
/// </summary>
[TestFixture]
public sealed class BlobCacheEntryExpirationBoundaryTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    // ---- Sliding-window guard -------------------------------------------

    [Test]
    public void DistributedCacheEntryOptions_itself_rejects_a_non_positive_sliding_window()
    {
        // Pins why Compute's own non-positive-window guard is belt-and-braces rather
        // than a live path: the options type validates in its setter, so a caller
        // cannot hand Compute a zero or negative window through the public API. If a
        // future Microsoft.Extensions.Caching.Abstractions relaxes this, the guard
        // becomes reachable and this test is the signal to cover it directly.
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.Zero },
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(-1) },
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Compute_accepts_the_smallest_positive_window()
    {
        var options = new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromTicks(1) };

        var values = BlobCacheEntryExpiration.Compute(options, Now);

        Assert.That(values.Effective, Is.EqualTo(Now.AddTicks(1)));
    }

    [Test]
    public void Compute_caps_the_smallest_positive_window_at_an_earlier_absolute_expiration()
    {
        // A window that is positive but tiny still runs the cap comparison, so the
        // absolute expiration wins only when it is genuinely the earlier instant.
        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpiration = Now.AddTicks(-1).AddTicks(2),
            SlidingExpiration = TimeSpan.FromDays(1),
        };

        var values = BlobCacheEntryExpiration.Compute(options, Now);

        Assert.That(values.Effective, Is.EqualTo(Now.AddTicks(1)));
    }

    // ---- Metadata parsing: every discard reason -------------------------

    [Test]
    public void FromMetadata_discards_an_unparsable_sliding_window()
    {
        // Distinct from the negative-window case: here the value never parses at all,
        // which is the shape a truncated or hand-edited metadata write produces.
        var metadata = Metadata((BlobCacheEntryExpiration.SlidingExpirationMetadataKey, "not-a-number"));

        Assert.That(BlobCacheEntryExpiration.FromMetadata(metadata).Sliding, Is.Null);
    }

    [Test]
    public void FromMetadata_discards_a_zero_sliding_window()
    {
        var metadata = Metadata((BlobCacheEntryExpiration.SlidingExpirationMetadataKey, "0"));

        Assert.That(
            BlobCacheEntryExpiration.FromMetadata(metadata).Sliding,
            Is.Null,
            "A zero window is not positive, so it must read back as absent rather than "
            + "as a window that expires the entry on every read.");
    }

    [Test]
    public void FromMetadata_keeps_the_smallest_positive_sliding_window()
    {
        var metadata = Metadata((BlobCacheEntryExpiration.SlidingExpirationMetadataKey, "1"));

        Assert.That(BlobCacheEntryExpiration.FromMetadata(metadata).Sliding, Is.EqualTo(TimeSpan.FromTicks(1)));
    }

    [TestCase("-1", TestName = "FromMetadata_discards_negative_ticks")]
    [TestCase("not-a-number", TestName = "FromMetadata_discards_unparsable_ticks")]
    [TestCase("", TestName = "FromMetadata_discards_empty_ticks")]
    public void FromMetadata_discards_an_unusable_instant(string raw)
    {
        var metadata = Metadata(
            (BlobCacheEntryExpiration.AbsoluteExpirationMetadataKey, raw),
            (BlobCacheEntryExpiration.EffectiveExpirationMetadataKey, raw));

        var values = BlobCacheEntryExpiration.FromMetadata(metadata);

        Assert.Multiple(() =>
        {
            Assert.That(values.Absolute, Is.Null);
            Assert.That(values.Effective, Is.Null);
        });
    }

    [Test]
    public void FromMetadata_discards_an_instant_beyond_the_representable_range()
    {
        // A tick count that parses as a long but exceeds DateTimeOffset.MaxValue would
        // throw inside the DateTimeOffset constructor. The range check must reject it
        // first, so a corrupt blob degrades to "never expires" instead of faulting
        // every read of that entry.
        var beyondMax = (DateTimeOffset.MaxValue.UtcTicks + 1).ToString(CultureInfo.InvariantCulture);
        var metadata = Metadata(
            (BlobCacheEntryExpiration.AbsoluteExpirationMetadataKey, beyondMax),
            (BlobCacheEntryExpiration.EffectiveExpirationMetadataKey, long.MaxValue.ToString(CultureInfo.InvariantCulture)));

        BlobCacheEntryExpiration.Values? parsed = null;
        Assert.DoesNotThrow(() => parsed = BlobCacheEntryExpiration.FromMetadata(metadata));

        Assert.Multiple(() =>
        {
            Assert.That(parsed!.Value.Absolute, Is.Null);
            Assert.That(parsed.Value.Effective, Is.Null);
        });
    }

    [Test]
    public void FromMetadata_accepts_the_representable_boundaries()
    {
        var metadata = Metadata(
            (BlobCacheEntryExpiration.AbsoluteExpirationMetadataKey, DateTimeOffset.MaxValue.UtcTicks.ToString(CultureInfo.InvariantCulture)),
            (BlobCacheEntryExpiration.EffectiveExpirationMetadataKey, "0"));

        var values = BlobCacheEntryExpiration.FromMetadata(metadata);

        Assert.Multiple(() =>
        {
            Assert.That(values.Absolute, Is.EqualTo(new DateTimeOffset(DateTimeOffset.MaxValue.UtcTicks, TimeSpan.Zero)));
            Assert.That(values.Effective, Is.EqualTo(new DateTimeOffset(0, TimeSpan.Zero)));
        });
    }

    // ---- Slide: the cap present but not binding -------------------------

    [Test]
    public void Slide_advances_normally_when_an_absolute_cap_exists_but_does_not_bind()
    {
        // The cap is far enough out that the slid window lands below it, so the slide
        // must return the full window rather than clamping to the cap.
        var window = TimeSpan.FromMinutes(10);
        var absolute = Now.AddHours(6);
        var values = new BlobCacheEntryExpiration.Values(absolute, window, Now + window);
        var readAt = Now.AddMinutes(4);

        var slid = BlobCacheEntryExpiration.Slide(values, readAt);

        Assert.Multiple(() =>
        {
            Assert.That(slid, Is.EqualTo(readAt + window));
            Assert.That(slid, Is.LessThan(absolute));
        });
    }

    [Test]
    public void Slide_clamps_to_the_cap_exactly_at_the_boundary()
    {
        // candidate == absolute is not "greater than", so the cap must not be applied
        // as a clamp - the candidate already equals it and is returned as an advance.
        var window = TimeSpan.FromMinutes(10);
        var absolute = Now.AddMinutes(14);
        var values = new BlobCacheEntryExpiration.Values(absolute, window, Now.AddMinutes(10));
        var readAt = Now.AddMinutes(4);

        Assert.That(BlobCacheEntryExpiration.Slide(values, readAt), Is.EqualTo(absolute));
    }

    private static Dictionary<string, string> Metadata(params (string Key, string Value)[] entries)
    {
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in entries)
        {
            metadata[key] = value;
        }

        return metadata;
    }
}
