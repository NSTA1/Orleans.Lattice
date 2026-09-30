using System.Text;
using Azure;
using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Behavioural coverage for <see cref="AzureBlobDistributedCache"/> driven against
/// an in-memory <see cref="FakeBlobStore"/>, so the cache's own orchestration runs
/// in the default test suite rather than only when Azurite happens to be up.
/// <para>
/// Every other behavioural fixture for this type is
/// <c>[Category("AzureStorageEmulator")]</c> and so is excluded by the repository's
/// standard filter. That left the whole read/write path - lazy container creation,
/// the 404 miss arms, expiry-on-read eviction, and the sliding renewal - unexecuted
/// in an ordinary run, and it is also the shape the testing conventions warn about:
/// an emulator-gated fixture reports <c>Inconclusive</c> when the emulator is
/// absent, which NUnit counts as neither pass nor skip, so the suite still prints
/// <c>Passed!</c> while the coverage silently vanishes.
/// </para>
/// <para>
/// The fake models content, metadata, and a changing <see cref="ETag"/>, so the
/// assertions here are about observable cache behaviour - a value round-trips, an
/// expired entry reads as a miss and is evicted, a sliding entry's stored expiry
/// advances - and not merely about which branch was taken.
/// </para>
/// </summary>
[TestFixture]
public sealed class AzureBlobDistributedCacheBehaviourTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private FakeBlobStore _store = null!;
    private MutableTimeProvider _clock = null!;

    [SetUp]
    public void SetUp()
    {
        _store = new FakeBlobStore();
        _clock = new MutableTimeProvider(Start);
    }

    private AzureBlobDistributedCache CreateCache(string keyPrefix = "") =>
        new(_store.CreateContainerClient(), keyPrefix, _clock);

    private static byte[] Payload(string text = "payload") => Encoding.UTF8.GetBytes(text);

    private static DistributedCacheEntryOptions Absolute(TimeSpan ttl) =>
        new() { AbsoluteExpirationRelativeToNow = ttl };

    private static DistributedCacheEntryOptions Sliding(TimeSpan window) =>
        new() { SlidingExpiration = window };

    private string BlobNameFor(string key, string keyPrefix = "") =>
        BlobCacheKeyMap.ToBlobName(keyPrefix, key);

    private DateTimeOffset EffectiveExpiryOf(string key, string keyPrefix = "")
    {
        var metadata = _store.MetadataOf(BlobNameFor(key, keyPrefix));
        var ticks = long.Parse(
            metadata[BlobCacheEntryExpiration.EffectiveExpirationMetadataKey],
            System.Globalization.CultureInfo.InvariantCulture);
        return new DateTimeOffset(ticks, TimeSpan.Zero);
    }

    // ----- Round-trip -----

    [Test]
    public async Task SetAsync_then_GetAsync_round_trips_the_value()
    {
        var cache = CreateCache();
        var value = Payload();

        await cache.SetAsync("k", value, new DistributedCacheEntryOptions());
        var read = await cache.GetAsync("k");

        Assert.That(read, Is.EqualTo(value));
    }

    [Test]
    public async Task SetAsync_writes_under_the_hashed_blob_name_including_the_prefix()
    {
        var cache = CreateCache(keyPrefix: "tokens/");

        await cache.SetAsync("user-1", Payload(), new DistributedCacheEntryOptions());

        Assert.That(_store.Names, Is.EqualTo(new[] { BlobNameFor("user-1", "tokens/") }));
    }

    [Test]
    public async Task SetAsync_overwrites_an_existing_entry()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload("first"), new DistributedCacheEntryOptions());

        await cache.SetAsync("k", Payload("second"), new DistributedCacheEntryOptions());

        Assert.That(await cache.GetAsync("k"), Is.EqualTo(Payload("second")));
    }

    [Test]
    public async Task GetAsync_returns_null_for_a_key_that_was_never_written()
    {
        var cache = CreateCache();

        Assert.That(await cache.GetAsync("absent"), Is.Null);
    }

    // ----- Expiry enforced lazily on read -----

    [Test]
    public async Task GetAsync_reports_a_miss_and_evicts_an_entry_past_its_absolute_expiry()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Absolute(TimeSpan.FromMinutes(5)));

        _clock.Advance(TimeSpan.FromMinutes(6));
        var read = await cache.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.Null, "an expired entry reads as a miss");
            Assert.That(_store.Exists(BlobNameFor("k")), Is.False, "and is evicted on the read that found it expired");
        });
    }

    [Test]
    public async Task GetAsync_returns_the_value_while_the_entry_is_still_within_its_absolute_expiry()
    {
        // Anti-vacuity for the eviction above: the same entry, read before the cap.
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Absolute(TimeSpan.FromMinutes(5)));

        _clock.Advance(TimeSpan.FromMinutes(4));

        Assert.Multiple(() =>
        {
            Assert.That(cache.GetAsync("k").GetAwaiter().GetResult(), Is.EqualTo(Payload()));
            Assert.That(_store.Exists(BlobNameFor("k")), Is.True);
        });
    }

    [Test]
    public async Task GetAsync_still_reports_a_miss_when_evicting_the_expired_entry_fails()
    {
        // The eviction is best-effort: a concurrent delete or a transient failure
        // must not turn an expired read into a throw.
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Absolute(TimeSpan.FromMinutes(5)));
        _store.OnDelete = _ => new RequestFailedException(403, "Forbidden", "AuthorizationFailure", innerException: null);

        _clock.Advance(TimeSpan.FromMinutes(6));
        var read = await cache.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.Null);
            Assert.That(_store.DeleteCalls, Is.EqualTo(1), "the eviction must have been attempted, so the swallowed failure is real");
        });
    }

    [Test]
    public async Task An_entry_written_with_no_expiry_never_expires_on_its_own()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), new DistributedCacheEntryOptions());

        _clock.Advance(TimeSpan.FromDays(3650));

        Assert.That(await cache.GetAsync("k"), Is.EqualTo(Payload()));
    }

    // ----- Sliding renewal -----

    [Test]
    public async Task GetAsync_advances_the_stored_expiry_of_a_sliding_entry()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Sliding(TimeSpan.FromMinutes(10)));
        var initial = EffectiveExpiryOf("k");

        _clock.Advance(TimeSpan.FromMinutes(4));
        await cache.GetAsync("k");

        Assert.That(
            EffectiveExpiryOf("k"),
            Is.EqualTo(initial + TimeSpan.FromMinutes(4)),
            "reading a sliding entry must push its effective expiry forward by the elapsed time");
    }

    [Test]
    public async Task GetAsync_does_not_write_metadata_for_an_entry_with_no_sliding_window()
    {
        // The slide short-circuits before any storage call when there is nothing to
        // slide, which is what keeps a plain read a single round trip.
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Absolute(TimeSpan.FromHours(1)));

        await cache.GetAsync("k");

        Assert.That(_store.SetMetadataCalls, Is.Zero);
    }

    [Test]
    public async Task GetAsync_caps_a_sliding_entry_at_its_absolute_expiration()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(10),
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(12),
        });

        _clock.Advance(TimeSpan.FromMinutes(5));
        await cache.GetAsync("k");

        Assert.That(
            EffectiveExpiryOf("k"),
            Is.EqualTo(Start + TimeSpan.FromMinutes(12)),
            "the slide must not push the entry past its absolute cap");
    }

    [Test]
    public async Task GetAsync_still_returns_the_value_when_the_sliding_renewal_fails()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Sliding(TimeSpan.FromMinutes(10)));
        _store.OnSetMetadata = _ => new RequestFailedException(412, "ConditionNotMet", "ConditionNotMet", innerException: null);

        _clock.Advance(TimeSpan.FromMinutes(1));
        var read = await cache.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(Payload()), "a lost slide only shortens the window; it must not fail the read");
            Assert.That(_store.SetMetadataCalls, Is.EqualTo(1));
        });
    }

    // ----- RefreshAsync -----

    [Test]
    public async Task RefreshAsync_advances_the_stored_expiry_without_downloading_the_value()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Sliding(TimeSpan.FromMinutes(10)));
        var initial = EffectiveExpiryOf("k");
        var downloadsBefore = _store.DownloadCalls;

        _clock.Advance(TimeSpan.FromMinutes(3));
        await cache.RefreshAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(EffectiveExpiryOf("k"), Is.EqualTo(initial + TimeSpan.FromMinutes(3)));
            Assert.That(_store.DownloadCalls, Is.EqualTo(downloadsBefore), "a refresh reads properties, never content");
        });
    }

    [Test]
    public void RefreshAsync_is_a_no_op_for_a_key_that_was_never_written()
    {
        var cache = CreateCache();

        Assert.DoesNotThrowAsync(() => cache.RefreshAsync("absent"));
    }

    [Test]
    public async Task RefreshAsync_evicts_an_entry_it_finds_expired()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Absolute(TimeSpan.FromMinutes(5)));

        _clock.Advance(TimeSpan.FromMinutes(6));
        await cache.RefreshAsync("k");

        Assert.That(_store.Exists(BlobNameFor("k")), Is.False);
    }

    [Test]
    public async Task RefreshAsync_does_not_throw_when_evicting_the_expired_entry_fails()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Absolute(TimeSpan.FromMinutes(5)));
        _store.OnDelete = _ => new RequestFailedException(403, "Forbidden", "AuthorizationFailure", innerException: null);

        _clock.Advance(TimeSpan.FromMinutes(6));

        Assert.DoesNotThrowAsync(() => cache.RefreshAsync("k"));
        Assert.That(_store.DeleteCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task RefreshAsync_does_not_throw_when_the_sliding_renewal_fails()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), Sliding(TimeSpan.FromMinutes(10)));
        _store.OnSetMetadata = _ => new RequestFailedException(404, "BlobNotFound", "BlobNotFound", innerException: null);

        _clock.Advance(TimeSpan.FromMinutes(1));

        Assert.DoesNotThrowAsync(() => cache.RefreshAsync("k"));
        Assert.That(_store.SetMetadataCalls, Is.EqualTo(1));
    }

    // ----- RemoveAsync -----

    [Test]
    public async Task RemoveAsync_deletes_the_entry()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Payload(), new DistributedCacheEntryOptions());

        await cache.RemoveAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(_store.Exists(BlobNameFor("k")), Is.False);
            Assert.That(cache.GetAsync("k").GetAwaiter().GetResult(), Is.Null);
        });
    }

    [Test]
    public void RemoveAsync_is_a_no_op_for_a_key_that_was_never_written()
    {
        var cache = CreateCache();

        Assert.DoesNotThrowAsync(() => cache.RemoveAsync("absent"));
    }

    // ----- Container initialisation -----

    [Test]
    public async Task The_container_is_created_once_however_many_operations_run()
    {
        // _containerReady latches after the first call, so the create is not
        // re-issued on every subsequent operation.
        var cache = CreateCache();

        await cache.SetAsync("k", Payload(), new DistributedCacheEntryOptions());
        await cache.GetAsync("k");
        await cache.RefreshAsync("k");
        await cache.RemoveAsync("k");

        Assert.That(_store.ContainerCreateCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task Concurrent_first_operations_create_the_container_once()
    {
        // The second caller through the init gate takes the inner double-checked
        // return rather than issuing a second create.
        var cache = CreateCache();

        await Task.WhenAll(
            cache.GetAsync("a"),
            cache.GetAsync("b"),
            cache.GetAsync("c"),
            cache.GetAsync("d"));

        Assert.That(_store.ContainerCreateCalls, Is.EqualTo(1));
    }

    // ----- Synchronous IDistributedCache surface -----

    [Test]
    public void The_synchronous_members_delegate_to_their_asynchronous_counterparts()
    {
        var cache = CreateCache();
        var value = Payload();

        cache.Set("k", value, Sliding(TimeSpan.FromMinutes(10)));
        var read = cache.Get("k");
        cache.Refresh("k");
        cache.Remove("k");

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(value));
            Assert.That(_store.Exists(BlobNameFor("k")), Is.False);
            Assert.That(cache.Get("k"), Is.Null);
        });
    }
}
