using System.Text;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// End-to-end tests for <see cref="AzureBlobDistributedCache"/> driven against a
/// live Azure Blob endpoint (Azurite on the default development connection
/// string). These exercise the real blob I/O paths - upload, download, metadata
/// slide, lazy expiry eviction, and container auto-creation - that the pure-unit
/// fixtures cannot reach.
/// <para>
/// The client is pinned to a blob-service API version the running emulator
/// accepts (a newer SDK default is rejected by older Azurite builds), so the
/// suite runs deterministically against the shared emulator rather than skipping.
/// Each test uses a unique container so concurrent suites never collide, and the
/// container is deleted on teardown. A <see cref="MutableTimeProvider"/> makes
/// expiry and sliding deterministic without any real waiting.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureBlobDistributedCacheAzuriteTests
{
    private BlobServiceClient _adminClient = null!;
    private string _containerName = null!;
    private MutableTimeProvider _clock = null!;

    private AzureBlobDistributedCache CreateCache(string keyPrefix = "")
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceClient = AzuriteEmulator.CreateServiceClient(),
            ContainerName = _containerName,
        };
        return new AzureBlobDistributedCache(options.BuildContainerClient(), keyPrefix, _clock);
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _adminClient = AzuriteEmulator.CreateServiceClient();
        try
        {
            await foreach (var _ in _adminClient.GetBlobContainersAsync())
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azurite is not reachable on the default development endpoint ({AzuriteEmulator.ConnectionString}). "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [SetUp]
    public void SetUp()
    {
        // A unique, storage-legal container name per test keeps concurrent
        // suites (other agents sharing this emulator) from colliding.
        _containerName = "covblob-" + Guid.NewGuid().ToString("n");
        _clock = new MutableTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
    }

    [TearDown]
    public async Task TearDown()
    {
        await _adminClient.DeleteBlobContainerAsync(_containerName);
    }

    [Test]
    public async Task SetAsync_then_GetAsync_round_trips_the_value()
    {
        var cache = CreateCache();
        var value = Encoding.UTF8.GetBytes("hello-cache");

        await cache.SetAsync("k1", value, new DistributedCacheEntryOptions());
        var read = await cache.GetAsync("k1");

        Assert.That(read, Is.EqualTo(value));
    }

    [Test]
    public async Task GetAsync_returns_null_for_a_missing_key()
    {
        var cache = CreateCache();

        Assert.That(await cache.GetAsync("absent"), Is.Null);
    }

    [Test]
    public async Task SetAsync_overwrites_an_existing_entry()
    {
        var cache = CreateCache();

        await cache.SetAsync("k", Encoding.UTF8.GetBytes("v1"), new DistributedCacheEntryOptions());
        await cache.SetAsync("k", Encoding.UTF8.GetBytes("v2"), new DistributedCacheEntryOptions());

        Assert.That(await cache.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("v2")));
    }

    [Test]
    public async Task RemoveAsync_deletes_the_entry()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Encoding.UTF8.GetBytes("v"), new DistributedCacheEntryOptions());

        await cache.RemoveAsync("k");

        Assert.That(await cache.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task RemoveAsync_on_a_missing_key_is_a_no_op()
    {
        var cache = CreateCache();

        Assert.DoesNotThrowAsync(() => cache.RemoveAsync("absent"));
    }

    [Test]
    public async Task GetAsync_treats_an_absolutely_expired_entry_as_a_miss()
    {
        var cache = CreateCache();
        await cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) });

        _clock.Advance(TimeSpan.FromMinutes(6));

        Assert.That(await cache.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task GetAsync_evicts_an_expired_entry_so_a_later_read_still_misses()
    {
        var cache = CreateCache();
        await cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) });

        _clock.Advance(TimeSpan.FromMinutes(6));

        // First read evicts the expired blob; the blob is now physically gone,
        // so the second read takes the 404 miss path rather than the expiry path.
        Assert.That(await cache.GetAsync("k"), Is.Null);
        _clock.Advance(TimeSpan.FromMinutes(1));
        Assert.That(await cache.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task GetAsync_before_expiry_returns_the_value()
    {
        var cache = CreateCache();
        await cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) });

        _clock.Advance(TimeSpan.FromMinutes(4));

        Assert.That(await cache.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("v")));
    }

    [Test]
    public async Task GetAsync_slides_a_sliding_entry_forward_on_read()
    {
        var cache = CreateCache();
        await cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(10) });

        // Read at t+8 slides the window to t+18; without the slide the entry
        // would die at t+10, so surviving to t+15 proves the slide persisted.
        _clock.Advance(TimeSpan.FromMinutes(8));
        Assert.That(await cache.GetAsync("k"), Is.Not.Null);

        _clock.Advance(TimeSpan.FromMinutes(7));
        Assert.That(await cache.GetAsync("k"), Is.Not.Null);
    }

    [Test]
    public async Task GetAsync_lets_a_sliding_entry_expire_after_a_full_idle_window()
    {
        var cache = CreateCache();
        await cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(10) });

        _clock.Advance(TimeSpan.FromMinutes(11));

        Assert.That(await cache.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task RefreshAsync_on_a_missing_key_is_a_no_op()
    {
        var cache = CreateCache();

        Assert.DoesNotThrowAsync(() => cache.RefreshAsync("absent"));
    }

    [Test]
    public async Task RefreshAsync_slides_a_sliding_entry_without_reading_the_value()
    {
        var cache = CreateCache();
        await cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(10) });

        // Refresh at t+8 slides the window to t+18. Advancing to t+15 (past the
        // original t+10) and still reading a value proves Refresh persisted the
        // slide via SetMetadata.
        _clock.Advance(TimeSpan.FromMinutes(8));
        await cache.RefreshAsync("k");

        _clock.Advance(TimeSpan.FromMinutes(7));
        Assert.That(await cache.GetAsync("k"), Is.Not.Null);
    }

    [Test]
    public async Task RefreshAsync_evicts_an_expired_entry()
    {
        var cache = CreateCache();
        await cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) });

        _clock.Advance(TimeSpan.FromMinutes(6));
        await cache.RefreshAsync("k");

        Assert.That(await cache.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task KeyPrefix_scopes_entries_within_a_shared_container()
    {
        var cacheA = CreateCache("a/");
        var cacheB = CreateCache("b/");

        await cacheA.SetAsync("same", Encoding.UTF8.GetBytes("from-a"), new DistributedCacheEntryOptions());

        Assert.Multiple(async () =>
        {
            Assert.That(await cacheA.GetAsync("same"), Is.EqualTo(Encoding.UTF8.GetBytes("from-a")));
            Assert.That(await cacheB.GetAsync("same"), Is.Null);
        });
    }

    [Test]
    public async Task GetAsync_returns_the_value_for_a_never_expiring_entry_without_sliding()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Encoding.UTF8.GetBytes("v"), new DistributedCacheEntryOptions());

        // A second read of a non-sliding entry takes the "no slide" branch.
        Assert.That(await cache.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("v")));
        Assert.That(await cache.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("v")));
    }

    [Test]
    public void Set_then_Get_synchronous_wrappers_round_trip_the_value()
    {
        var cache = CreateCache();
        var value = Encoding.UTF8.GetBytes("sync-value");

        cache.Set("k", value, new DistributedCacheEntryOptions());
        var read = cache.Get("k");

        Assert.That(read, Is.EqualTo(value));
    }

    [Test]
    public void Refresh_synchronous_wrapper_does_not_throw_for_a_live_entry()
    {
        var cache = CreateCache();
        cache.Set(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(10) });

        Assert.DoesNotThrow(() => cache.Refresh("k"));
    }

    [Test]
    public void Remove_synchronous_wrapper_deletes_the_entry()
    {
        var cache = CreateCache();
        cache.Set("k", Encoding.UTF8.GetBytes("v"), new DistributedCacheEntryOptions());

        cache.Remove("k");

        Assert.That(cache.Get("k"), Is.Null);
    }

    [Test]
    public async Task EnsureContainer_creates_the_container_once_across_many_operations()
    {
        var cache = CreateCache();

        // The first operation creates the container; subsequent operations take
        // the already-ready fast path. All must succeed.
        await cache.SetAsync("a", Encoding.UTF8.GetBytes("1"), new DistributedCacheEntryOptions());
        await cache.SetAsync("b", Encoding.UTF8.GetBytes("2"), new DistributedCacheEntryOptions());

        Assert.Multiple(async () =>
        {
            Assert.That(await cache.GetAsync("a"), Is.EqualTo(Encoding.UTF8.GetBytes("1")));
            Assert.That(await cache.GetAsync("b"), Is.EqualTo(Encoding.UTF8.GetBytes("2")));
        });
    }
}
