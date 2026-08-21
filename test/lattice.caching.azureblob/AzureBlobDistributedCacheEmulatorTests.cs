using System.Text;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// End-to-end tests for the Azure Blob <see cref="IDistributedCache"/> driven
/// against an Azure Blob Storage endpoint - canonically
/// <see href="https://learn.microsoft.com/azure/storage/common/storage-use-azurite">Azurite</see>
/// on the default development connection string. Gated under the
/// <c>AzureBlobEmulator</c> NUnit category so the default dev loop skips them when
/// no emulator is running; <see cref="OneTimeSetUp"/> probes reachability and
/// falls through to <see cref="Assert.Inconclusive(string)"/> when unreachable.
/// The client is pinned to a blob-service API version the shared emulator accepts
/// (the SDK's newest default can outrun the emulator build), so the suite runs
/// against the shared Azurite rather than skipping.
/// A <see cref="MutableTimeProvider"/> makes expiry and sliding deterministic.
/// </summary>
[TestFixture]
[Category("AzureBlobEmulator")]
public sealed class AzureBlobDistributedCacheEmulatorTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";

    // Pinned to the newest blob-service API version the CI Azurite build (3.36.0)
    // accepts (2025-11-05). Without a pin the SDK default outruns the emulator, the
    // reachability probe throws, and every test here self-skips. Kept identical to
    // the sibling versioned fixtures so all blob suites make the same assumption.
    private const BlobClientOptions.ServiceVersion EmulatorApiVersion =
        BlobClientOptions.ServiceVersion.V2025_11_05;

    private BlobServiceClient _adminClient = null!;
    private string _containerName = null!;
    private MutableTimeProvider _clock = null!;
    private IDistributedCache _cache = null!;

    private static BlobServiceClient CreateServiceClient() =>
        new(AzuriteConnectionString, new BlobClientOptions(EmulatorApiVersion));

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _adminClient = CreateServiceClient();
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
                $"Azurite is not reachable on the default development endpoint ({AzuriteConnectionString}). "
                + "Start it via 'azurite --silent --location <dir>' or skip the AzureBlobEmulator category. "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [SetUp]
    public void SetUp()
    {
        _containerName = "cache-test-" + Guid.NewGuid().ToString("n");
        _clock = new MutableTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceClient = CreateServiceClient(),
            ContainerName = _containerName,
        };
        _cache = new AzureBlobDistributedCache(options.BuildContainerClient(), options.KeyPrefix, _clock);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _adminClient.DeleteBlobContainerAsync(_containerName);
    }

    [Test]
    public async Task Set_then_Get_round_trips_the_value()
    {
        var value = Encoding.UTF8.GetBytes("hello-cache");

        await _cache.SetAsync("k1", value, new DistributedCacheEntryOptions());
        var read = await _cache.GetAsync("k1");

        Assert.That(read, Is.EqualTo(value));
    }

    [Test]
    public async Task Get_returns_null_for_a_missing_key()
    {
        Assert.That(await _cache.GetAsync("absent"), Is.Null);
    }

    [Test]
    public async Task Set_overwrites_an_existing_entry()
    {
        await _cache.SetAsync("k", Encoding.UTF8.GetBytes("v1"), new DistributedCacheEntryOptions());
        await _cache.SetAsync("k", Encoding.UTF8.GetBytes("v2"), new DistributedCacheEntryOptions());

        Assert.That(await _cache.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("v2")));
    }

    [Test]
    public async Task Remove_deletes_the_entry()
    {
        await _cache.SetAsync("k", Encoding.UTF8.GetBytes("v"), new DistributedCacheEntryOptions());
        await _cache.RemoveAsync("k");

        Assert.That(await _cache.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task Get_treats_an_absolutely_expired_entry_as_a_miss()
    {
        await _cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) });

        _clock.Advance(TimeSpan.FromMinutes(6));

        Assert.That(await _cache.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task Get_before_expiry_returns_the_value()
    {
        await _cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) });

        _clock.Advance(TimeSpan.FromMinutes(4));

        Assert.That(await _cache.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("v")));
    }

    [Test]
    public async Task Sliding_entry_is_kept_alive_by_reads()
    {
        await _cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(10) });

        // Two reads eight minutes apart: without sliding the entry would die at
        // t+10; each read slides the window forward, so it survives to t+16.
        _clock.Advance(TimeSpan.FromMinutes(8));
        Assert.That(await _cache.GetAsync("k"), Is.Not.Null);

        _clock.Advance(TimeSpan.FromMinutes(8));
        Assert.That(await _cache.GetAsync("k"), Is.Not.Null);
    }

    [Test]
    public async Task Sliding_entry_expires_after_a_full_idle_window()
    {
        await _cache.SetAsync(
            "k",
            Encoding.UTF8.GetBytes("v"),
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(10) });

        _clock.Advance(TimeSpan.FromMinutes(11));

        Assert.That(await _cache.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task Refresh_on_a_missing_key_is_a_no_op()
    {
        Assert.DoesNotThrowAsync(() => _cache.RefreshAsync("absent"));
    }

    [Test]
    public async Task KeyPrefix_scopes_entries_within_a_shared_container()
    {
        var shared = new LatticeAzureBlobCacheOptions
        {
            ServiceClient = CreateServiceClient(),
            ContainerName = _containerName,
        };
        var cacheA = new AzureBlobDistributedCache(shared.BuildContainerClient(), "a/", _clock);
        var cacheB = new AzureBlobDistributedCache(shared.BuildContainerClient(), "b/", _clock);

        await cacheA.SetAsync("same", Encoding.UTF8.GetBytes("from-a"), new DistributedCacheEntryOptions());

        Assert.Multiple(async () =>
        {
            Assert.That(await cacheA.GetAsync("same"), Is.EqualTo(Encoding.UTF8.GetBytes("from-a")));
            Assert.That(await cacheB.GetAsync("same"), Is.Null);
        });
    }
}
