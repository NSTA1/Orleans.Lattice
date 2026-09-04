using System.Net;
using System.Text;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Covers the resilience paths of <see cref="AzureBlobDistributedCache"/> that a
/// healthy emulator never exercises: the two best-effort storage calls that are
/// allowed to fail silently, and the double-checked container initialisation that
/// only a second caller arriving while the first holds the gate can reach.
/// <para>
/// Each case drives a real Azurite endpoint through
/// <see cref="InterceptingHttpHandler"/>, so only the one call under test is
/// synthesised and every surrounding operation is genuine blob I/O. That keeps the
/// assertions behavioural - a read still returns its value, an expired read still
/// reports a miss - rather than merely asserting that a catch block was entered.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureBlobDistributedCacheFaultInjectionTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private BlobServiceClient _adminClient = null!;
    private InterceptingHttpHandler _handler = null!;
    private HttpClient _httpClient = null!;
    private string _containerName = null!;
    private MutableTimeProvider _clock = null!;

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
        _containerName = "covfault-" + Guid.NewGuid().ToString("n");
        _clock = new MutableTimeProvider(Start);
        _handler = new InterceptingHttpHandler();
        _httpClient = new HttpClient(_handler);
    }

    [TearDown]
    public async Task TearDown()
    {
        _httpClient.Dispose();
        await _adminClient.DeleteBlobContainerAsync(_containerName);
    }

    /// <summary>
    /// Builds a cache whose transport routes through <see cref="_handler"/>. The
    /// service client is constructed here rather than left to the options' connection
    /// -string mode because the blob API version must be pinned to one the emulator
    /// accepts, and that is fixed at <c>BlobClientOptions</c> construction. Retries
    /// are disabled so an injected failure is delivered to the code under test once,
    /// immediately, instead of being retried into a timeout.
    /// </summary>
    private AzureBlobDistributedCache CreateCache() =>
        new(BuildOptions().BuildContainerClient(), string.Empty, _clock);

    private LatticeAzureBlobCacheOptions BuildOptions() => new()
    {
        ServiceClient = CreateInterceptedServiceClient(),
        ContainerName = _containerName,
    };

    private BlobServiceClient CreateInterceptedServiceClient()
    {
        var clientOptions = new BlobClientOptions(AzuriteEmulator.ApiVersion)
        {
            Transport = new Azure.Core.Pipeline.HttpClientTransport(_httpClient),
        };
        clientOptions.Retry.MaxRetries = 0;
        return new BlobServiceClient(AzuriteEmulator.ConnectionString, clientOptions);
    }

    // ---- Best-effort sliding renewal ------------------------------------

    [Test]
    public async Task GetAsync_still_returns_the_value_when_the_sliding_renewal_fails()
    {
        var cache = CreateCache();
        var value = Encoding.UTF8.GetBytes("payload");
        await cache.SetAsync("k", value, new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(10),
        });

        // Fail only the metadata write the slide performs. The download that
        // precedes it, and the container calls around it, still hit the emulator.
        var setMetadataAttempts = 0;
        _handler.Interceptor = (request, _) =>
        {
            if (InterceptingHttpHandler.IsSetMetadata(request))
            {
                Interlocked.Increment(ref setMetadataAttempts);
                return Task.FromResult<HttpResponseMessage?>(
                    InterceptingHttpHandler.StorageError(HttpStatusCode.NotFound, "BlobNotFound"));
            }

            return Task.FromResult<HttpResponseMessage?>(null);
        };

        _clock.Advance(TimeSpan.FromMinutes(1));
        var read = await cache.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(value),
                "A failed sliding renewal must not fail the read that triggered it; a lost "
                + "slide only shortens the window.");
            Assert.That(setMetadataAttempts, Is.EqualTo(1),
                "The read must have attempted exactly one slide, so the swallowed failure is real.");
        });
    }

    [Test]
    public async Task RefreshAsync_does_not_throw_when_the_sliding_renewal_fails()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Encoding.UTF8.GetBytes("payload"), new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(10),
        });

        _handler.Interceptor = (request, _) => Task.FromResult(
            InterceptingHttpHandler.IsSetMetadata(request)
                ? InterceptingHttpHandler.StorageError(HttpStatusCode.PreconditionFailed, "ConditionNotMet")
                : null);

        _clock.Advance(TimeSpan.FromMinutes(1));

        Assert.DoesNotThrowAsync(() => cache.RefreshAsync("k"));
    }

    // ---- Best-effort eviction of an expired entry ------------------------

    [Test]
    public async Task GetAsync_reports_a_miss_when_evicting_the_expired_entry_fails()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Encoding.UTF8.GetBytes("payload"), new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),
        });

        // A 403 carries no BlobNotFound error code, so DeleteIfExistsAsync surfaces
        // it as a RequestFailedException rather than absorbing it as "already gone" -
        // which is precisely the case the cache's own catch is there to handle.
        var deleteAttempts = 0;
        _handler.Interceptor = (request, _) =>
        {
            if (InterceptingHttpHandler.IsBlobDelete(request))
            {
                Interlocked.Increment(ref deleteAttempts);
                return Task.FromResult<HttpResponseMessage?>(
                    InterceptingHttpHandler.StorageError(HttpStatusCode.Forbidden, "AuthorizationFailure"));
            }

            return Task.FromResult<HttpResponseMessage?>(null);
        };

        _clock.Advance(TimeSpan.FromMinutes(6));
        var read = await cache.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.Null, "An expired entry reads as a miss whether or not it could be evicted.");
            Assert.That(deleteAttempts, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RefreshAsync_does_not_throw_when_evicting_the_expired_entry_fails()
    {
        var cache = CreateCache();
        await cache.SetAsync("k", Encoding.UTF8.GetBytes("payload"), new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),
        });

        _handler.Interceptor = (request, _) => Task.FromResult(
            InterceptingHttpHandler.IsBlobDelete(request)
                ? InterceptingHttpHandler.StorageError(HttpStatusCode.Forbidden, "AuthorizationFailure")
                : null);

        _clock.Advance(TimeSpan.FromMinutes(6));

        Assert.DoesNotThrowAsync(() => cache.RefreshAsync("k"));
    }

    // ---- Double-checked container initialisation -------------------------

    [Test]
    public async Task A_second_caller_arriving_during_initialisation_does_not_recreate_the_container()
    {
        var cache = CreateCache();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCreateStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var createAttempts = 0;

        _handler.Interceptor = async (request, _) =>
        {
            if (InterceptingHttpHandler.IsContainerCreate(request))
            {
                Interlocked.Increment(ref createAttempts);
                firstCreateStarted.TrySetResult();
                await release.Task.ConfigureAwait(false);
            }

            return null;
        };

        var value = Encoding.UTF8.GetBytes("v");

        // Caller one enters the init gate and parks inside CreateIfNotExists.
        var first = cache.SetAsync("a", value, new DistributedCacheEntryOptions());
        await firstCreateStarted.Task;

        // Caller two runs synchronously as far as the gate's WaitAsync, which cannot
        // complete while caller one holds it - so by the time this call returns its
        // task, caller two is provably queued behind the initialisation.
        var second = cache.SetAsync("b", value, new DistributedCacheEntryOptions());

        release.SetResult();
        await Task.WhenAll(first, second);

        Assert.Multiple(() =>
        {
            Assert.That(createAttempts, Is.EqualTo(1),
                "The second caller must observe the completed initialisation under the gate "
                + "and skip the create, not issue a redundant one.");
            Assert.That(cache.Get("a"), Is.EqualTo(value));
            Assert.That(cache.Get("b"), Is.EqualTo(value));
        });
    }

    // ---- Clock resolution through the DI seam ----------------------------

    [Test]
    public async Task A_container_registered_TimeProvider_drives_the_resolved_cache_expiry()
    {
        // The registration prefers a container-registered TimeProvider over
        // TimeProvider.System. Asserting it behaviourally - by expiring an entry with
        // a clock the test owns and no real waiting - proves the resolved cache is
        // actually wired to it.
        var clock = new MutableTimeProvider(Start);
        var serviceClient = CreateInterceptedServiceClient();
        using var provider = new ServiceCollection()
            .AddSingleton<TimeProvider>(clock)
            .AddAzureBlobDistributedCache(o =>
            {
                o.ServiceClient = serviceClient;
                o.ContainerName = _containerName;
            })
            .BuildServiceProvider();

        var cache = provider.GetRequiredService<IDistributedCache>();
        await cache.SetAsync("k", Encoding.UTF8.GetBytes("v"), new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),
        });

        Assert.That(await cache.GetAsync("k"), Is.Not.Null, "The entry is live before the clock advances.");

        clock.Advance(TimeSpan.FromMinutes(6));

        Assert.That(
            await cache.GetAsync("k"),
            Is.Null,
            "Advancing only the registered TimeProvider expired the entry, so the resolved "
            + "cache used it rather than the system clock.");
    }
}
