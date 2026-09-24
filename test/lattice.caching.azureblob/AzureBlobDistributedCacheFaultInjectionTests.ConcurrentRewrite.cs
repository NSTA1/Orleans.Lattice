using System.Text;
using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// A second replica rewrites an entry in the window between a read and the
/// best-effort write that read triggers. The interceptor performs that rewrite
/// through an un-intercepted cache on the same container at the exact moment the
/// sliding renewal or the expired-entry eviction is about to be sent, so the race
/// is deterministic. Each case asserts on what the rewriting replica observes
/// afterwards, which is where an unconditional write does its damage.
/// </summary>
public sealed partial class AzureBlobDistributedCacheFaultInjectionTests
{
    private AzureBlobDistributedCache CreateUninterceptedCache() =>
        new(new LatticeAzureBlobCacheOptions
        {
            ServiceClient = AzuriteEmulator.CreateServiceClient(),
            ContainerName = _containerName,
        }.BuildContainerClient(), string.Empty, _clock);

    [Test]
    public async Task GetAsync_sliding_renewal_does_not_overwrite_the_expiry_of_a_concurrent_rewrite()
    {
        var reader = CreateCache();
        var writer = CreateUninterceptedCache();
        await reader.SetAsync("k", Encoding.UTF8.GetBytes("old"), new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(10),
        });

        var rewritten = Encoding.UTF8.GetBytes("rewritten");
        var rewrites = 0;
        _handler.Interceptor = async (request, cancellationToken) =>
        {
            if (InterceptingHttpHandler.IsSetMetadata(request) && Interlocked.Increment(ref rewrites) == 1)
            {
                await writer.SetAsync("k", rewritten, new DistributedCacheEntryOptions
                {
                    AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(2),
                }, cancellationToken);
            }

            return null;
        };

        _clock.Advance(TimeSpan.FromMinutes(1));
        await reader.GetAsync("k");
        _handler.Interceptor = null;

        // Past the old entry's ten-minute sliding window, well inside the
        // rewrite's own two-hour cap.
        _clock.Advance(TimeSpan.FromMinutes(30));
        var observed = await writer.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(rewrites, Is.EqualTo(1), "The rewrite must have landed between the read and its slide.");
            Assert.That(observed, Is.EqualTo(rewritten),
                "The slide stamped the superseded entry's sliding window onto the rewritten value, "
                + "so a value written to live two hours expired after eleven minutes.");
        });
    }

    [Test]
    public async Task RefreshAsync_sliding_renewal_does_not_overwrite_the_expiry_of_a_concurrent_rewrite()
    {
        var reader = CreateCache();
        var writer = CreateUninterceptedCache();
        await reader.SetAsync("k", Encoding.UTF8.GetBytes("old"), new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(10),
        });

        var rewritten = Encoding.UTF8.GetBytes("rewritten");
        var rewrites = 0;
        _handler.Interceptor = async (request, cancellationToken) =>
        {
            if (InterceptingHttpHandler.IsSetMetadata(request) && Interlocked.Increment(ref rewrites) == 1)
            {
                await writer.SetAsync("k", rewritten, new DistributedCacheEntryOptions(), cancellationToken);
            }

            return null;
        };

        _clock.Advance(TimeSpan.FromMinutes(1));
        await reader.RefreshAsync("k");
        _handler.Interceptor = null;

        _clock.Advance(TimeSpan.FromMinutes(30));
        var observed = await writer.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(rewrites, Is.EqualTo(1), "The rewrite must have landed between the read and its slide.");
            Assert.That(observed, Is.EqualTo(rewritten),
                "A rewrite with no expiry must never inherit the superseded entry's sliding window.");
        });
    }

    [Test]
    public async Task GetAsync_eviction_of_an_expired_entry_does_not_delete_a_concurrent_rewrite()
    {
        var reader = CreateCache();
        var writer = CreateUninterceptedCache();
        await reader.SetAsync("k", Encoding.UTF8.GetBytes("old"), new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),
        });

        var fresh = Encoding.UTF8.GetBytes("fresh");
        var rewrites = 0;
        _handler.Interceptor = async (request, cancellationToken) =>
        {
            if (InterceptingHttpHandler.IsBlobDelete(request) && Interlocked.Increment(ref rewrites) == 1)
            {
                await writer.SetAsync("k", fresh, new DistributedCacheEntryOptions(), cancellationToken);
            }

            return null;
        };

        _clock.Advance(TimeSpan.FromMinutes(6));
        var read = await reader.GetAsync("k");
        _handler.Interceptor = null;

        var observed = await writer.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(rewrites, Is.EqualTo(1), "The rewrite must have landed between the read and its eviction.");
            Assert.That(read, Is.Null, "The entry the reader saw was expired, so its own read is a miss.");
            Assert.That(observed, Is.EqualTo(fresh),
                "Evicting the expired version deleted the fresh entry another replica had just written.");
        });
    }

    [Test]
    public async Task RefreshAsync_eviction_of_an_expired_entry_does_not_delete_a_concurrent_rewrite()
    {
        var reader = CreateCache();
        var writer = CreateUninterceptedCache();
        await reader.SetAsync("k", Encoding.UTF8.GetBytes("old"), new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),
        });

        var fresh = Encoding.UTF8.GetBytes("fresh");
        var rewrites = 0;
        _handler.Interceptor = async (request, cancellationToken) =>
        {
            if (InterceptingHttpHandler.IsBlobDelete(request) && Interlocked.Increment(ref rewrites) == 1)
            {
                await writer.SetAsync("k", fresh, new DistributedCacheEntryOptions(), cancellationToken);
            }

            return null;
        };

        _clock.Advance(TimeSpan.FromMinutes(6));
        await reader.RefreshAsync("k");
        _handler.Interceptor = null;

        var observed = await writer.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(rewrites, Is.EqualTo(1), "The rewrite must have landed between the read and its eviction.");
            Assert.That(observed, Is.EqualTo(fresh),
                "Evicting the expired version deleted the fresh entry another replica had just written.");
        });
    }

    [Test]
    public async Task GetAsync_still_slides_an_entry_nobody_rewrote()
    {
        // Positive control for the conditional renewal: with no concurrent writer
        // the ETag matches, the slide lands, and the entry outlives its original
        // window. Without this, a condition that always failed would pass the
        // rewrite cases above while silently disabling sliding expiration.
        var cache = CreateCache();
        var value = Encoding.UTF8.GetBytes("payload");
        await cache.SetAsync("k", value, new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(10),
        });

        _clock.Advance(TimeSpan.FromMinutes(8));
        Assert.That(await cache.GetAsync("k"), Is.EqualTo(value));

        _clock.Advance(TimeSpan.FromMinutes(8));
        Assert.That(await cache.GetAsync("k"), Is.EqualTo(value),
            "Sixteen minutes after the write, the entry is alive only because the read at eight minutes slid it.");
    }
}
