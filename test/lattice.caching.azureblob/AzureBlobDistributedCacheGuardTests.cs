using Microsoft.Extensions.Caching.Distributed;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Argument-guard unit tests for <see cref="AzureBlobDistributedCache"/>. Every
/// public entry point validates its arguments before touching storage, so these
/// tests need no emulator: a cache built over a lazily-constructed container
/// client throws on a bad argument without any network I/O.
/// </summary>
[TestFixture]
public sealed class AzureBlobDistributedCacheGuardTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";

    private static AzureBlobDistributedCache CreateCache()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ConnectionString = DevConnectionString,
            ContainerName = "guard-container",
        };
        return new AzureBlobDistributedCache(options.BuildContainerClient(), options.KeyPrefix, TimeProvider.System);
    }

    [Test]
    public void Constructor_throws_on_null_container()
    {
        Assert.Throws<ArgumentNullException>(
            () => new AzureBlobDistributedCache(null!, string.Empty, TimeProvider.System));
    }

    [Test]
    public void Constructor_throws_on_null_key_prefix()
    {
        var options = new LatticeAzureBlobCacheOptions { ConnectionString = DevConnectionString };

        Assert.Throws<ArgumentNullException>(
            () => new AzureBlobDistributedCache(options.BuildContainerClient(), null!, TimeProvider.System));
    }

    [Test]
    public void Constructor_throws_on_null_time_provider()
    {
        var options = new LatticeAzureBlobCacheOptions { ConnectionString = DevConnectionString };

        Assert.Throws<ArgumentNullException>(
            () => new AzureBlobDistributedCache(options.BuildContainerClient(), string.Empty, null!));
    }

    [Test]
    public void GetAsync_throws_on_null_key()
    {
        var cache = CreateCache();

        Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetAsync(null!));
    }

    [Test]
    public void SetAsync_throws_on_null_key()
    {
        var cache = CreateCache();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => cache.SetAsync(null!, [1], new DistributedCacheEntryOptions()));
    }

    [Test]
    public void SetAsync_throws_on_null_value()
    {
        var cache = CreateCache();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => cache.SetAsync("k", null!, new DistributedCacheEntryOptions()));
    }

    [Test]
    public void SetAsync_throws_on_null_options()
    {
        var cache = CreateCache();

        Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetAsync("k", [1], null!));
    }

    [Test]
    public void RefreshAsync_throws_on_null_key()
    {
        var cache = CreateCache();

        Assert.ThrowsAsync<ArgumentNullException>(() => cache.RefreshAsync(null!));
    }

    [Test]
    public void RemoveAsync_throws_on_null_key()
    {
        var cache = CreateCache();

        Assert.ThrowsAsync<ArgumentNullException>(() => cache.RemoveAsync(null!));
    }

    [Test]
    public void GetAsync_honours_a_pre_cancelled_token()
    {
        var cache = CreateCache();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => cache.GetAsync("k", cts.Token));
    }

    [Test]
    public void SetAsync_honours_a_pre_cancelled_token()
    {
        var cache = CreateCache();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            () => cache.SetAsync("k", [1], new DistributedCacheEntryOptions(), cts.Token));
    }

    [Test]
    public void RefreshAsync_honours_a_pre_cancelled_token()
    {
        var cache = CreateCache();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => cache.RefreshAsync("k", cts.Token));
    }

    [Test]
    public void RemoveAsync_honours_a_pre_cancelled_token()
    {
        var cache = CreateCache();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => cache.RemoveAsync("k", cts.Token));
    }
}
