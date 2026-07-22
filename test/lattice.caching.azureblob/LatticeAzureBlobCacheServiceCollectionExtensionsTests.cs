using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Unit tests for
/// <see cref="LatticeAzureBlobCacheServiceCollectionExtensions.AddAzureBlobDistributedCache"/>:
/// the registration wires the options and resolves the blob-backed
/// <see cref="IDistributedCache"/> as the winning registration, without any
/// network I/O (the container client is built lazily and touches storage only on
/// first use).
/// </summary>
[TestFixture]
public sealed class LatticeAzureBlobCacheServiceCollectionExtensionsTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";

    [Test]
    public void AddAzureBlobDistributedCache_throws_on_null_services()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeAzureBlobCacheServiceCollectionExtensions.AddAzureBlobDistributedCache(null!, _ => { }));
    }

    [Test]
    public void AddAzureBlobDistributedCache_throws_on_null_configure()
    {
        Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAzureBlobDistributedCache(null!));
    }

    [Test]
    public void AddAzureBlobDistributedCache_resolves_the_blob_backed_cache()
    {
        using var provider = new ServiceCollection()
            .AddAzureBlobDistributedCache(o => o.ConnectionString = DevConnectionString)
            .BuildServiceProvider();

        var cache = provider.GetRequiredService<IDistributedCache>();

        Assert.That(cache.GetType().Name, Is.EqualTo("AzureBlobDistributedCache"));
    }

    [Test]
    public void AddAzureBlobDistributedCache_wins_over_an_earlier_memory_cache()
    {
        using var provider = new ServiceCollection()
            .AddDistributedMemoryCache()
            .AddAzureBlobDistributedCache(o => o.ConnectionString = DevConnectionString)
            .BuildServiceProvider();

        var cache = provider.GetRequiredService<IDistributedCache>();

        Assert.That(cache.GetType().Name, Is.EqualTo("AzureBlobDistributedCache"));
    }

    [Test]
    public void AddAzureBlobDistributedCache_applies_the_configure_callback_to_options()
    {
        using var provider = new ServiceCollection()
            .AddAzureBlobDistributedCache(o =>
            {
                o.ConnectionString = DevConnectionString;
                o.ContainerName = "custom";
                o.KeyPrefix = "tokens/";
            })
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeAzureBlobCacheOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.ContainerName, Is.EqualTo("custom"));
            Assert.That(options.KeyPrefix, Is.EqualTo("tokens/"));
        });
    }

    [Test]
    public void AddAzureBlobDistributedCache_registers_a_singleton()
    {
        using var provider = new ServiceCollection()
            .AddAzureBlobDistributedCache(o => o.ConnectionString = DevConnectionString)
            .BuildServiceProvider();

        var first = provider.GetRequiredService<IDistributedCache>();
        var second = provider.GetRequiredService<IDistributedCache>();

        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void Resolving_the_cache_throws_when_no_authentication_mode_is_configured()
    {
        using var provider = new ServiceCollection()
            .AddAzureBlobDistributedCache(_ => { })
            .BuildServiceProvider();

        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IDistributedCache>());
    }
}
