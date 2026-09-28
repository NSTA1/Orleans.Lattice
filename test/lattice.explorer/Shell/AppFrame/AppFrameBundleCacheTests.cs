using Orleans.Lattice.Explorer.Shell.Framing;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.AppFrameTestData;

namespace Orleans.Lattice.Explorer.Tests.Shell.Framing;

/// <summary>The bundle cache: bounded, copy-on-admit, and keyed per bundle identity.</summary>
[TestFixture]
public sealed class AppFrameBundleCacheTests
{
    private static async Task<AppFrameLaunch> LaunchAsync()
    {
        var loader = new AppFrameBundleLoader(Workspace(), new AppFrameBundleCache(), Microsoft.Extensions.Logging.Abstractions.NullLogger<AppFrameBundleLoader>.Instance);
        return (await loader.AuthorizeAsync(Slug)).Launch!;
    }

    [Test]
    public async Task TryAdd_copies_so_the_caller_cannot_mutate_a_cached_asset()
    {
        var cache = new AppFrameBundleCache();
        var launch = await LaunchAsync();
        var bytes = new byte[] { 1, 2, 3 };

        cache.TryAdd(launch, Entry, bytes);
        bytes[0] = 9;

        Assert.Multiple(() =>
        {
            Assert.That(cache.TryGet(launch, Entry, out var cached), Is.True);
            Assert.That(cached.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(cache.TotalBytes, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task TryAdd_stops_admitting_at_the_bound()
    {
        var cache = new AppFrameBundleCache(maxTotalBytes: 4);
        var launch = await LaunchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(cache.TryAdd(launch, "a.png", new byte[3]), Is.True);
            Assert.That(cache.TryAdd(launch, "b.png", new byte[2]), Is.False);
            Assert.That(cache.TotalBytes, Is.EqualTo(3));
            Assert.That(cache.TryGet(launch, "b.png", out _), Is.False);
        });
    }

    [Test]
    public async Task TryAdd_a_duplicate_is_not_counted_twice()
    {
        var cache = new AppFrameBundleCache();
        var launch = await LaunchAsync();

        cache.TryAdd(launch, Entry, new byte[5]);
        var second = cache.TryAdd(launch, Entry, new byte[5]);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.False);
            Assert.That(cache.TotalBytes, Is.EqualTo(5));
            Assert.That(cache.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_zero_bound_disables_caching_and_a_negative_bound_throws()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new AppFrameBundleCache(0).MaxTotalBytes, Is.Zero);
            Assert.That(new AppFrameBundleCache().MaxTotalBytes, Is.EqualTo(AppFrameBundleCache.DefaultMaxTotalBytes));
            Assert.That(() => new AppFrameBundleCache(-1), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }
}
