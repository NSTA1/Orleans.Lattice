using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the runtime <see cref="LatticeOptions.MaxCacheValueBytes"/>
/// override applied through the control-plane
/// <see cref="ILatticeRegistry.SetMaxCacheValueBytesAsync"/> seam and honoured
/// by <see cref="Grains.LeafCacheGrain"/> on a warm activation.
/// <para>
/// The read-through cache re-resolves its payload budget on every refresh, so a
/// per-tree override established at runtime takes effect on the next read
/// without re-activating the cache grain. These tests assert the user-visible
/// invariant that survives eviction: every written value still reads back
/// correctly through the cache (an evicted payload delegates to the primary
/// leaf), both before and after the override is changed on a warm activation.
/// </para>
/// <para>
/// Written for the epic coordinator's warm-activation re-read verification;
/// marked <c>Integration</c> so it is excluded from the unit-only inner loop.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafCacheBudgetOverrideIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeRegistry Registry =>
        _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task Runtime_override_is_honoured_on_warm_activation_without_losing_values()
    {
        var treeId = $"cache-budget-override-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Warm the tree and its cache activation with an initial read.
        await router.SetAsync("seed", Encoding.UTF8.GetBytes("seed-value"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("seed"))!), Is.EqualTo("seed-value"));

        // Establish a tight per-tree runtime override AFTER the cache activation
        // is already warm. The cache must pick it up on its next refresh.
        await Registry.SetMaxCacheValueBytesAsync(treeId, 32);

        // Write several payloads whose aggregate far exceeds the 32-byte cap so
        // the LRU budget must evict resident payloads down to metadata sentinels.
        var keys = new List<string>();
        for (var i = 0; i < 8; i++)
        {
            var key = $"k{i}";
            keys.Add(key);
            await router.SetAsync(key, Encoding.UTF8.GetBytes($"payload-value-{i:D3}"));
        }

        // The user-visible invariant: every value still reads back correctly
        // through the cache on the warm activation, because an evicted payload
        // delegates to the authoritative primary leaf.
        foreach (var key in keys)
        {
            var value = await router.GetAsync(key);
            Assert.That(value, Is.Not.Null, $"key '{key}' must resolve through the cache under the runtime override");
            Assert.That(Encoding.UTF8.GetString(value!), Does.StartWith("payload-value-"));
        }
    }

    [Test]
    public async Task Clearing_override_on_warm_activation_restores_unbounded_behaviour()
    {
        var treeId = $"cache-budget-clear-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        await Registry.SetMaxCacheValueBytesAsync(treeId, 16);
        await router.SetAsync("a", Encoding.UTF8.GetBytes("alpha-payload"));
        await router.SetAsync("b", Encoding.UTF8.GetBytes("bravo-payload"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("a"))!), Is.EqualTo("alpha-payload"));

        // Clear the override on the warm activation - subsequent refreshes fall
        // back to the silo-wide static option (unbounded by default).
        await Registry.SetMaxCacheValueBytesAsync(treeId, null);

        await router.SetAsync("c", Encoding.UTF8.GetBytes("charlie-payload"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("a"))!), Is.EqualTo("alpha-payload"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("b"))!), Is.EqualTo("bravo-payload"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("c"))!), Is.EqualTo("charlie-payload"));
    }
}
