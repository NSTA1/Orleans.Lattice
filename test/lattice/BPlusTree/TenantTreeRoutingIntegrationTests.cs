using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end verification that a segmented tenant tree id
/// (<c>t/{tenantId}/{name}</c>, composed by <see cref="LatticeTenantTrees.Compose"/>)
/// routes, shards, and round-trips through a live cluster exactly like any other
/// opaque tree id, and that the reserved <c>t/</c> namespace is guarded on the
/// public <see cref="ILattice"/> data plane just as <c>sys-</c> is: a direct
/// user-origin write is refused, while a first-party write under a
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope is permitted.
/// The final fixture proves legacy bare-id behaviour is unchanged by the new
/// reserved namespace (default-tenant adoption is non-destructive).
/// </summary>
[TestFixture]
[Category("Integration")]
public class TenantTreeRoutingIntegrationTests
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
    public Task OneTimeTearDown() => _fixture.DisposeAsync();

    [Test]
    public void Public_ILattice_rejects_direct_user_write_to_tenant_prefix()
    {
        var treeId = LatticeTenantTrees.Compose(TenantId.Parse("acme"), "orders-reject");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => tree.SetAsync("k", Encoding.UTF8.GetBytes("v")));
    }

    [Test]
    public async Task System_origin_write_to_tenant_tree_round_trips()
    {
        var treeId = LatticeTenantTrees.Compose(TenantId.Parse("acme"), "orders-roundtrip");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var value = Encoding.UTF8.GetBytes("tenant-ok");

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await tree.SetAsync("k", value);

            var read = await tree.GetAsync("k");
            Assert.That(read, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("tenant-ok"));

            Assert.That(await tree.ExistsAsync("k"), Is.True);

            var existed = await tree.DeleteAsync("k");
            Assert.That(existed, Is.True);
            Assert.That(await tree.ExistsAsync("k"), Is.False);
        }
    }

    [Test]
    public async Task Segmented_tenant_id_enumerates_like_a_normal_tree()
    {
        var treeId = LatticeTenantTrees.Compose(TenantId.Parse("acme"), "orders-enumerate");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await tree.SetAsync("a", Encoding.UTF8.GetBytes("1"));
            await tree.SetAsync("b", Encoding.UTF8.GetBytes("2"));
            await tree.SetAsync("c", Encoding.UTF8.GetBytes("3"));

            Assert.That(await tree.CountAsync(), Is.EqualTo(3));

            var keys = new List<string>();
            await foreach (var k in tree.KeysAsync())
                keys.Add(k);
            Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));

            var entries = new List<string>();
            await foreach (var e in tree.ScanEntriesAsync())
                entries.Add($"{e.Key}={Encoding.UTF8.GetString(e.Value)}");
            Assert.That(entries, Is.EqualTo(new[] { "a=1", "b=2", "c=3" }));
        }
    }

    [Test]
    public async Task Two_tenants_with_the_same_local_name_are_isolated_trees()
    {
        var acmeId = LatticeTenantTrees.Compose(TenantId.Parse("acme"), "orders-shared");
        var globexId = LatticeTenantTrees.Compose(TenantId.Parse("globex"), "orders-shared");
        var acme = _cluster.GrainFactory.GetGrain<ILattice>(acmeId);
        var globex = _cluster.GrainFactory.GetGrain<ILattice>(globexId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await acme.SetAsync("k", Encoding.UTF8.GetBytes("acme-value"));
            await globex.SetAsync("k", Encoding.UTF8.GetBytes("globex-value"));

            Assert.That(Encoding.UTF8.GetString((await acme.GetAsync("k"))!), Is.EqualTo("acme-value"));
            Assert.That(Encoding.UTF8.GetString((await globex.GetAsync("k"))!), Is.EqualTo("globex-value"));
        }
    }

    [Test]
    public async Task Legacy_bare_id_round_trips_unchanged_after_reserving_tenant_namespace()
    {
        // The whole point of default-tenant adoption is non-destructiveness: a
        // bare, unsegmented legacy id keeps behaving byte-for-byte as before the
        // t/ namespace existed. No system-origin scope is required.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("legacy-bare-tree");

        await tree.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await tree.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        Assert.That(Encoding.UTF8.GetString((await tree.GetAsync("a"))!), Is.EqualTo("1"));
        Assert.That(await tree.CountAsync(), Is.EqualTo(2));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));

        Assert.That(await tree.DeleteAsync("a"), Is.True);
        Assert.That(await tree.GetAsync("a"), Is.Null);
    }
}
