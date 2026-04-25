using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end verification that the internal <see cref="ISystemLattice"/>
/// bypass works through a live cluster: user-visible
/// <see cref="ILattice"/> rejects reserved ids, while library-internal
/// <see cref="ISystemLattice"/> resolution round-trips reads/writes
/// against the same activation. Also exercises the registry's
/// self-hosted storage path, which now flows through
/// <see cref="ISystemLattice"/> and must continue to behave normally
/// after the switchover.
/// </summary>
[TestFixture]
public class SystemLatticeIntegrationTests
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
    public void Public_ILattice_rejects_reserved_prefix()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("_lattice_replog_integration");

        Assert.ThrowsAsync<InvalidOperationException>(
            () => tree.SetAsync("k", Encoding.UTF8.GetBytes("v")));
        Assert.ThrowsAsync<InvalidOperationException>(() => tree.GetAsync("k"));
    }

    [Test]
    public async Task Internal_ISystemLattice_bypass_round_trips_a_value()
    {
        var tree = _cluster.GrainFactory.GetGrain<ISystemLattice>("_lattice_replog_roundtrip");
        var value = Encoding.UTF8.GetBytes("bypass-ok");

        await tree.SetAsync("k", value);
        var read = await tree.GetAsync("k");

        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("bypass-ok"));

        var existed = await tree.DeleteAsync("k");
        Assert.That(existed, Is.True);
        Assert.That(await tree.ExistsAsync("k"), Is.False);
    }

    [Test]
    public async Task Internal_ISystemLattice_keys_enumeration_works()
    {
        var tree = _cluster.GrainFactory.GetGrain<ISystemLattice>("_lattice_replog_keys");
        for (int i = 0; i < 5; i++)
        {
            await tree.SetAsync($"k-{i:D2}", [(byte)i]);
        }

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.That(keys, Has.Count.EqualTo(5));
        Assert.That(keys, Is.EqualTo(new[] { "k-00", "k-01", "k-02", "k-03", "k-04" }));
    }

    [Test]
    public async Task Internal_ISystemLattice_entries_enumeration_works()
    {
        var tree = _cluster.GrainFactory.GetGrain<ISystemLattice>("_lattice_replog_entries");
        for (int i = 0; i < 5; i++)
        {
            await tree.SetAsync($"k-{i:D2}", [(byte)i]);
        }

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync())
            entries.Add(e);

        Assert.That(entries, Has.Count.EqualTo(5));
        Assert.That(entries[0].Key, Is.EqualTo("k-00"));
        Assert.That(entries[4].Key, Is.EqualTo("k-04"));
        Assert.That(entries[2].Value, Is.EqualTo(new byte[] { 2 }));
    }

    [Test]
    public async Task Registry_self_hosted_storage_still_round_trips()
    {
        // The registry now resolves its backing tree via ISystemLattice. This
        // test confirms the switchover left the existing registry surface
        // behaving identically from the public caller's perspective.
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(
            LatticeConstants.RegistryTreeId);

        const string treeId = "registry-self-host-smoke";
        await registry.RegisterAsync(treeId);

        Assert.That(await registry.ExistsAsync(treeId), Is.True);
        var entry = await registry.GetEntryAsync(treeId);
        Assert.That(entry, Is.Not.Null);

        await registry.UnregisterAsync(treeId);
        Assert.That(await registry.ExistsAsync(treeId), Is.False);
    }
}
