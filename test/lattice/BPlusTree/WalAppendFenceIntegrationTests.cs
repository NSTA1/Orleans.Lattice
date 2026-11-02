using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the WAL append fence (epic #5605) on a live cluster.
/// Each test supersedes the live activation of a tree's root leaf exactly as a
/// stale duplicate would be superseded - a newer epoch persisted to the leaf's
/// epoch row and claimed at every WAL partition - and then drives one caller of
/// the leaf write surface. The superseded activation's next append is rejected,
/// it deactivates, and the caller's seam retries against a fresh activation, so
/// the caller sees exactly its 10.2.0 outcome: the write succeeds, with no new
/// exception.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalAppendFenceIntegrationTests
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

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainStorage Storage => SiloServices.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    private async Task<(ILattice Tree, Guid Leaf)> NewTreeAsync(string name)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(name, new TreeRegistryEntry
        {
            MaxLeafKeys = 1024,
            MaxInternalChildren = 16,
            ShardCount = 1,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(name);
        await tree.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));

        // A single-shard tree's root leaf has the id the shard root derives from its key.
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{name}/0"));
        return (tree, new Guid(hash.AsSpan(0, 16)));
    }

    /// <summary>
    /// Supersedes the leaf's live activation as a newer activation would: persists
    /// the next epoch to the leaf's epoch row and claims it at every partition.
    /// Returns the superseding epoch.
    /// </summary>
    private async Task<long> SupersedeAsync(string treeName, Guid leaf)
    {
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf);
        Assert.That(row.State.Epoch, Is.GreaterThan(0), "precondition: the live activation claimed an epoch with the gate open");
        var superseding = row.State.Epoch + 1;
        row.State.Epoch = superseding;
        await WalAppendFenceRows.WriteAsync(Storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(leaf), row);

        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(treeName);
        for (var partition = 0; partition < partitions; partition++)
        {
            var claim = await _cluster.Client.GetGrain<IWalShardGrain>($"{treeName}/{partition}")
                .ClaimAppendEpochAsync(leaf, superseding, CancellationToken.None);
            Assert.That(claim.Accepted, Is.True, $"the superseding claim at partition {partition}");
        }

        return superseding;
    }

    private async Task AssertSupersededActivationWasReplacedAsync(Guid leaf, long superseding)
    {
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf);
        Assert.That(row.State.Epoch, Is.GreaterThan(superseding),
            "the seam's retry reached a fresh activation, which claimed above the superseding epoch");
    }

    [Test]
    public async Task The_gate_is_open_on_a_cluster_of_capable_silos()
    {
        Assert.That(SiloServices.GetRequiredService<IWalAppendFenceGate>().IsOpen, Is.True);
        var (_, leaf) = await NewTreeAsync("fence-gate");
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf);
        Assert.That(row.State.Epoch, Is.GreaterThan(0), "a leaf claims its epoch eagerly before its first append");
    }

    [Test]
    public async Task A_set_on_a_superseded_activation_is_retried_and_succeeds()
    {
        var (tree, leaf) = await NewTreeAsync("fence-set");
        var superseding = await SupersedeAsync("fence-set", leaf);

        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        Assert.That(Encoding.UTF8.GetString((await tree.GetAsync("k"))!), Is.EqualTo("v"));
        Assert.That(Encoding.UTF8.GetString((await tree.GetAsync("seed"))!), Is.EqualTo("seed"),
            "the fresh activation replayed every acknowledged write");
        await AssertSupersededActivationWasReplacedAsync(leaf, superseding);
    }

    [Test]
    public async Task A_delete_on_a_superseded_activation_is_retried_and_succeeds()
    {
        var (tree, leaf) = await NewTreeAsync("fence-delete");
        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        var superseding = await SupersedeAsync("fence-delete", leaf);

        var deleted = await tree.DeleteAsync("k");
        var after = await tree.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True, "DeleteAsync returns what 10.2.0 returns");
            Assert.That(after, Is.Null);
        });
        await AssertSupersededActivationWasReplacedAsync(leaf, superseding);
    }

    [Test]
    public async Task A_crdt_delta_on_a_superseded_activation_is_retried_and_succeeds()
    {
        var (tree, leaf) = await NewTreeAsync("fence-crdt");
        await tree.GCounter("c").IncrementAsync("r", 2);
        var superseding = await SupersedeAsync("fence-crdt", leaf);

        await tree.GCounter("c").IncrementAsync("r", 3);

        Assert.That(await tree.GCounter("c").ValueAsync(), Is.EqualTo(5));
        await AssertSupersededActivationWasReplacedAsync(leaf, superseding);
    }

    [Test]
    public async Task An_atomic_write_on_a_superseded_activation_commits()
    {
        var (tree, leaf) = await NewTreeAsync("fence-atomic");
        var superseding = await SupersedeAsync("fence-atomic", leaf);

        await tree.SetManyAtomicAsync(new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
        });

        var a = await tree.GetAsync("a");
        var b = await tree.GetAsync("b");
        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(a!), Is.EqualTo("1"),
                "a fence rejection of a prepare is retried, never a nack: the saga commits as in 10.2.0");
            Assert.That(Encoding.UTF8.GetString(b!), Is.EqualTo("2"));
        });
        await AssertSupersededActivationWasReplacedAsync(leaf, superseding);
    }

    [Test]
    public async Task A_batch_write_on_a_superseded_activation_is_retried_and_succeeds()
    {
        var (tree, leaf) = await NewTreeAsync("fence-batch");
        var superseding = await SupersedeAsync("fence-batch", leaf);

        await tree.SetManyAsync(new List<KeyValuePair<string, byte[]>>
        {
            new("x", Encoding.UTF8.GetBytes("1")),
            new("y", Encoding.UTF8.GetBytes("2")),
        });

        Assert.That(Encoding.UTF8.GetString((await tree.GetAsync("y"))!), Is.EqualTo("2"));
        await AssertSupersededActivationWasReplacedAsync(leaf, superseding);
    }
}
