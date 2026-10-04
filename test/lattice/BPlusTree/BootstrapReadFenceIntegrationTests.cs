using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4526 on a real in-process cluster: while a tree's shards carry the
/// receiver bootstrap read fence, every read of the tree is refused with
/// <see cref="LatticeTreeBootstrappingException"/> and plain writes still apply;
/// no split can open on a fenced shard and no resize or undo can start on the
/// tree, each failing closed; and lifting the fence restores reads.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BootstrapReadFenceIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private const int ShardCount = 2;

    private async Task<ILattice> CreateTreeAsync(string treeName)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = ShardCount });
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        await tree.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await tree.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        return tree;
    }

    private async Task<TreeBootstrapReadFence.Shards> ShardsAsync(string treeName)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physical = await registry.ResolveAsync(treeName);
        var entry = await registry.GetEntryAsync(treeName);
        return new TreeBootstrapReadFence.Shards(physical, RoutedShardIndices.Resolve(ShardCount, entry?.ShardMap));
    }

    [Test]
    public async Task A_fenced_tree_refuses_every_read_admits_writes_and_reads_again_once_lifted()
    {
        var treeName = $"fence-reads-{Guid.NewGuid():N}";
        var tree = await CreateTreeAsync(treeName);
        var shards = await ShardsAsync(treeName);
        await TreeBootstrapReadFence.SetAsync(_cluster.GrainFactory, shards, fenced: true);

        var refusals = new List<(string Read, Exception? Fault)>();
        async Task Probe(string name, Func<Task> read)
        {
            try { await read(); refusals.Add((name, null)); }
            catch (Exception ex) { refusals.Add((name, ex)); }
        }

        await Probe("GetAsync", () => tree.GetAsync("a"));
        await Probe("GetManyAsync", () => tree.GetManyAsync(["a", "b"]));
        await Probe("ExistsAsync", () => tree.ExistsAsync("a"));
        await Probe("CountAsync", () => tree.CountAsync());
        await Probe("KeysAsync", async () => { await foreach (var _ in tree.KeysAsync()) { } });
        await Probe("GetOrSetAsync", () => tree.GetOrSetAsync("c", Encoding.UTF8.GetBytes("3")));

        await tree.SetAsync("d", Encoding.UTF8.GetBytes("written-while-fenced"));
        await tree.DeleteAsync("b");
        var fenced = await TreeBootstrapReadFence.FindFencedShardAsync(_cluster.GrainFactory, shards.PhysicalTreeId, shards.ShardIndices);

        await TreeBootstrapReadFence.SetAsync(_cluster.GrainFactory, shards, fenced: false);
        var after = await tree.GetManyAsync(["a", "b", "d"]);

        Assert.Multiple(() =>
        {
            foreach (var (read, fault) in refusals)
                Assert.That(fault, Is.InstanceOf<LatticeTreeBootstrappingException>(), $"{read} must be refused while fenced");
            Assert.That(fenced, Is.Not.Null);
            Assert.That(after.Keys, Is.EquivalentTo(new[] { "a", "d" }), "writes made while fenced are visible once lifted");
            Assert.That(Encoding.UTF8.GetString(after["d"]), Is.EqualTo("written-while-fenced"));
        });
    }

    [Test]
    public async Task A_fenced_shard_refuses_to_open_a_split()
    {
        var treeName = $"fence-split-{Guid.NewGuid():N}";
        await CreateTreeAsync(treeName);
        var shards = await ShardsAsync(treeName);
        await TreeBootstrapReadFence.SetAsync(_cluster.GrainFactory, shards, fenced: true);
        var source = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{shards.PhysicalTreeId}/0");

        var refused = Assert.ThrowsAsync<InvalidOperationException>(
            () => source.BeginSplitAsync(targetShardIndex: 1, movedSlots: [0], virtualShardCount: 64));

        await TreeBootstrapReadFence.SetAsync(_cluster.GrainFactory, shards, fenced: false);
        Assert.That(refused!.Message, Does.Contain("snapshot bootstrap"));
        Assert.That(await source.IsSplittingAsync(), Is.False);

        // Lifting the fence releases the hold: the same split now opens.
        await source.BeginSplitAsync(targetShardIndex: 1, movedSlots: [0], virtualShardCount: 64);
        Assert.That(await source.IsSplittingAsync(), Is.True);
        await source.AbortSplitAsync();
    }

    [Test]
    public async Task An_undo_of_a_completed_resize_is_refused_while_the_resized_copy_is_fenced()
    {
        var treeName = $"fence-undo-{Guid.NewGuid():N}";
        await CreateTreeAsync(treeName);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(64, 64);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (!await resize.IsIdleAsync())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("PRECONDITION: the resize did not complete");
            await Task.Delay(200);
        }

        var shards = await ShardsAsync(treeName);
        Assert.That(shards.PhysicalTreeId, Is.Not.EqualTo(treeName), "PRECONDITION: the tree routes to the resized copy");
        await TreeBootstrapReadFence.SetAsync(_cluster.GrainFactory, shards, fenced: true);

        var refused = Assert.ThrowsAsync<InvalidOperationException>(() => resize.UndoResizeAsync());
        var stillOnCopy = await _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(treeName);

        await TreeBootstrapReadFence.SetAsync(_cluster.GrainFactory, shards, fenced: false);
        Assert.Multiple(() =>
        {
            Assert.That(refused!.Message, Does.Contain("snapshot bootstrap"));
            Assert.That(stillOnCopy, Is.EqualTo(shards.PhysicalTreeId), "the refused undo must not have moved the tree");
        });
    }

    [Test]
    public async Task A_resize_of_a_fenced_tree_is_refused_and_leaves_no_resize_in_flight()
    {
        var treeName = $"fence-resize-{Guid.NewGuid():N}";
        await CreateTreeAsync(treeName);
        var shards = await ShardsAsync(treeName);
        await TreeBootstrapReadFence.SetAsync(_cluster.GrainFactory, shards, fenced: true);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);

        var refused = Assert.ThrowsAsync<InvalidOperationException>(() => resize.ResizeAsync(64, 64));
        var idle = await resize.IsIdleAsync();
        var holds = await resize.HoldsShardSplitsAsync();

        await TreeBootstrapReadFence.SetAsync(_cluster.GrainFactory, shards, fenced: false);
        Assert.Multiple(() =>
        {
            Assert.That(refused!.Message, Does.Contain("snapshot bootstrap"));
            Assert.That(idle, Is.True);
            Assert.That(holds, Is.False);
        });
    }

    [Test]
    public async Task The_blocker_check_reports_an_open_split_on_a_fenced_tree()
    {
        var treeName = $"fence-blocker-{Guid.NewGuid():N}";
        await CreateTreeAsync(treeName);
        var shards = await ShardsAsync(treeName);
        var source = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{shards.PhysicalTreeId}/0");
        await source.BeginSplitAsync(targetShardIndex: 1, movedSlots: [0], virtualShardCount: 64);

        var blocker = await TreeBootstrapReadFence.FindBlockerAsync(_cluster.GrainFactory, treeName, shards);
        await source.AbortSplitAsync();
        var clear = await TreeBootstrapReadFence.FindBlockerAsync(_cluster.GrainFactory, treeName, shards);

        Assert.Multiple(() =>
        {
            Assert.That(blocker, Does.Contain("shard 0"));
            Assert.That(clear, Is.Null);
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
