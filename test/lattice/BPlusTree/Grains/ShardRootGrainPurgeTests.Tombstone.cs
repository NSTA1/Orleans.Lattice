using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The purge tombstone (issue #4503): a purged shard keeps refusing whoever still
/// addresses its copy unless the registry names the copy live again.
/// </summary>
public sealed partial class ShardRootGrainPurgeTests
{
    private const string PurgedTreeId = "purge-tree";

    private static ILatticeRegistry RegistryOf(PurgeHarness harness)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        harness.Factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId, Arg.Any<string?>()).Returns(registry);
        return registry;
    }

    private static async Task<T> StampedAsync<T>(string logicalTreeId, Func<Task<T>> call)
    {
        var key = LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey;
        RequestContext.Set(key, logicalTreeId);
        try
        {
            return await call();
        }
        finally
        {
            RequestContext.Remove(key);
        }
    }

    [Test]
    public async Task PurgeAsync_leaves_only_a_purge_tombstone()
    {
        var harness = new PurgeHarness();
        harness.State.State.IsRegistered = true;
        harness.State.State.IsDeleted = true;

        await harness.Grain.PurgeAsync();

        var purged = harness.State.State;
        Assert.Multiple(() =>
        {
            Assert.That(purged.IsPurged, Is.True);
            Assert.That(purged.IsDeleted, Is.False);
            Assert.That(purged.IsRegistered, Is.False);
            Assert.That(purged.RootNodeId, Is.Null);
            Assert.That(purged.ShadowForward, Is.Null, "the tombstone mirrors nowhere");
        });
    }

    [Test]
    public async Task A_routed_call_whose_tree_resolves_elsewhere_is_refused_as_stale()
    {
        var harness = new PurgeHarness();
        await harness.Grain.PurgeAsync();
        RegistryOf(harness).ResolveAsync("logical").Returns("live-copy");

        var refusal = Assert.ThrowsAsync<StaleTreeRoutingException>(
            () => StampedAsync("logical", () => harness.Grain.GetAsync("k")));

        Assert.That(refusal!.LogicalTreeId, Is.EqualTo("logical"));
        Assert.That(refusal.StalePhysicalTreeId, Is.EqualTo(PurgedTreeId));
        Assert.That(refusal.DestinationPhysicalTreeId, Is.EqualTo("live-copy"));
        Assert.That(harness.State.State.IsPurged, Is.True);
        Assert.That(harness.State.State.RootNodeId, Is.Null);
    }

    [Test]
    public async Task An_unrouted_read_of_a_copy_the_registry_aliases_elsewhere_answers_empty_and_seeds_nothing()
    {
        var harness = new PurgeHarness();
        await harness.Grain.PurgeAsync();
        RegistryOf(harness).GetEntryAsync(PurgedTreeId)
            .Returns(new TreeRegistryEntry { PhysicalTreeId = "purge-tree/resized/op" });

        Assert.That(await harness.Grain.GetAsync("k"), Is.Null);
        Assert.That(harness.State.State.RootNodeId, Is.Null);
        Assert.That(harness.State.State.IsPurged, Is.True);
    }

    [Test]
    public async Task An_unrouted_operation_on_an_unregistered_purged_copy_is_refused_as_purged()
    {
        var harness = new PurgeHarness();
        await harness.Grain.PurgeAsync();
        RegistryOf(harness).GetEntryAsync(PurgedTreeId).Returns((TreeRegistryEntry?)null);

        var refusal = Assert.ThrowsAsync<LatticeTreePurgedException>(() => harness.Grain.WarmUpAsync());

        Assert.That(refusal!.PhysicalTreeId, Is.EqualTo(PurgedTreeId));
        Assert.That(refusal, Is.InstanceOf<InvalidOperationException>());
        Assert.That(refusal, Is.InstanceOf<ILatticeDomainFault>(),
            "a broad InvalidOperationException clause written for a misuse must be able to decline it");
        Assert.That(harness.State.State.RootNodeId, Is.Null);
    }

    [Test]
    public async Task A_routed_write_whose_tree_resolves_here_reuses_the_id_and_lifts_the_tombstone()
    {
        var harness = new PurgeHarness();
        await harness.Grain.PurgeAsync();
        var registry = RegistryOf(harness);
        registry.ResolveAsync(PurgedTreeId).Returns(PurgedTreeId);
        registry.ExistsAsync(PurgedTreeId).Returns(true);
        var leafContext = Substitute.For<IGrainContext>();
        leafContext.GrainId.Returns(GrainId.Create("leaf", "seeded"));
        var leaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)leaf).GrainContext.Returns(leafContext);
        harness.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>(), Arg.Any<string?>()).Returns(leaf);

        await StampedAsync(PurgedTreeId, async () => { await harness.Grain.WarmUpAsync(); return true; });

        Assert.That(harness.State.State.IsPurged, Is.False);
        Assert.That(harness.State.State.RootNodeId, Is.Not.Null);
    }
}
