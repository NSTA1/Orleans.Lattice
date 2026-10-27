using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public sealed class LatticeOptionsResolverWalActivationTests
{
    private static (LatticeOptionsResolver Resolver, ILatticeRegistry Registry, LatticeOptions Options) Create()
    {
        var options = new LatticeOptions { WalPartitions = 8 };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var registry = Substitute.For<ILatticeRegistry>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (new LatticeOptionsResolver(factory, monitor), registry, options);
    }

    [Test]
    public async Task Cold_activation_reads_one_entry_for_provenance_and_partition_pin()
    {
        var (resolver, registry, _) = Create();
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry { DerivedFrom = "owner", WalPartitions = 4 });

        Assert.That(await resolver.ResolveWalActivationMetadataAsync("copy"), Is.EqualTo(("owner", 4)));
        Assert.That(await resolver.GetWalPartitionsAsync("copy"), Is.EqualTo(4));
        await registry.Received(1).GetEntryAsync("copy");
        Assert.That(registry.ReceivedCalls().All(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetEntryAsync)), Is.True);
    }

    [Test]
    public async Task Warm_activation_refreshes_reused_identity_but_keeps_the_first_partition_pin()
    {
        var (resolver, registry, _) = Create();
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry { DerivedFrom = "owner", WalPartitions = 4 });
        Assert.That(await resolver.ResolveWalActivationMetadataAsync("copy"), Is.EqualTo(("owner", 4)));
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry { WalPartitions = 16 });

        Assert.That(await resolver.ResolveWalActivationMetadataAsync("copy"), Is.EqualTo(("copy", 4)));
        await registry.Received(2).GetEntryAsync("copy");
    }

    [Test]
    public async Task Purged_row_keeps_retired_provenance_without_registering_it_again()
    {
        var (resolver, registry, _) = Create();
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry { DerivedFrom = "owner", WalPartitions = 4 });
        await resolver.ResolveMetricTreeIdAsync("copy");
        registry.GetEntryAsync("copy").Returns((TreeRegistryEntry?)null);
        registry.ClearReceivedCalls();

        Assert.That(await resolver.ResolveWalActivationMetadataAsync("copy"), Is.EqualTo(("owner", 8)));
        Assert.That(registry.ReceivedCalls().Select(c => c.GetMethodInfo().Name),
            Is.EqualTo(new[] { nameof(ILatticeRegistry.GetEntryAsync) }));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Missing_or_unpinned_row_caches_first_live_partition_value(bool missing)
    {
        var (resolver, registry, options) = Create();
        registry.GetEntryAsync("tree").Returns(missing ? null : new TreeRegistryEntry());

        Assert.That(await resolver.ResolveWalActivationMetadataAsync("tree"), Is.EqualTo(("tree", 8)));
        options.WalPartitions = 16;
        Assert.That(await resolver.ResolveWalActivationMetadataAsync("tree"), Is.EqualTo(("tree", 8)));
        Assert.That(await resolver.GetWalPartitionsAsync("tree"), Is.EqualTo(8));
        await registry.Received(2).GetEntryAsync("tree");
    }

    [Test]
    public async Task Racing_partition_read_preserves_the_existing_cache_winner()
    {
        var (resolver, registry, _) = Create();
        var pending = new TaskCompletionSource<TreeRegistryEntry?>(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.GetEntryAsync("tree").Returns(pending.Task);
        var activation = resolver.ResolveWalActivationMetadataAsync("tree");
        resolver.Resolve("tree", new TreeRegistryEntry { WalPartitions = 4 });
        pending.SetResult(new TreeRegistryEntry { DerivedFrom = "owner", WalPartitions = 16 });

        Assert.That(await activation, Is.EqualTo(("owner", 4)));
    }

    [Test]
    public async Task System_activation_bypasses_the_registry()
    {
        var (resolver, registry, _) = Create();
        Assert.That(await resolver.ResolveWalActivationMetadataAsync(LatticeConstants.RegistryTreeId),
            Is.EqualTo((LatticeConstants.RegistryTreeId, LatticeConstants.DefaultSystemTreeWalPartitions)));
        Assert.That(registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void Registry_failure_is_not_replaced_with_default_metadata()
    {
        var (resolver, registry, _) = Create();
        registry.GetEntryAsync("tree").Returns<Task<TreeRegistryEntry?>>(_ => throw new TimeoutException("unavailable"));
        Assert.ThrowsAsync<TimeoutException>(async () => await resolver.ResolveWalActivationMetadataAsync("tree"));
    }
}
