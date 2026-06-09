using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the WAL placement resolution surface of
/// <see cref="LatticeOptionsResolver"/>: <c>ResolveWalProvider</c> (fail-closed
/// catalog lookup with the legacy default-key compatibility path) and
/// <c>GetWalPlacementSnapshotAsync</c> (fresh registry read, system-tree
/// short-circuit).
/// </summary>
[TestFixture]
public sealed class LatticeOptionsResolverWalProviderTests
{
    private static (LatticeOptionsResolver Resolver, ILatticeRegistry Registry, IWalStorageProviderCatalog Catalog) Build(
        LatticeOptions? options = null,
        IWalStorageProviderCatalog? catalog = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetWalPlacementAsync(Arg.Any<string>()).Returns(Task.FromResult(WalPlacementPin.Create()));

        catalog ??= Substitute.For<IWalStorageProviderCatalog>();
        return (new LatticeOptionsResolver(factory, monitor, logger: null, walProviderCatalog: catalog), registry, catalog);
    }

    [Test]
    public void ResolveWalProvider_default_key_uses_catalog_baseline_when_no_legacy_resolver()
    {
        var baseline = Substitute.For<IWalStorageProvider>();
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out Arg.Any<IWalStorageProvider>())
            .Returns(ci => { ci[1] = baseline; return true; });
        var (resolver, _, _) = Build(catalog: catalog);

        var (provider, key) = resolver.ResolveWalProvider("tree-a", WalPlacementPin.Create(), 0);

        Assert.That(provider, Is.SameAs(baseline));
        Assert.That(key, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
    }

    [Test]
    public void ResolveWalProvider_default_key_honours_legacy_per_tree_resolver()
    {
        var legacy = Substitute.For<IWalStorageProvider>();
        var options = new LatticeOptions { WalStorageProvider = _ => legacy };
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        var (resolver, _, _) = Build(options, catalog);

        var (provider, key) = resolver.ResolveWalProvider("tree-a", WalPlacementPin.Create(), 0);

        Assert.That(provider, Is.SameAs(legacy));
        Assert.That(key, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        // The legacy default-key path must not consult the catalog.
        catalog.DidNotReceiveWithAnyArgs().TryGet(default!, out Arg.Any<IWalStorageProvider>());
    }

    [Test]
    public void ResolveWalProvider_named_key_resolves_through_catalog()
    {
        var secondary = Substitute.For<IWalStorageProvider>();
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet("secondary", out Arg.Any<IWalStorageProvider>())
            .Returns(ci => { ci[1] = secondary; return true; });
        var (resolver, _, _) = Build(catalog: catalog);
        var pin = WalPlacementPin.Create().WithPartition(0, "secondary", 1);

        var (provider, key) = resolver.ResolveWalProvider("tree-a", pin, 0);

        Assert.That(provider, Is.SameAs(secondary));
        Assert.That(key, Is.EqualTo("secondary"));
    }

    [Test]
    public void ResolveWalProvider_named_key_fails_closed_when_catalog_missing_key()
    {
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet("secondary", out Arg.Any<IWalStorageProvider>()).Returns(false);
        var (resolver, _, _) = Build(catalog: catalog);
        var pin = WalPlacementPin.Create().WithPartition(2, "secondary", 1);

        Assert.That(() => resolver.ResolveWalProvider("tree-a", pin, 2),
            Throws.TypeOf<LatticeWalProviderMissingException>()
                .With.Property(nameof(LatticeWalProviderMissingException.ProviderKey)).EqualTo("secondary")
                .And.Property(nameof(LatticeWalProviderMissingException.Partition)).EqualTo(2));
    }

    [Test]
    public void ResolveWalProvider_without_catalog_throws_invalid_operation()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var factory = Substitute.For<IGrainFactory>();
        var resolver = new LatticeOptionsResolver(factory, monitor);

        Assert.That(() => resolver.ResolveWalProvider("tree-a", WalPlacementPin.Create(), 0),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task GetWalPlacementSnapshotAsync_system_tree_returns_default_pin_without_registry()
    {
        var (resolver, registry, _) = Build();

        var pin = await resolver.GetWalPlacementSnapshotAsync(LatticeConstants.SystemTreePrefix + "trees");

        Assert.That(pin.Version, Is.EqualTo(0));
        await registry.DidNotReceiveWithAnyArgs().GetWalPlacementAsync(default!);
    }

    [Test]
    public async Task GetWalPlacementSnapshotAsync_user_tree_reads_registry()
    {
        var (resolver, registry, _) = Build();
        registry.GetWalPlacementAsync("tree-a")
            .Returns(Task.FromResult(WalPlacementPin.Create().WithPartition(0, "secondary", 5)));

        var pin = await resolver.GetWalPlacementSnapshotAsync("tree-a");

        Assert.That(pin.Version, Is.EqualTo(5));
        Assert.That(pin.ResolveKey(0), Is.EqualTo("secondary"));
        await registry.Received(1).GetWalPlacementAsync("tree-a");
    }
}
