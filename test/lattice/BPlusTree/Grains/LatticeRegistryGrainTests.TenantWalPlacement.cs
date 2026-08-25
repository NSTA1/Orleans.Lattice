using System.Text.Json;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the T18 tree-placement seam wiring in
/// <see cref="LatticeRegistryGrain"/>: the grain seeds a tree's durable WAL
/// placement pin from the <see cref="ITreePlacementResolver"/> at first
/// registration only, so a baseline resolution leaves routing byte-for-byte
/// unchanged and a dedicated resolution pins every partition. Placement is
/// immutable for a tree's lifetime.
/// </summary>
public partial class LatticeRegistryGrainTests
{
    private static (LatticeRegistryGrain grain, ISystemLattice registryTree, ITreePlacementResolver resolver)
        CreateGrainWithResolver(ITreePlacementResolver resolver, LatticeOptions? options = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var registryTree = Substitute.For<ISystemLattice>();
        grainFactory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId).Returns(registryTree);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var grain = new LatticeRegistryGrain(grainFactory, optionsMonitor, resolver);
        return (grain, registryTree, resolver);
    }

    /// <summary>
    /// A substitute resolver that resolves <paramref name="treeId"/> synchronously
    /// to <paramref name="placement"/> and every other id to the baseline.
    /// </summary>
    private static ITreePlacementResolver SyncResolver(string treeId, TreePhysicalPlacement placement)
    {
        var resolver = Substitute.For<ITreePlacementResolver>();
        resolver.TryResolveForRegistration(Arg.Any<string>(), out Arg.Any<TreePhysicalPlacement>())
            .Returns(call =>
            {
                var id = (string)call[0];
                call[1] = string.Equals(id, treeId, StringComparison.Ordinal)
                    ? placement
                    : TreePhysicalPlacement.Default;
                return true;
            });
        return resolver;
    }

    private static WalPlacementPin? ReadWalPlacement(byte[] written)
    {
        var entry = JsonSerializer.Deserialize<TreeRegistryEntry>(written);
        return entry?.WalPlacement;
    }

    [Test]
    public async Task RegisterAsync_with_no_resolver_leaves_WalPlacement_null()
    {
        // The core CreateGrain path constructs the grain with no placement
        // resolver: registration must behave exactly as before T18 - a null pin.
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("legacy-tree").Returns(false);

        byte[]? written = null;
        await tree.SetAsync("legacy-tree", Arg.Do<byte[]>(b => written = b));

        await grain.RegisterAsync("legacy-tree");

        Assert.That(written, Is.Not.Null);
        Assert.That(ReadWalPlacement(written!), Is.Null);
    }

    [Test]
    public async Task RegisterAsync_with_baseline_resolution_leaves_WalPlacement_null()
    {
        // A resolver that returns the default key must leave the pin null so the
        // legacy per-tree LatticeOptions.WalStorageProvider resolver still wins.
        var resolver = SyncResolver("shared-tree", TreePhysicalPlacement.Default);
        var (grain, tree, _) = CreateGrainWithResolver(resolver);
        tree.ExistsAsync("shared-tree").Returns(false);

        byte[]? written = null;
        await tree.SetAsync("shared-tree", Arg.Do<byte[]>(b => written = b));

        await grain.RegisterAsync("shared-tree");

        Assert.That(written, Is.Not.Null);
        Assert.That(ReadWalPlacement(written!), Is.Null);
    }

    [Test]
    public async Task RegisterAsync_with_dedicated_resolution_pins_every_partition()
    {
        // A non-baseline key pins the pin's default key, which routes every
        // partition to the dedicated provider.
        var placement = new TreePhysicalPlacement { WalProviderKey = "wal-acme" };
        var resolver = SyncResolver("t/acme/orders", placement);
        var (grain, tree, _) = CreateGrainWithResolver(resolver);
        tree.ExistsAsync("t/acme/orders").Returns(false);

        byte[]? written = null;
        await tree.SetAsync("t/acme/orders", Arg.Do<byte[]>(b => written = b));

        await grain.RegisterAsync("t/acme/orders");

        Assert.That(written, Is.Not.Null);
        var pin = ReadWalPlacement(written!);
        Assert.That(pin, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(pin!.DefaultProviderKey, Is.EqualTo("wal-acme"));
            Assert.That(pin.Version, Is.EqualTo(0));
            Assert.That(pin.ResolveKey(0), Is.EqualTo("wal-acme"));
            Assert.That(pin.ResolveKey(7), Is.EqualTo("wal-acme"));
        });
    }

    [Test]
    public async Task RegisterAsync_uses_the_async_path_when_sync_resolution_declines()
    {
        // A resolver that cannot answer synchronously (returns false) must be
        // driven through the async path; the seeded pin still reflects it.
        var resolver = Substitute.For<ITreePlacementResolver>();
        resolver.TryResolveForRegistration(Arg.Any<string>(), out Arg.Any<TreePhysicalPlacement>())
            .Returns(call => { call[1] = TreePhysicalPlacement.Default; return false; });
        resolver.ResolveForRegistrationAsync("t/acme/orders", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<TreePhysicalPlacement>(
                new TreePhysicalPlacement { WalProviderKey = "wal-acme" }));

        var (grain, tree, _) = CreateGrainWithResolver(resolver);
        tree.ExistsAsync("t/acme/orders").Returns(false);

        byte[]? written = null;
        await tree.SetAsync("t/acme/orders", Arg.Do<byte[]>(b => written = b));

        await grain.RegisterAsync("t/acme/orders");

        Assert.That(written, Is.Not.Null);
        Assert.That(ReadWalPlacement(written!)!.DefaultProviderKey, Is.EqualTo("wal-acme"));
    }

    [Test]
    public async Task RegisterAsync_preserves_a_caller_supplied_WalPlacement_pin()
    {
        // When the caller already supplies a placement pin, the resolver must not
        // override it - the resolver only seeds a tree that has none.
        var resolver = SyncResolver("t/acme/orders", new TreePhysicalPlacement { WalProviderKey = "wal-acme" });
        var (grain, tree, placementResolver) = CreateGrainWithResolver(resolver);
        tree.ExistsAsync("t/acme/orders").Returns(false);

        byte[]? written = null;
        await tree.SetAsync("t/acme/orders", Arg.Do<byte[]>(b => written = b));

        var caller = new TreeRegistryEntry
        {
            WalPlacement = WalPlacementPin.Create() with { DefaultProviderKey = "wal-caller" },
        };
        await grain.RegisterAsync("t/acme/orders", caller);

        Assert.That(written, Is.Not.Null);
        Assert.That(ReadWalPlacement(written!)!.DefaultProviderKey, Is.EqualTo("wal-caller"));
        placementResolver.DidNotReceive().TryResolveForRegistration(
            Arg.Any<string>(), out Arg.Any<TreePhysicalPlacement>());
    }

    [Test]
    public async Task RegisterAsync_does_not_re_place_an_already_registered_tree()
    {
        // Immutability (v1): a re-register of an existing tree returns past the
        // idempotency guard before the placement seam, so the resolver is never
        // consulted and nothing is written - a later placement change cannot
        // migrate a tree that already exists.
        var resolver = SyncResolver("t/acme/orders", new TreePhysicalPlacement { WalProviderKey = "wal-acme" });
        var (grain, tree, placementResolver) = CreateGrainWithResolver(resolver);
        tree.ExistsAsync("t/acme/orders").Returns(true);

        await grain.RegisterAsync("t/acme/orders");

        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        placementResolver.DidNotReceive().TryResolveForRegistration(
            Arg.Any<string>(), out Arg.Any<TreePhysicalPlacement>());
    }

    [Test]
    public async Task RegisterAsync_leaves_a_non_tenant_tree_unpinned_under_an_active_resolver()
    {
        // With an active resolver present, a non-tenant id the resolver maps to
        // the baseline must still get a null pin - routing for legacy/system
        // trees is unchanged when tenancy is enabled.
        var resolver = SyncResolver("t/acme/orders", new TreePhysicalPlacement { WalProviderKey = "wal-acme" });
        var (grain, tree, _) = CreateGrainWithResolver(resolver);
        tree.ExistsAsync("legacy-tree").Returns(false);

        byte[]? written = null;
        await tree.SetAsync("legacy-tree", Arg.Do<byte[]>(b => written = b));

        await grain.RegisterAsync("legacy-tree");

        Assert.That(written, Is.Not.Null);
        Assert.That(ReadWalPlacement(written!), Is.Null);
    }
}
