using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the reservation of the tenant-wide authorization sentinel
/// (<c>t/{tenant}/*</c>): a tenant-wide rule is keyed by that id and stands for
/// every tree the tenant owns, so no real tree may ever carry it. Even the active
/// tenant's own composed id is refused on the user-origin data-mutation surface
/// when it ends in <c>/*</c>, while ordinary tenant trees, reads, and
/// system-origin writes are unaffected.
/// </summary>
[TestFixture]
public sealed class LatticeGrainTenantWideSentinelGuardTests
{
    private const string SentinelTreeId = "t/contoso/*";

    private static (LatticeGrain grain, IGrainFactory factory) CreateGrain(string treeId)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, grainFactory);
    }

    [Test]
    public void The_active_tenant_cannot_create_its_own_tenant_wide_sentinel_tree()
    {
        var (grain, _) = CreateGrain(SentinelTreeId);
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SetAsync("k", [1]));
            Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SetManyAsync([new("k", [1])]));
            Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.DeleteAsync("k"));
            Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.BulkLoadAsync([new("k", [1])]));
        });
    }

    [Test]
    public void The_name_star_composed_by_tenant_resolution_is_the_refused_sentinel()
    {
        // A tenant caller naming its tree "*" is composed into t/{tenant}/*, so the
        // tenant-wide sentinel can never be reached as a legal tree id.
        var composed = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), "*");
        Assert.That(composed, Is.EqualTo(SentinelTreeId));

        var (grain, _) = CreateGrain(composed);
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SetAsync("k", [1]));
    }

    [Test]
    public void Any_tenant_tree_id_ending_in_slash_star_is_refused()
    {
        var (grain, _) = CreateGrain("t/contoso/orders/*");
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SetAsync("k", [1]));
    }

    [Test]
    public void The_rejection_names_the_tenant_wide_sentinel()
    {
        var (grain, _) = CreateGrain(SentinelTreeId);
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        var ex = Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SetAsync("k", [1]));

        Assert.That(ex!.Message, Does.Contain("tenant-wide authorization sentinel"));
    }

    [TestCase("t/contoso/orders")]
    [TestCase("t/contoso/orders*")]
    [TestCase("t/contoso/*orders")]
    public void Ordinary_tenant_trees_remain_writable(string treeId)
    {
        var (grain, _) = CreateGrain(treeId);
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1]));
    }

    [Test]
    public void A_bare_id_ending_in_slash_star_is_unaffected()
    {
        // Only the tenant namespace is reserved; a default-tenant tree keeps
        // today's behaviour exactly.
        var (grain, _) = CreateGrain("orders/*");

        Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1]));
    }

    [Test]
    public void Reads_of_the_sentinel_are_not_gated()
    {
        var (grain, _) = CreateGrain(SentinelTreeId);
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.DoesNotThrowAsync(() => grain.GetAsync("k"));
    }

    [Test]
    public void System_origin_is_exempt()
    {
        var (grain, _) = CreateGrain(SentinelTreeId);
        using var _origin = LatticeAccessGateContext.EnterSystemOrigin();

        Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1]));
    }
}
