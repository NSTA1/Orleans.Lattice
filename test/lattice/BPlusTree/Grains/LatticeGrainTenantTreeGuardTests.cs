using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the user-origin data-mutation guard that reserves the structural tenant
/// namespace (<see cref="LatticeTenantTrees.SegmentPrefix"/>, <c>t/</c>) as a
/// third reserved namespace alongside <c>_lattice_</c> and <c>sys-</c>. A direct
/// user write to a <c>t/</c>-prefixed id must throw
/// <see cref="LatticeReservedTreeNamespaceException"/> on the public <see cref="ILattice"/>
/// surface, mirroring the <c>sys-</c> system-data guard. The guard sits only on
/// the write surface (reads are never gated) and is suppressed under a
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope so the tenancy
/// layer's internally-composed routing is unaffected.
/// </summary>
[TestFixture]
public class LatticeGrainTenantTreeGuardTests
{
    private const string TenantTreeId = "t/contoso/orders";

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

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, grainFactory);
    }

    private static ILattice CreateGrainFor(string treeId) => CreateGrain(treeId).grain;

    // ----- User-origin writes to a reserved t/ id are refused -----

    [Test]
    public void SetAsync_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).SetAsync("k", [1]));

    [Test]
    public void SetAsync_ttl_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).SetAsync("k", [1], TimeSpan.FromMinutes(1)));

    [Test]
    public void SetIfVersionAsync_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).SetIfVersionAsync("k", [1], HybridLogicalClock.Zero));

    [Test]
    public void GetOrSetAsync_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).GetOrSetAsync("k", [1]));

    [Test]
    public void SetManyAsync_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).SetManyAsync([new("k", [1])]));

    [Test]
    public void SetManyAtomicAsync_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).SetManyAtomicAsync([new("k", [1])]));

    [Test]
    public void SetManyAtomicAsync_with_operation_id_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).SetManyAtomicAsync([new("k", [1])], "op-1"));

    [Test]
    public void DeleteAsync_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).DeleteAsync("k"));

    [Test]
    public void DeleteRangeAsync_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).DeleteRangeAsync("a", "z"));

    [Test]
    public void BulkLoadAsync_rejects_a_tenant_id()
        => Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).BulkLoadAsync([new("k", [1])]));

    // ----- Message identifies the reserved tenant namespace -----

    [Test]
    public void SetAsync_rejection_names_the_tenant_namespace()
    {
        var ex = Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).SetAsync("k", [1]));

        Assert.That(ex!.Message, Does.Contain("structural tenant namespace"));
        Assert.That(ex.Message, Does.Contain(LatticeTenantTrees.SegmentPrefix));
    }

    // ----- Only a leading t/ is reserved -----

    [Test]
    public void SetAsync_allows_a_bare_id_that_merely_embeds_t_slash()
    {
        // Only a leading "t/" is the reserved prefix; an embedded one is a normal
        // bare (default-tenant) id and is writable.
        var (grain, factory) = CreateGrain("orders/t/eu");
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1]));
    }

    // ----- Reads are never gated by this guard -----

    [Test]
    public void GetAsync_allows_a_tenant_id()
    {
        // The reserved-tenant guard sits on the write surface only, mirroring the
        // sys- guard: a read of a t/ id is not refused by it.
        var (grain, factory) = CreateGrain(TenantTreeId);
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        Assert.DoesNotThrowAsync(() => grain.GetAsync("k"));
    }

    // ----- System-origin suppresses the guard (tenancy-layer composed routing) -----

    // ----- The active tenant's own composed id is admitted (user-origin) -----

    [Test]
    public void SetAsync_allows_the_active_tenants_own_composed_id()
    {
        // The external facades compose t/{activeTenant}/{name} and then write it
        // under the caller's own identity, so the write arrives user-origin by
        // design. Refusing it here made every tenant-scoped write through the
        // data plane fail once the facades began composing.
        var (grain, factory) = CreateGrain(TenantTreeId);
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        using var _ = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1]));
    }

    [Test]
    public void Every_write_verb_allows_the_active_tenants_own_composed_id()
    {
        var (grain, factory) = CreateGrain(TenantTreeId);
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        using var _ = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.Multiple(() =>
        {
            Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1], TimeSpan.FromMinutes(1)));
            Assert.DoesNotThrowAsync(() => grain.GetOrSetAsync("k", [1]));
            Assert.DoesNotThrowAsync(() => grain.SetManyAsync([new("k", [1])]));
            Assert.DoesNotThrowAsync(() => grain.DeleteAsync("k"));
            Assert.DoesNotThrowAsync(() => grain.DeleteRangeAsync("a", "z"));
            Assert.DoesNotThrowAsync(() => grain.BulkLoadAsync([new("k", [1])]));
        });
    }

    // ----- but only that tenant's own namespace -----

    [Test]
    public void SetAsync_rejects_another_tenants_id_while_a_tenant_is_active()
    {
        // Acting as one tenant must never let a caller name another tenant's
        // namespace: the ownership exit is an equality check, not a t/ bypass.
        var (grain, factory) = CreateGrain(TenantTreeId);
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        using var _ = LatticeActiveTenantContext.With(TenantId.Parse("globex"));

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SetAsync("k", [1]));
    }

    [Test]
    public void SetAsync_rejects_a_malformed_tenant_id_while_a_tenant_is_active()
    {
        // A malformed t/ id has no owning tenant (it classifies as platform-owned),
        // so it can never match the active tenant and stays uncreatable.
        var (grain, factory) = CreateGrain("t/contoso");
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        using var _ = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SetAsync("k", [1]));
    }

    // ----- Tenancy off is unchanged: no active tenant, namespace uncreatable -----

    [Test]
    public void SetAsync_still_rejects_a_tenant_id_when_no_tenant_is_active()
    {
        // With no tenancy add-on registered the ambient active tenant is never
        // set, so the ownership exit is unreachable and the t/ namespace remains
        // wholly uncreatable - the tenancy-off behaviour is byte-for-byte intact.
        Assert.That(LatticeActiveTenantContext.Current, Is.Null);

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => CreateGrainFor(TenantTreeId).SetAsync("k", [1]));
    }

    [Test]
    public void The_guard_is_restored_after_the_active_tenant_scope_ends()
    {
        var (grain, factory) = CreateGrain(TenantTreeId);
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        using (LatticeActiveTenantContext.With(TenantId.Parse("contoso")))
        {
            Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1]));
        }

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() => grain.SetAsync("k", [1]));
    }

    // ----- The sibling reserved namespaces are not widened by the exit -----

    [Test]
    public void An_active_tenant_does_not_unlock_the_system_namespaces()
    {
        // The ownership exit is scoped to the t/ guard alone; sys- and the
        // all-trees sentinel stay refused however the ambient tenant is set.
        using var _ = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
                () => CreateGrainFor("sys-auth-policy").SetAsync("k", [1]));
            Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
                () => CreateGrainFor("*").SetAsync("k", [1]));
        });
    }
}
