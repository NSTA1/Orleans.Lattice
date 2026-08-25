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
/// Pins the per-tenant write-admission layer that extends the user-origin
/// data-mutation seam (<c>ThrowIfUserOriginSystemDataTree</c>) with an
/// additional, opt-in check against the DI-registered
/// <see cref="ITenantAdmissionController"/>. When no controller is registered,
/// or the registered controller reports itself inactive, the write path must be
/// byte-for-byte identical to the pre-tenancy behaviour (admit all). An active
/// controller admits or refuses each user-origin write for the caller's active
/// tenant; a refusal throws <see cref="LatticeTenantAccessDeniedException"/>.
/// The layer sits only on the write surface and shares the structural guards'
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> bypass.
/// </summary>
[TestFixture]
public class LatticeGrainTenantAdmissionTests
{
    private const string TreeId = "orders";

    private static (LatticeGrain grain, IGrainFactory factory) CreateGrain(
        string treeId, ITenantAdmissionController? controller)
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

        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITenantAdmissionController)).Returns(controller);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, grainFactory);
    }

    /// <summary>
    /// A configurable in-memory admission controller. Records the arguments of
    /// the most recent decision so a test can assert the active tenant and tree
    /// id were threaded through, and observes the supplied cancellation token so
    /// the cancellation path can be pinned without any timing dependency.
    /// </summary>
    private sealed class FakeAdmissionController(bool active, bool admit) : ITenantAdmissionController
    {
        public bool IsActive => active;

        public int CallCount { get; private set; }

        public TenantId LastTenant { get; private set; }

        public string? LastTreeId { get; private set; }

        public ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default)
        {
            CallCount++;
            LastTenant = tenant;
            LastTreeId = treeId;
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<bool>(admit);
        }
    }

    // ----- No controller registered: admit-all, path unchanged -----

    [Test]
    public void SetAsync_with_no_controller_registered_proceeds()
        => Assert.DoesNotThrowAsync(() => CreateGrain(TreeId, controller: null).grain.SetAsync("k", [1]));

    [Test]
    public void DeleteAsync_with_no_controller_registered_proceeds()
        => Assert.DoesNotThrowAsync(() => CreateGrain(TreeId, controller: null).grain.DeleteAsync("k"));

    // ----- Inactive controller: admit-all, controller never consulted -----

    [Test]
    public async Task SetAsync_with_inactive_controller_proceeds_without_consulting_it()
    {
        var controller = new FakeAdmissionController(active: false, admit: false);
        var (grain, _) = CreateGrain(TreeId, controller);

        Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1]));
        await Task.CompletedTask;
        Assert.That(controller.CallCount, Is.Zero);
    }

    // ----- Active controller that admits: write proceeds, tenant threaded -----

    [Test]
    public async Task SetAsync_with_active_admitting_controller_proceeds()
    {
        var controller = new FakeAdmissionController(active: true, admit: true);
        var (grain, _) = CreateGrain(TreeId, controller);
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));

        await grain.SetAsync("k", [1]);

        Assert.That(controller.CallCount, Is.EqualTo(1));
        Assert.That(controller.LastTenant, Is.EqualTo(TenantId.Parse("contoso")));
        Assert.That(controller.LastTreeId, Is.EqualTo(TreeId));
    }

    [Test]
    public async Task DeleteAsync_with_active_admitting_controller_proceeds()
    {
        var controller = new FakeAdmissionController(active: true, admit: true);
        var (grain, _) = CreateGrain(TreeId, controller);

        await grain.DeleteAsync("k");

        Assert.That(controller.CallCount, Is.EqualTo(1));
    }

    [Test]
    public async Task SetAsync_with_no_active_tenant_admits_under_the_default_tenant()
    {
        var controller = new FakeAdmissionController(active: true, admit: true);
        var (grain, _) = CreateGrain(TreeId, controller);

        await grain.SetAsync("k", [1]);

        Assert.That(controller.LastTenant, Is.EqualTo(TenantId.Default));
    }

    // ----- Active controller that refuses: write throws -----

    [Test]
    public void SetAsync_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.SetAsync("k", [1]));

    [Test]
    public void SetAsync_ttl_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.SetAsync("k", [1], TimeSpan.FromMinutes(1)));

    [Test]
    public void SetIfVersionAsync_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.SetIfVersionAsync("k", [1], HybridLogicalClock.Zero));

    [Test]
    public void GetOrSetAsync_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.GetOrSetAsync("k", [1]));

    [Test]
    public void SetManyAsync_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.SetManyAsync([new("k", [1])]));

    [Test]
    public void SetManyAtomicAsync_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.SetManyAtomicAsync([new("k", [1])]));

    [Test]
    public void SetManyAtomicAsync_with_operation_id_and_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.SetManyAtomicAsync([new("k", [1])], "op-1"));

    [Test]
    public void DeleteAsync_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.DeleteAsync("k"));

    [Test]
    public void DeleteRangeAsync_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.DeleteRangeAsync("a", "z"));

    [Test]
    public void BulkLoadAsync_with_active_refusing_controller_throws()
        => Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.BulkLoadAsync([new("k", [1])]));

    [Test]
    public void SetAsync_refusal_names_the_tenant_and_tree()
    {
        using var _tenant = LatticeActiveTenantContext.With(TenantId.Parse("contoso"));
        var ex = Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => CreateGrain(TreeId, new FakeAdmissionController(active: true, admit: false)).grain.SetAsync("k", [1]));

        Assert.That(ex!.Message, Does.Contain("contoso"));
        Assert.That(ex.Message, Does.Contain(TreeId));
    }

    // ----- System-origin bypasses admission entirely -----

    [Test]
    public void SetAsync_under_system_origin_bypasses_a_refusing_controller()
    {
        var controller = new FakeAdmissionController(active: true, admit: false);
        var (grain, _) = CreateGrain(TreeId, controller);

        using var _origin = LatticeAccessGateContext.EnterSystemOrigin();

        Assert.DoesNotThrowAsync(() => grain.SetAsync("k", [1]));
        Assert.That(controller.CallCount, Is.Zero);
    }

    [Test]
    public void DeleteAsync_under_system_origin_bypasses_a_refusing_controller()
    {
        var controller = new FakeAdmissionController(active: true, admit: false);
        var (grain, _) = CreateGrain(TreeId, controller);

        using var _origin = LatticeAccessGateContext.EnterSystemOrigin();

        Assert.DoesNotThrowAsync(() => grain.DeleteAsync("k"));
        Assert.That(controller.CallCount, Is.Zero);
    }

    // ----- Reads are never gated by admission -----

    [Test]
    public void GetAsync_is_not_gated_by_a_refusing_controller()
    {
        var controller = new FakeAdmissionController(active: true, admit: false);
        var (grain, _) = CreateGrain(TreeId, controller);

        Assert.DoesNotThrowAsync(() => grain.GetAsync("k"));
        Assert.That(controller.CallCount, Is.Zero);
    }

    // ----- Cancellation flows into the admission decision -----

    [Test]
    public void SetAsync_with_active_controller_observes_a_cancelled_token()
    {
        var controller = new FakeAdmissionController(active: true, admit: true);
        var (grain, _) = CreateGrain(TreeId, controller);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => grain.SetAsync("k", [1], cts.Token));
    }

    [Test]
    public void SetManyAtomicAsync_with_active_controller_observes_a_cancelled_token()
    {
        var controller = new FakeAdmissionController(active: true, admit: true);
        var (grain, _) = CreateGrain(TreeId, controller);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => grain.SetManyAtomicAsync([new("k", [1])], cts.Token));
    }
}
