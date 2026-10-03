using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Membership;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for issue #4065: <see cref="TenantContextResolver"/>'s active-tenant
/// re-validation previously answered from the compiled tenant-policy snapshot alone, so for
/// one rebuild window it could resolve a tenant-local name into a subject the registry of
/// record had already removed, or refuse to resolve one the registry had already added. These
/// tests drive the real <see cref="CompiledTenantPolicySnapshotMaintainer"/> (built through
/// <see cref="TenantPolicyEpochTestCluster"/>) and <see cref="LatticeTenantPolicyEngine"/> over
/// a <see cref="HoldableTenantRegistry"/> whose rebuild scan is held, so the non-authoritative
/// window is deterministic - no timing, no polling - while a point read answers with the
/// committed record.
/// </summary>
[TestFixture]
public sealed class TenantContextResolverRegistryLagTests
{
    private static readonly TenantId Beta = TenantId.Parse("beta");
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    // ---- ResolveCurrentAsync: the registry-confirmation path -------------

    [Test]
    public async Task ResolveCurrentAsync_admin_removed_mid_rebuild_must_not_be_resolved()
    {
        await using var world = await World.CreateAsync();

        world.Beta.RemoveAdminSubject("bob", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var resolved = await world.ResolveCurrentAsync("bob");

        Assert.That(resolved, Is.EqualTo(default(TenantId)), "a subject removed from the tenant must not keep resolving into it");
    }

    [Test]
    public async Task ResolveCurrentAsync_tenant_suspended_mid_rebuild_must_not_be_resolved()
    {
        await using var world = await World.CreateAsync();

        world.Beta.SetStatus(TenantStatus.Suspended, Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var resolved = await world.ResolveCurrentAsync("bob");

        Assert.That(resolved, Is.EqualTo(default(TenantId)), "a just-suspended tenant must not keep resolving names into it");
    }

    [Test]
    public async Task ResolveCurrentAsync_tenant_deleted_mid_rebuild_must_not_be_resolved()
    {
        await using var world = await World.CreateAsync();

        world.Registry.Records.Remove(world.Beta);
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var resolved = await world.ResolveCurrentAsync("bob");

        Assert.That(resolved, Is.EqualTo(default(TenantId)), "a just-deleted tenant must not keep resolving names into it");
    }

    [Test]
    public async Task ResolveCurrentAsync_admin_added_mid_rebuild_is_resolved_read_your_writes()
    {
        await using var world = await World.CreateAsync();
        Assert.That(await world.ResolveCurrentAsync("carol"), Is.EqualTo(default(TenantId)), "precondition: carol is not yet an admin");

        world.Beta.AddAdminSubject("carol", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        Assert.That(
            world.Engine.ValidateActiveTenant("carol", Beta).Allowed,
            Is.False,
            "precondition: the snapshot does not yet list carol");

        var resolved = await world.ResolveCurrentAsync("carol");

        Assert.That(resolved, Is.EqualTo(Beta), "an added admin is read-your-writes");
    }

    [Test]
    public async Task ResolveCurrentAsync_owned_tenant_in_the_window_reads_exactly_one_registry_record()
    {
        await using var world = await World.CreateAsync();
        world.Beta.AddAdminSubject("carol", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var reads = world.Registry.PointReads;

        var resolved = await world.ResolveCurrentAsync("bob");

        Assert.That(resolved, Is.EqualTo(Beta), "a still-listed admin resolves");
        Assert.That(world.Registry.PointReads - reads, Is.EqualTo(1), "one record decides an owned-tenant resolution");
    }

    [Test]
    public async Task ResolveCurrentAsync_registry_failure_in_the_window_refuses_to_resolve()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Beta, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("registry down"));

        var resolved = await world.WithRegistry(registry).ResolveCurrentAsync("bob");

        Assert.That(resolved, Is.EqualTo(default(TenantId)), "an unconfirmable membership never resolves, never admitted");
    }

    [Test]
    public async Task ResolveCurrentAsync_registry_record_for_another_tenant_is_not_taken_as_the_active_tenant()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Beta, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TenantRecord?>(Record("acme", admins: ["bob"])));

        var resolved = await world.WithRegistry(registry).ResolveCurrentAsync("bob");

        Assert.That(resolved, Is.EqualTo(default(TenantId)), "a record keyed to another tenant never confirms the asserted one");
    }

    [Test]
    public async Task ResolveCurrentAsync_authoritative_steady_state_never_reads_the_registry()
    {
        await using var world = await World.CreateAsync();
        var reads = world.Registry.PointReads;

        var allowed = await world.ResolveCurrentAsync("bob");
        var denied = await world.ResolveCurrentAsync("mallory");

        Assert.That(allowed, Is.EqualTo(Beta), "the steady state still resolves a valid admin");
        Assert.That(denied, Is.EqualTo(default(TenantId)), "the steady state still refuses a non-admin");
        Assert.That(world.Registry.PointReads, Is.EqualTo(reads), "the steady state never reads the registry");
    }

    // ---- TryResolveCurrent: defers, never denies, and never reads the registry ----

    [Test]
    public async Task TryResolveCurrent_while_the_snapshot_is_not_authoritative_defers_without_reading_the_registry()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var reads = world.Registry.PointReads;
        LatticeActiveTenantContext.Current = Beta;

        var resolved = world.ResolverOver("bob").TryResolveCurrent(out var tenant);

        Assert.That(resolved, Is.False, "the synchronous form cannot confirm membership, so it must defer rather than deny");
        Assert.That(tenant, Is.EqualTo(default(TenantId)));
        Assert.That(world.Registry.PointReads, Is.EqualTo(reads), "the synchronous form never touches the registry");
    }

    [Test]
    public async Task TryResolveCurrent_authoritative_steady_state_resolves_synchronously()
    {
        await using var world = await World.CreateAsync();
        LatticeActiveTenantContext.Current = Beta;

        var resolved = world.ResolverOver("bob").TryResolveCurrent(out var tenant);

        Assert.That(resolved, Is.True, "the steady state resolves synchronously");
        Assert.That(tenant, Is.EqualTo(Beta));
    }

    /// <summary>
    /// Tenant <c>beta</c> (admin <c>bob</c>), wired through a leased (authoritative) maintainer
    /// and the real engine, so <see cref="TenantContextResolver"/> decides exactly as production
    /// does over a <see cref="HoldableTenantRegistry"/> whose rebuild scan can be held open.
    /// </summary>
    private sealed class World : IAsyncDisposable
    {
        private World(
            HoldableTenantRegistry registry,
            TenantRecord beta,
            CompiledTenantPolicySnapshotMaintainer maintainer)
        {
            Registry = registry;
            Beta = beta;
            Maintainer = maintainer;
            Engine = new LatticeTenantPolicyEngine(maintainer);
            _registryOverride = registry;
        }

        public HoldableTenantRegistry Registry { get; }

        public TenantRecord Beta { get; }

        public CompiledTenantPolicySnapshotMaintainer Maintainer { get; }

        public LatticeTenantPolicyEngine Engine { get; }

        private ITenantRegistry _registryOverride;

        public static async Task<World> CreateAsync()
        {
            var registry = new HoldableTenantRegistry();
            var beta = Record("beta", admins: ["bob"]);
            registry.Records.Add(beta);

            var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry);
            return new World(registry, beta, maintainer);
        }

        /// <summary>Returns this world with the registry the next resolver is built over swapped.</summary>
        public World WithRegistry(ITenantRegistry registry)
        {
            _registryOverride = registry;
            return this;
        }

        public TenantContextResolver ResolverOver(string subjectId) =>
            new(
                Engine,
                Membership(subjectId),
                Maintainer,
                _registryOverride,
                NullLogger<TenantContextResolver>.Instance);

        public async Task<TenantId> ResolveCurrentAsync(string subjectId)
        {
            LatticeActiveTenantContext.Current = Beta.Id;
            return await ResolverOver(subjectId).ResolveCurrentAsync();
        }

        /// <summary>
        /// Publishes the registry write to the maintainer exactly as the core write path does,
        /// with the rebuild's registry scan held, and asserts the resulting non-authoritative
        /// window.
        /// </summary>
        public async Task PublishRegistryWriteWithRebuildHeldAsync()
        {
            Registry.HoldScans();
            await Maintainer.OnMutationAsync(
                new LatticeMutation { TreeId = TenantTreeNames.RegistryTree },
                CancellationToken.None);
            Assert.That(Maintainer.IsSnapshotAuthoritative, Is.False, "precondition: the rebuild is outstanding");
        }

        /// <summary>Asserts the stale snapshot on its own would still validate <paramref name="subject"/> as <c>beta</c>.</summary>
        public void AssertSnapshotStillValidates(string subject) =>
            Assert.That(
                Engine.ValidateActiveTenant(subject, Beta.Id).Allowed,
                Is.True,
                "precondition: the snapshot still validates the subject, so trusting it would admit");

        public async ValueTask DisposeAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
        }
    }

    private static ILatticeMembershipContext Membership(string subjectId)
    {
        var membership = Substitute.For<ILatticeMembershipContext>();
        var subject = new LatticeSubject(subjectId);
        membership.TryResolveCurrent(out Arg.Any<LatticeSubject>()).Returns(call =>
        {
            call[0] = subject;
            return true;
        });
        membership.ResolveCurrentAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<LatticeSubject>(subject));
        return membership;
    }
}
