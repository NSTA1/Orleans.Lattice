using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Membership;
using static Orleans.Lattice.Tenancy.Tests.ObservabilityTestData;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for issue #4065: <see cref="TenantObservabilityView"/>'s active-tenant
/// re-validation previously answered from the compiled tenant-policy snapshot alone, so for
/// one rebuild window it could admit a subject the registry of record had already removed, or
/// deny one the registry had already added. These tests drive the real
/// <see cref="CompiledTenantPolicySnapshotMaintainer"/> (built through
/// <see cref="TenantPolicyEpochTestCluster"/>) and <see cref="LatticeTenantPolicyEngine"/> over a
/// <see cref="HoldableTenantRegistry"/> whose rebuild scan is held, so the non-authoritative
/// window is deterministic - no timing, no polling - while a point read answers with the
/// committed record.
/// </summary>
[TestFixture]
public sealed class TenantObservabilityViewRegistryLagTests
{
    private static readonly TenantId Beta = TenantId.Parse("beta");
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    [Test]
    public async Task GetActiveTenantAsync_admin_removed_mid_rebuild_must_not_be_admitted()
    {
        await using var world = await World.CreateAsync();

        world.Beta.RemoveAdminSubject("bob", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var snapshot = await world.GetActiveTenantAsync("bob");

        Assert.That(snapshot, Is.Null, "a subject removed from the tenant must not keep reading its own series");
    }

    [Test]
    public async Task GetActiveTenantAsync_tenant_suspended_mid_rebuild_must_not_be_admitted()
    {
        await using var world = await World.CreateAsync();

        world.Beta.SetStatus(TenantStatus.Suspended, Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var snapshot = await world.GetActiveTenantAsync("bob");

        Assert.That(snapshot, Is.Null, "a just-suspended tenant must not keep serving its own series");
    }

    [Test]
    public async Task GetActiveTenantAsync_tenant_deleted_mid_rebuild_must_not_be_admitted()
    {
        await using var world = await World.CreateAsync();

        world.Registry.Records.Remove(world.Beta);
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var snapshot = await world.GetActiveTenantAsync("bob");

        Assert.That(snapshot, Is.Null, "a just-deleted tenant must not keep serving its own series");
    }

    [Test]
    public async Task GetActiveTenantAsync_admin_added_mid_rebuild_is_admitted_read_your_writes()
    {
        await using var world = await World.CreateAsync();
        Assert.That(await world.GetActiveTenantAsync("carol"), Is.Null, "precondition: carol is not yet an admin");

        world.Beta.AddAdminSubject("carol", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        Assert.That(
            world.Engine.ValidateActiveTenant("carol", Beta).Allowed,
            Is.False,
            "precondition: the snapshot does not yet list carol");

        var snapshot = await world.GetActiveTenantAsync("carol");

        Assert.That(snapshot, Is.Not.Null, "an added admin is read-your-writes");
    }

    [Test]
    public async Task GetActiveTenantAsync_owned_tenant_in_the_window_reads_exactly_one_registry_record()
    {
        await using var world = await World.CreateAsync();
        world.Beta.AddAdminSubject("carol", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var reads = world.Registry.PointReads;

        var snapshot = await world.GetActiveTenantAsync("bob");

        Assert.That(snapshot, Is.Not.Null, "a still-listed admin is admitted");
        Assert.That(world.Registry.PointReads - reads, Is.EqualTo(1), "one record decides an owned-tenant read");
    }

    [Test]
    public async Task GetActiveTenantAsync_registry_failure_in_the_window_denies()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Beta, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("registry down"));

        var snapshot = await world.WithRegistry(registry).GetActiveTenantAsync("bob");

        Assert.That(snapshot, Is.Null, "an unconfirmable membership is denied, never admitted");
    }

    [Test]
    public async Task GetActiveTenantAsync_registry_record_for_another_tenant_is_not_taken_as_the_active_tenant()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Beta, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TenantRecord?>(Record("acme", admins: ["bob"])));

        var snapshot = await world.WithRegistry(registry).GetActiveTenantAsync("bob");

        Assert.That(snapshot, Is.Null, "a record keyed to another tenant never confirms the asserted one");
    }

    [Test]
    public async Task GetActiveTenantAsync_authoritative_steady_state_never_reads_the_registry()
    {
        await using var world = await World.CreateAsync();
        var reads = world.Registry.PointReads;

        var allowed = await world.GetActiveTenantAsync("bob");
        var denied = await world.GetActiveTenantAsync("mallory");

        Assert.That(allowed, Is.Not.Null, "the steady state still admits a valid admin");
        Assert.That(denied, Is.Null, "the steady state still denies a non-admin");
        Assert.That(world.Registry.PointReads, Is.EqualTo(reads), "the steady state never reads the registry");
    }

    /// <summary>
    /// Tenant <c>beta</c> (admin <c>bob</c>) with a seeded usage-index entry (so a non-null
    /// snapshot result unambiguously proves admission: <see cref="TenantObservabilitySource"/>
    /// also returns <c>null</c> for an admitted tenant with no usage-index entry), wired through
    /// a leased (authoritative) maintainer and the real engine.
    /// </summary>
    private sealed class World : IAsyncDisposable
    {
        private World(
            HoldableTenantRegistry registry,
            TenantRecord beta,
            CompiledTenantPolicySnapshotMaintainer maintainer,
            FakeTenantUsageIndex usageIndex)
        {
            Registry = registry;
            Beta = beta;
            Maintainer = maintainer;
            UsageIndex = usageIndex;
            Engine = new LatticeTenantPolicyEngine(maintainer);
            _registryOverride = registry;
        }

        public HoldableTenantRegistry Registry { get; }

        public TenantRecord Beta { get; }

        public CompiledTenantPolicySnapshotMaintainer Maintainer { get; }

        public FakeTenantUsageIndex UsageIndex { get; }

        public LatticeTenantPolicyEngine Engine { get; }

        private ITenantRegistry _registryOverride;

        public static async Task<World> CreateAsync()
        {
            var registry = new HoldableTenantRegistry();
            var beta = Record("beta", admins: ["bob"]);
            registry.Records.Add(beta);

            var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry);
            var usageIndex = new FakeTenantUsageIndex().With(beta.Id, View(Quotas(bytes: 1_000), Usage(bytes: 100)));
            return new World(registry, beta, maintainer, usageIndex);
        }

        /// <summary>Returns this world with the registry the next view is built over swapped.</summary>
        public World WithRegistry(ITenantRegistry registry)
        {
            _registryOverride = registry;
            return this;
        }

        private ITenantObservabilityView ViewOver(string subjectId) =>
            new TenantObservabilityView(
                new TenantObservabilitySource(UsageIndex, new FakeTenantOverageBilling()),
                AllowingGate(),
                Engine,
                Membership(subjectId),
                Maintainer,
                _registryOverride,
                NullLogger<TenantObservabilityView>.Instance);

        public Task<TenantObservabilitySnapshot?> GetActiveTenantAsync(string subjectId)
        {
            LatticeActiveTenantContext.Current = Beta.Id;
            return ViewOver(subjectId).GetActiveTenantAsync();
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
