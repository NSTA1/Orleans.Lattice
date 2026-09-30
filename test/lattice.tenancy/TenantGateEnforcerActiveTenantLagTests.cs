using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Auth;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for issue #4053: active-tenant validation (membership and
/// tenant status) decided while the compiled tenant-policy snapshot is not
/// authoritative. A tenant-registry write only schedules a rebuild, so for one
/// rebuild the snapshot still lists a removed admin, still reports a suspended
/// tenant active, and still holds a deleted tenant. These tests drive the real
/// <see cref="CompiledTenantPolicySnapshotMaintainer"/> (built through
/// <see cref="TenantPolicyEpochTestCluster"/>), <see cref="LatticeTenantPolicyEngine"/>
/// and <see cref="TenantGateEnforcer"/> over a <see cref="HoldableTenantRegistry"/>
/// whose rebuild scan is held, so the window is deterministic - no timing, no
/// polling - while a point read answers with the committed record.
/// </summary>
[TestFixture]
public sealed class TenantGateEnforcerActiveTenantLagTests
{
    private const string OwnedTree = "t/beta/orders";
    private const string SharedTree = "t/acme/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    // ---- the owned-tree window the issue names --------------------------

    [Test]
    public async Task ValidateActiveTenant_admin_removed_mid_rebuild_must_not_be_admitted()
    {
        await using var world = await World.CreateAsync();

        world.Beta.RemoveAdminSubject("bob", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var decision = await world.EnforceAsync(OwnedTree, "bob");

        Assert.That(decision.Allowed, Is.False, "a subject removed from the tenant must not keep acting as it on its own trees");
        Assert.That(decision.Reason, Does.Contain("not an admin of tenant 'beta'"));

        await world.ReleaseRebuildAsync();
        Assert.That((await world.EnforceAsync(OwnedTree, "bob")).Allowed, Is.False, "the removal holds once the rebuild lands");
    }

    [Test]
    public async Task EnforceAsync_tenant_suspended_mid_rebuild_is_refused_on_its_own_tree()
    {
        await using var world = await World.CreateAsync();

        world.Beta.SetStatus(TenantStatus.Suspended, Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var decision = await world.EnforceAsync(OwnedTree, "bob");

        Assert.That(decision.Allowed, Is.False, "a just-suspended tenant must not keep serving its own trees");
        Assert.That(decision.Reason, Does.Contain("is not active"));
    }

    [Test]
    public async Task EnforceAsync_tenant_deleted_mid_rebuild_is_refused_on_its_own_tree()
    {
        await using var world = await World.CreateAsync();

        world.Registry.Records.Remove(world.Beta);
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var decision = await world.EnforceAsync(OwnedTree, "bob");

        Assert.That(decision.Allowed, Is.False, "a just-deleted tenant must not keep serving its own trees");
        Assert.That(decision.Reason, Does.Contain("Tenant 'beta' is not registered"));
    }

    [Test]
    public async Task EnforceAsync_admin_added_mid_rebuild_is_admitted_read_your_writes()
    {
        await using var world = await World.CreateAsync();
        Assert.That((await world.EnforceAsync(OwnedTree, "carol")).Allowed, Is.False, "precondition: carol is not yet an admin");

        world.Beta.AddAdminSubject("carol", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        Assert.That(
            world.Engine.ValidateActiveTenant("carol", Beta).Allowed,
            Is.False,
            "precondition: the snapshot does not yet list carol");

        var decision = await world.EnforceAsync(OwnedTree, "carol");

        Assert.That(decision.Allowed, Is.True, $"an added admin is read-your-writes; denied with: {decision.Reason}");
    }

    [Test]
    public async Task EnforceAsync_owned_tree_in_the_window_reads_exactly_one_registry_record()
    {
        await using var world = await World.CreateAsync();
        world.Beta.AddAdminSubject("carol", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var reads = world.Registry.PointReads;

        var decision = await world.EnforceAsync(OwnedTree, "bob");

        Assert.That(decision.Allowed, Is.True, $"a still-listed admin is admitted; denied with: {decision.Reason}");
        Assert.That(world.Registry.PointReads - reads, Is.EqualTo(1), "one record decides an owned-tree request");
    }

    [Test]
    public async Task EnforceAsync_owned_tree_confirmed_in_the_window_is_gated_on_residency()
    {
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Beta).Returns(false);
        await using var world = await World.CreateAsync(residency);
        await world.PublishRegistryWriteWithRebuildHeldAsync();

        var decision = await world.EnforceAsync(OwnedTree, "bob");

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("not online"));
    }

    // ---- fail closed ----------------------------------------------------

    [Test]
    public async Task EnforceAsync_registry_failure_on_an_owned_tree_in_the_window_denies()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Beta, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("registry down"));
        var enforcer = world.EnforcerOver(registry);
        LatticeActiveTenantContext.Current = Beta;
        var request = Request(OwnedTree, "bob");

        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.False, "an unconfirmable membership is denied, never admitted");
        Assert.That(decision.Reason, Does.Contain("acting as tenant 'beta' could not be confirmed"));
    }

    [Test]
    public async Task EnforceAsync_registry_record_for_another_tenant_is_not_taken_as_the_active_tenant()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Beta, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TenantRecord?>(Record("acme", admins: ["bob"])));
        var enforcer = world.EnforcerOver(registry);
        LatticeActiveTenantContext.Current = Beta;
        var request = Request(OwnedTree, "bob");

        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.False, "a record keyed to another tenant never confirms the asserted one");
        Assert.That(decision.Reason, Does.Contain("Tenant 'beta' is not registered"));
    }

    [Test]
    public async Task Enforce_synchronous_owned_tree_while_the_snapshot_is_not_authoritative_denies()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        LatticeActiveTenantContext.Current = Beta;
        var request = Request(OwnedTree, "bob");

        var decision = world.Enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.False, "the synchronous form cannot confirm membership, so it denies");
        Assert.That(decision.Reason, Does.Contain("acting as tenant 'beta' could not be confirmed"));
    }

    // ---- the crossing branch's own active-tenant validation -------------

    [Test]
    public async Task EnforceAsync_crossing_by_an_admin_removed_mid_rebuild_is_refused_despite_an_active_grant()
    {
        await using var world = await World.CreateAsync();
        world.AssertAdmitted(await world.EnforceAsync(SharedTree, "bob"), "precondition: the active grant admits bob's crossing");

        world.Beta.RemoveAdminSubject("bob", Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillValidates("bob");

        var decision = await world.EnforceAsync(SharedTree, "bob");

        Assert.That(decision.Allowed, Is.False, "a confirmed grant never stands in for the subject's right to act as the grantee");
        Assert.That(decision.Reason, Does.Contain("not an admin of tenant 'beta'"));
    }

    [Test]
    public async Task EnforceAsync_crossing_as_a_tenant_suspended_mid_rebuild_is_refused_despite_an_active_grant()
    {
        await using var world = await World.CreateAsync();

        world.Beta.SetStatus(TenantStatus.Suspended, Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();

        var decision = await world.EnforceAsync(SharedTree, "bob");

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("is not active"));
    }

    [Test]
    public async Task EnforceAsync_crossing_with_a_valid_member_still_requires_the_grant()
    {
        await using var world = await World.CreateAsync();

        world.RevokeGrant();
        await world.PublishRegistryWriteWithRebuildHeldAsync();

        var decision = await world.EnforceAsync(SharedTree, "bob");

        Assert.That(decision.Allowed, Is.False, "a confirmed membership never stands in for the grant");
        Assert.That(decision.Reason, Does.Contain("no active grant"));
    }

    [Test]
    public async Task EnforceAsync_crossing_in_the_window_reads_the_active_and_owner_records_once_each()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var reads = world.Registry.PointReads;

        var decision = await world.EnforceAsync(SharedTree, "bob");

        world.AssertAdmitted(decision, "a valid member with an active grant crosses");
        Assert.That(world.Registry.PointReads - reads, Is.EqualTo(2), "one read per record the crossing depends on");
    }

    [Test]
    public async Task EnforceAsync_crossing_registry_failure_on_the_active_tenant_record_denies()
    {
        await using var world = await World.CreateAsync();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Beta, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("registry down"));
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).Returns(_ => world.Registry.GetAsync(Acme));
        var enforcer = world.EnforcerOver(registry);
        LatticeActiveTenantContext.Current = Beta;
        var request = Request(SharedTree, "bob");

        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.False, "a readable grant never admits when the grantee's membership is unreadable");
        Assert.That(decision.Reason, Does.Contain("could not be confirmed"));
    }

    // ---- the steady state is untouched ----------------------------------

    [Test]
    public async Task EnforceAsync_authoritative_owned_tree_completes_synchronously_without_reading_the_registry()
    {
        await using var world = await World.CreateAsync();
        var reads = world.Registry.PointReads;
        LatticeActiveTenantContext.Current = Beta;
        var request = Request(OwnedTree, "bob");

        var pending = world.Enforcer.EnforceAsync(in request);

        Assert.That(pending.IsCompletedSuccessfully, Is.True, "the steady state never pays an async continuation");
        Assert.That(pending.Result.Allowed, Is.True);
        Assert.That(world.Registry.PointReads, Is.EqualTo(reads), "the steady state never reads the registry");
    }

    [Test]
    public async Task EnforceAsync_authoritative_denial_is_decided_from_the_snapshot_without_reading_the_registry()
    {
        await using var world = await World.CreateAsync();
        var reads = world.Registry.PointReads;
        LatticeActiveTenantContext.Current = Beta;
        var request = Request(OwnedTree, "mallory");

        var pending = world.Enforcer.EnforceAsync(in request);

        Assert.That(pending.IsCompletedSuccessfully, Is.True);
        Assert.That(pending.Result.Allowed, Is.False);
        Assert.That(pending.Result.Reason, Does.Contain("not an admin"));
        Assert.That(world.Registry.PointReads, Is.EqualTo(reads));
    }

    // ---- helpers --------------------------------------------------------

    private static LatticeAccessRequest Request(string treeId, string subject) =>
        new(treeId, LatticeOperation.Read, new LatticeSubject(subject), "k");

    /// <summary>
    /// Tenant <c>beta</c> (admin <c>bob</c>) with its own tree, and tenant
    /// <c>acme</c> granting <c>beta</c> read on <c>t/acme/orders</c>, wired through
    /// a leased (authoritative) maintainer, the real engine, and the real enforcer.
    /// </summary>
    private sealed class World : IAsyncDisposable
    {
        private readonly ITenantResidencyResolver _residency;

        private World(
            HoldableTenantRegistry registry,
            TenantRecord owner,
            TenantRecord beta,
            CompiledTenantPolicySnapshotMaintainer maintainer,
            ITenantResidencyResolver residency)
        {
            Registry = registry;
            Owner = owner;
            Beta = beta;
            Maintainer = maintainer;
            _residency = residency;
            Engine = new LatticeTenantPolicyEngine(maintainer);
            Enforcer = EnforcerOver(registry);
        }

        public HoldableTenantRegistry Registry { get; }

        public TenantRecord Owner { get; }

        public TenantRecord Beta { get; }

        public CompiledTenantPolicySnapshotMaintainer Maintainer { get; }

        public LatticeTenantPolicyEngine Engine { get; }

        public TenantGateEnforcer Enforcer { get; }

        public static async Task<World> CreateAsync(ITenantResidencyResolver? residency = null)
        {
            var registry = new HoldableTenantRegistry();
            var owner = Record(
                "acme",
                grants: [CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, SharedTree, TenantGrantOperations.Read, TenantGrantState.Active)]);
            var beta = Record("beta", admins: ["bob"]);
            registry.Records.Add(owner);
            registry.Records.Add(beta);

            var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry);
            return new World(registry, owner, beta, maintainer, residency ?? new NullTenantResidencyResolver());
        }

        /// <summary>An enforcer over this world's maintainer and engine that confirms against <paramref name="registry"/>.</summary>
        public TenantGateEnforcer EnforcerOver(ITenantRegistry registry) =>
            new(Engine, _residency, Maintainer, registry, NullLogger<TenantGateEnforcer>.Instance);

        public void RevokeGrant() =>
            Owner.TransitionGrant(Owner.Grants.Single().GrantId, TenantGrantState.Revoked, Clock(9_000), "test");

        /// <summary>
        /// Publishes the registry write to the maintainer exactly as the core write
        /// path does, with the rebuild's registry scan held, and asserts the
        /// resulting non-authoritative window.
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
                Engine.ValidateActiveTenant(subject, TenantId.Parse("beta")).Allowed,
                Is.True,
                "precondition: the snapshot still validates the subject, so trusting it would admit");

        public void AssertAdmitted(LatticeAccessDecision decision, string because) =>
            Assert.That(decision.Allowed, Is.True, $"{because}; denied with: {decision.Reason}");

        public async Task<LatticeAccessDecision> EnforceAsync(string treeId, string subject)
        {
            LatticeActiveTenantContext.Current = TenantId.Parse("beta");
            var request = Request(treeId, subject);
            return await Enforcer.EnforceAsync(in request);
        }

        public async Task ReleaseRebuildAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
            Assert.That(Maintainer.IsSnapshotAuthoritative, Is.True, "the rebuild landed");
        }

        public async ValueTask DisposeAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
        }
    }
}
