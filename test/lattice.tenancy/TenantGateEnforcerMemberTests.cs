using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Gate-level tests for tenant members and group entries (epic #4154, T1), through
/// the real <see cref="CompiledTenantPolicySnapshotMaintainer"/>,
/// <see cref="LatticeTenantPolicyEngine"/> and <see cref="TenantGateEnforcer"/>.
/// The registry-confirmation fallback (issue #4053) must apply the same
/// group-aware rule as the snapshot, so a removed member is refused at once even
/// while the rebuild that would drop it is held. The rebuild scan is held by a
/// <see cref="HoldableTenantRegistry"/>, so each window is deterministic.
/// </summary>
[TestFixture]
public sealed class TenantGateEnforcerMemberTests
{
    private const string OwnedTree = "t/beta/orders";
    private const string ClusterGroup = "entra-sales";
    private const string TenantGroup = "t/beta/editors";

    private static readonly TenantId Beta = TenantId.Parse("beta");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    [Test]
    public async Task EnforceAsync_member_via_a_cluster_group_is_admitted_on_the_tenants_tree()
    {
        await using var world = await World.CreateAsync(enabled: true, members: [ClusterGroup]);

        var decision = await world.EnforceAsync("carol", ClusterGroup);

        Assert.That(decision.Allowed, Is.True, $"denied with: {decision.Reason}");
    }

    [Test]
    public async Task EnforceAsync_member_via_a_tenant_group_is_admitted_on_the_tenants_tree()
    {
        await using var world = await World.CreateAsync(enabled: true, members: [TenantGroup]);

        var decision = await world.EnforceAsync("carol", TenantGroup);

        Assert.That(decision.Allowed, Is.True, $"denied with: {decision.Reason}");
    }

    [Test]
    public async Task EnforceAsync_group_admin_is_admitted_on_the_tenants_tree()
    {
        await using var world = await World.CreateAsync(enabled: true, admins: ["bob", ClusterGroup]);

        var decision = await world.EnforceAsync("dave", ClusterGroup);

        Assert.That(decision.Allowed, Is.True, $"denied with: {decision.Reason}");
    }

    [Test]
    public async Task EnforceAsync_member_with_the_flag_off_is_refused()
    {
        await using var world = await World.CreateAsync(enabled: false, members: ["carol", ClusterGroup]);

        var exactMember = await world.EnforceAsync("carol");
        var groupMember = await world.EnforceAsync("dave", ClusterGroup);
        var admin = await world.EnforceAsync("bob");

        Assert.Multiple(() =>
        {
            Assert.That(exactMember.Allowed, Is.False, "an exact-id member entry is inert");
            Assert.That(groupMember.Allowed, Is.False, "a group member entry is inert");
            Assert.That(admin.Allowed, Is.True, "the exact-id admin still acts");
        });
    }

    [Test]
    public async Task EnforceAsync_member_removed_mid_rebuild_is_refused_at_once()
    {
        await using var world = await World.CreateAsync(enabled: true, members: [ClusterGroup]);
        Assert.That((await world.EnforceAsync("carol", ClusterGroup)).Allowed, Is.True, "precondition: the group member acts");

        world.Beta.RemoveMemberSubject(ClusterGroup, Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        Assert.That(
            world.Engine.ValidateActiveTenant("carol", [ClusterGroup], Beta).Allowed,
            Is.True,
            "precondition: the held snapshot still lists the member, so trusting it would admit");

        var decision = await world.EnforceAsync("carol", ClusterGroup);

        Assert.That(decision.Allowed, Is.False, "the registry confirmation applies the same group-aware rule and sees the removal");
        Assert.That(decision.Reason, Does.Contain("not an admin or member of tenant 'beta'"));

        await world.ReleaseRebuildAsync();
        Assert.That((await world.EnforceAsync("carol", ClusterGroup)).Allowed, Is.False, "the removal holds once the rebuild lands");
    }

    [Test]
    public async Task EnforceAsync_member_added_mid_rebuild_is_admitted_read_your_writes()
    {
        await using var world = await World.CreateAsync(enabled: true);
        Assert.That((await world.EnforceAsync("carol", ClusterGroup)).Allowed, Is.False, "precondition: not yet a member");

        world.Beta.AddMemberSubject(ClusterGroup, Clock(9_000), "test");
        await world.PublishRegistryWriteWithRebuildHeldAsync();

        var decision = await world.EnforceAsync("carol", ClusterGroup);

        Assert.That(decision.Allowed, Is.True, $"an added member is read-your-writes; denied with: {decision.Reason}");
    }

    [Test]
    public async Task EnforceAsync_flag_turned_off_refuses_a_member_at_once_while_the_rebuild_is_held()
    {
        await using var world = await World.CreateAsync(enabled: true, members: [ClusterGroup]);
        Assert.That((await world.EnforceAsync("carol", ClusterGroup)).Allowed, Is.True, "precondition: the group member acts");

        world.Registry.HoldScans();
        world.Flag.Set(false);
        Assert.That(world.Maintainer.IsSnapshotAuthoritative, Is.False, "precondition: the flag change is a pending rebuild");

        var decision = await world.EnforceAsync("carol", ClusterGroup);

        Assert.That(decision.Allowed, Is.False, "the confirmation compiles under the new, disabled posture");
        Assert.That(decision.Reason, Does.Contain("not an admin of tenant 'beta'"));
    }

    [Test]
    public async Task Enforce_synchronous_group_member_is_admitted_from_the_authoritative_snapshot()
    {
        await using var world = await World.CreateAsync(enabled: true, members: [ClusterGroup]);
        LatticeActiveTenantContext.Current = Beta;
        var request = Request("carol", ClusterGroup);

        var decision = world.Enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True, $"denied with: {decision.Reason}");
    }

    private static LatticeAccessRequest Request(string subject, params string[] groups) =>
        new(OwnedTree, LatticeOperation.Read, new LatticeSubject(subject, groups), "k");

    /// <summary>Tenant <c>beta</c> with its own tree, over a leased maintainer whose flag the test controls.</summary>
    private sealed class World : IAsyncDisposable
    {
        private World(HoldableTenantRegistry registry, TenantRecord beta, CompiledTenantPolicySnapshotMaintainer maintainer, DelegatedTenantAccessFlag flag)
        {
            Registry = registry;
            Beta = beta;
            Maintainer = maintainer;
            Flag = flag;
            Engine = new LatticeTenantPolicyEngine(maintainer);
            Enforcer = new TenantGateEnforcer(
                Engine,
                new NullTenantResidencyResolver(),
                maintainer,
                registry,
                NullLogger<TenantGateEnforcer>.Instance);
        }

        public HoldableTenantRegistry Registry { get; }

        public TenantRecord Beta { get; }

        public CompiledTenantPolicySnapshotMaintainer Maintainer { get; }

        public DelegatedTenantAccessFlag Flag { get; }

        public LatticeTenantPolicyEngine Engine { get; }

        public TenantGateEnforcer Enforcer { get; }

        public static async Task<World> CreateAsync(bool enabled, string[]? admins = null, string[]? members = null)
        {
            var registry = new HoldableTenantRegistry();
            var beta = Record("beta", admins: admins ?? ["bob"], members: members);
            registry.Records.Add(beta);
            var flag = new DelegatedTenantAccessFlag(enabled);
            var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, flag);
            return new World(registry, beta, maintainer, flag);
        }

        public async Task PublishRegistryWriteWithRebuildHeldAsync()
        {
            Registry.HoldScans();
            await Maintainer.OnMutationAsync(
                new LatticeMutation { TreeId = TenantTreeNames.RegistryTree },
                CancellationToken.None);
            Assert.That(Maintainer.IsSnapshotAuthoritative, Is.False, "precondition: the rebuild is outstanding");
        }

        public async Task ReleaseRebuildAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
            Assert.That(Maintainer.IsSnapshotAuthoritative, Is.True, "the rebuild landed");
        }

        public async Task<LatticeAccessDecision> EnforceAsync(string subject, params string[] groups)
        {
            LatticeActiveTenantContext.Current = Beta.Id;
            var request = Request(subject, groups);
            return await Enforcer.EnforceAsync(in request);
        }

        public async ValueTask DisposeAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
        }
    }
}
