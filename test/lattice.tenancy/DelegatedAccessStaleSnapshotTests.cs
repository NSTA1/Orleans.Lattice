using NSubstitute;
using Orleans.Lattice.Membership;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for the T1 review finding that, after the delegated access
/// flag is turned off, the consumers that read the engine's current snapshot
/// directly kept honouring member and group entries until the rebuild landed
/// (epic #4154, D10). The snapshot compiled with the flag on stays current while
/// its rebuild is held, so these tests pin that the engine - the single home of the
/// rule - applies the exact-id admin rule as soon as the live flag is off, for
/// <see cref="ITenantPolicyEngine.ValidateActiveTenant(string, TenantId)"/>, its
/// group-aware overload, both <c>ResolveAllowedTenants</c> overloads (which
/// <c>LatticeTenantSelfService</c> lists from), <see cref="TenantContextResolver"/>
/// and <see cref="TenantObservabilityView"/>.
/// </summary>
[TestFixture]
public sealed class DelegatedAccessStaleSnapshotTests
{
    private const string ClusterGroup = "entra-sales";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    [Test]
    public async Task ValidateActiveTenant_after_a_flip_to_off_refuses_members_while_the_rebuild_is_held()
    {
        await using var world = await World.FlippedOffWithRebuildHeldAsync();

        Assert.Multiple(() =>
        {
            Assert.That(world.Engine.ValidateActiveTenant("carol", [ClusterGroup], Acme).Allowed, Is.False, "group member");
            Assert.That(world.Engine.ValidateActiveTenant("erin", Acme).Allowed, Is.False, "exact-id member");
            Assert.That(world.Engine.ValidateActiveTenant("dave", [ClusterGroup], Beta).Allowed, Is.False, "group admin");
            Assert.That(world.Engine.ValidateActiveTenant("alice", Acme).Allowed, Is.True, "the exact-id admin still acts");
            Assert.That(
                world.Engine.ValidateActiveTenant("erin", Acme).Reason,
                Is.EqualTo("Subject 'erin' is not an admin of tenant 'acme'."),
                "the flag-off rule and its reason");
        });
    }

    [Test]
    public async Task ResolveAllowedTenants_after_a_flip_to_off_lists_only_exact_id_admin_tenants()
    {
        await using var world = await World.FlippedOffWithRebuildHeldAsync();

        Assert.Multiple(() =>
        {
            Assert.That(world.Engine.ResolveAllowedTenants("erin"), Is.Empty, "an exact-id member is not listed");
            Assert.That(world.Engine.ResolveAllowedTenants("carol", [ClusterGroup]), Is.Empty, "a group member or admin is not listed");
            Assert.That(world.Engine.ResolveAllowedTenants("alice"), Is.EqualTo(new[] { Acme }), "admin of acme, member of beta: only acme");
            Assert.That(world.Engine.ResolveAllowedTenants("alice", [ClusterGroup]), Is.EqualTo(new[] { Acme }));
        });
    }

    [Test]
    public async Task TenantContextResolver_after_a_flip_to_off_refuses_a_group_member()
    {
        await using var world = await World.FlippedOffWithRebuildHeldAsync();
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(
            world.Engine,
            Membership(new LatticeSubject("carol", [ClusterGroup])),
            world.Maintainer,
            world.Registry,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<TenantContextResolver>.Instance);

        // The non-authoritative snapshot (the rebuild's registry scan is held) is
        // never trusted either way: the synchronous path defers rather than
        // answering from stale state (issue #4065), and the async path then
        // confirms the deny against the registry of record with the live flag.
        Assert.That(resolver.TryResolveCurrent(out _), Is.False, "defers: the snapshot cannot be trusted while non-authoritative");
        Assert.That((await resolver.ResolveCurrentAsync()).Value, Is.Null, "the registry confirms a group member is refused with the flag off");
    }

    [Test]
    public async Task TenantObservabilityView_after_a_flip_to_off_refuses_a_group_member()
    {
        await using var world = await World.FlippedOffWithRebuildHeldAsync();
        LatticeActiveTenantContext.Current = Acme;
        var view = new TenantObservabilityView(
            new TenantObservabilitySource(
                new ObservabilityTestData.FakeTenantUsageIndex().With(
                    Acme,
                    ObservabilityTestData.View(OverageTestData.Quotas(bytes: 1000), OverageTestData.Usage(bytes: 100))),
                new ObservabilityTestData.FakeTenantOverageBilling()),
            ObservabilityTestData.AllowingGate(),
            world.Engine,
            Membership(new LatticeSubject("carol", [ClusterGroup])),
            world.Maintainer,
            world.Registry,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<TenantObservabilityView>.Instance);

        // The registry-confirmation path (issue #4065) reaches the same deny as
        // before, now via the registry of record rather than a stale snapshot.
        Assert.That(await view.GetActiveTenantAsync(), Is.Null, "the registry confirms a group member is refused with the flag off");
    }

    [Test]
    public async Task After_the_rebuild_lands_the_answers_are_unchanged()
    {
        await using var world = await World.FlippedOffWithRebuildHeldAsync();

        await world.ReleaseRebuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(world.Maintainer.Current.IsDelegatedAccessEnabled, Is.False);
            Assert.That(world.Engine.ValidateActiveTenant("carol", [ClusterGroup], Acme).Allowed, Is.False);
            Assert.That(world.Engine.ResolveAllowedTenants("alice"), Is.EqualTo(new[] { Acme }));
        });
    }

    [Test]
    public void ResolveAdminTenants_filters_member_entries_from_a_group_aware_snapshot()
    {
        var policy = CompiledTenantPolicy.Compile(
            [Record("acme", admins: ["alice"]), Record("beta", admins: ["owner"], members: ["alice"]), Record("gamma", admins: ["alice"])],
            true);

        Assert.Multiple(() =>
        {
            Assert.That(policy.ResolveAllowedTenants("alice"), Has.Count.EqualTo(3), "precondition: membership is indexed");
            Assert.That(policy.ResolveAdminTenants("alice"), Is.EqualTo(new[] { Acme, TenantId.Parse("gamma") }));
            Assert.That(policy.ResolveAdminTenants("owner"), Is.SameAs(policy.ResolveAllowedTenants("owner")), "nothing filtered: the cached array");
            Assert.That(policy.ResolveAdminTenants("nobody"), Is.Empty);
            Assert.That(() => policy.ResolveAdminTenants(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void ResolveAdminTenants_on_a_flag_off_snapshot_is_the_cached_admin_answer()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: ["alice"], members: ["erin"])], false);

        Assert.Multiple(() =>
        {
            Assert.That(policy.ResolveAdminTenants("alice"), Is.SameAs(policy.ResolveAllowedTenants("alice")));
            Assert.That(policy.ResolveAdminTenants("erin"), Is.Empty);
        });
    }

    [Test]
    public void ResolveAdminTenants_where_every_entry_is_a_membership_is_empty()
    {
        var policy = CompiledTenantPolicy.Compile([Record("beta", admins: ["owner"], members: ["erin"])], true);

        Assert.That(policy.ResolveAdminTenants("erin"), Is.Empty);
    }

    private static ILatticeMembershipContext Membership(LatticeSubject subject)
    {
        var membership = Substitute.For<ILatticeMembershipContext>();
        membership.TryResolveCurrent(out Arg.Any<LatticeSubject>())
            .Returns(call =>
            {
                call[0] = subject;
                return true;
            });
        membership.ResolveCurrentAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<LatticeSubject>(subject));
        return membership;
    }

    /// <summary>
    /// Tenant <c>acme</c> (admin <c>alice</c>; members <c>erin</c> and the cluster
    /// group) and tenant <c>beta</c> (admin the cluster group; member <c>alice</c>),
    /// compiled with the flag on, after which the flag is turned off with the
    /// rebuild's registry scan held, so the flag-on snapshot is still current.
    /// </summary>
    private sealed class World : IAsyncDisposable
    {
        private World(HoldableTenantRegistry registry, CompiledTenantPolicySnapshotMaintainer maintainer)
        {
            Registry = registry;
            Maintainer = maintainer;
            Engine = new LatticeTenantPolicyEngine(maintainer);
        }

        public HoldableTenantRegistry Registry { get; }

        public CompiledTenantPolicySnapshotMaintainer Maintainer { get; }

        public LatticeTenantPolicyEngine Engine { get; }

        public static async Task<World> FlippedOffWithRebuildHeldAsync()
        {
            var registry = new HoldableTenantRegistry();
            registry.Records.Add(Record("acme", admins: ["alice"], members: ["erin", ClusterGroup]));
            registry.Records.Add(Record("beta", admins: [ClusterGroup], members: ["alice"]));
            var flag = new DelegatedTenantAccessFlag(true);
            var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, flag);
            var world = new World(registry, maintainer);
            Assert.That(world.Engine.ValidateActiveTenant("carol", [ClusterGroup], Acme).Allowed, Is.True, "precondition: on, a group member acts");

            registry.HoldScans();
            flag.Set(false);
            Assert.Multiple(() =>
            {
                Assert.That(maintainer.Current.IsDelegatedAccessEnabled, Is.True, "precondition: the flag-on snapshot is still current");
                Assert.That(maintainer.IsDelegatedAccessEnabled, Is.False);
            });
            return world;
        }

        public async Task ReleaseRebuildAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
        }

        public async ValueTask DisposeAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
        }
    }
}
