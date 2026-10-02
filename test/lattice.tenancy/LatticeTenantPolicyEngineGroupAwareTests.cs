using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the group-aware <see cref="ITenantPolicyEngine"/> overloads
/// (epic #4154, T1) as the engine answers them from the compiled snapshot, and for
/// the snapshot rebuild a change of
/// <see cref="LatticeTenancyOptions.DelegatedAccessAdministrationEnabled"/>
/// triggers. Every flag flip is driven directly on a
/// <see cref="DelegatedTenantAccessFlag"/>, and every rebuild is awaited through
/// the maintainer's own background task, so nothing is timing-dependent.
/// </summary>
[TestFixture]
public sealed class LatticeTenantPolicyEngineGroupAwareTests
{
    private const string ClusterGroup = "entra-sales";

    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public async Task ValidateActiveTenant_with_groups_follows_the_flag_once_the_rebuild_lands()
    {
        var flag = new DelegatedTenantAccessFlag(false);
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"], members: [ClusterGroup]));
        var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, flag);
        var engine = new LatticeTenantPolicyEngine(maintainer);
        Assert.That(engine.ValidateActiveTenant("carol", [ClusterGroup], Acme).Allowed, Is.False, "precondition: off, members are inert");

        flag.Set(true);
        Assert.That(maintainer.IsSnapshotAuthoritative, Is.False, "a flag change stops the old snapshot being authoritative at once");
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "the rebuild lands");
            Assert.That(maintainer.Current.IsDelegatedAccessEnabled, Is.True);
            Assert.That(engine.ValidateActiveTenant("carol", [ClusterGroup], Acme).Allowed, Is.True, "on, the group member acts");
        });

        flag.Set(false);
        await maintainer.BackgroundRebuild;

        Assert.That(engine.ValidateActiveTenant("carol", [ClusterGroup], Acme).Allowed, Is.False, "off again, members are inert again");
    }

    [Test]
    public async Task Flag_set_to_its_current_value_does_not_invalidate_the_snapshot()
    {
        var flag = new DelegatedTenantAccessFlag(true);
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, flag);
        var epoch = maintainer.CurrentEpoch;

        flag.Set(true);

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True);
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(epoch), "no rebuild for a non-change");
    }

    [Test]
    public async Task Maintainer_without_a_flag_compiles_disabled_and_reports_it()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"], members: ["carol"]));
        var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry);

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.IsDelegatedAccessEnabled, Is.False);
            Assert.That(maintainer.Current.IsDelegatedAccessEnabled, Is.False);
            Assert.That(new LatticeTenantPolicyEngine(maintainer).ValidateActiveTenant("carol", [], Acme).Allowed, Is.False);
        });
    }

    [Test]
    public async Task Disposed_maintainer_stops_observing_the_flag()
    {
        var flag = new DelegatedTenantAccessFlag(false);
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, flag);

        maintainer.Dispose();
        flag.Set(true);

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "a disposed maintainer no longer reacts to the flag");
    }

    [Test]
    public async Task ResolveAllowedTenants_with_groups_reads_the_current_snapshot()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"], members: [ClusterGroup]));
        var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, new DelegatedTenantAccessFlag(true));
        var engine = new LatticeTenantPolicyEngine(maintainer);

        Assert.Multiple(() =>
        {
            Assert.That(engine.ResolveAllowedTenants("carol", [ClusterGroup]), Is.EqualTo(new[] { Acme }));
            Assert.That(engine.ResolveAllowedTenants("carol"), Is.Empty, "the groupless overload considers the id alone");
            Assert.That(() => engine.ResolveAllowedTenants("carol", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Interface_default_overloads_fall_back_to_the_exact_id_members()
    {
        ITenantPolicyEngine engine = new ExactIdOnlyEngine();

        Assert.Multiple(() =>
        {
            Assert.That(engine.ValidateActiveTenant("alice", [ClusterGroup], Acme).Allowed, Is.True);
            Assert.That(engine.ValidateActiveTenant("carol", [ClusterGroup], Acme).Allowed, Is.False, "groups are ignored by the default");
            Assert.That(engine.ResolveAllowedTenants("alice", [ClusterGroup]), Is.EqualTo(new[] { Acme }));
            Assert.That(engine.ResolveAllowedTenants("carol", [ClusterGroup]), Is.Empty);
            Assert.That(() => engine.ValidateActiveTenant("alice", null!, Acme), Throws.ArgumentNullException);
            Assert.That(() => engine.ResolveAllowedTenants("alice", null!), Throws.ArgumentNullException);
        });
    }

    /// <summary>An engine written before groups existed: it implements only the exact-id members.</summary>
    private sealed class ExactIdOnlyEngine : ITenantPolicyEngine
    {
        public long CurrentEpoch => 1;

        public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId) =>
            subjectId == "alice" ? [Acme] : [];

        public TenantAccessDecision ValidateActiveTenant(string subjectId, TenantId activeTenant) =>
            subjectId == "alice" ? TenantAccessDecision.Allow() : TenantAccessDecision.Deny("no");

        public TenantAccessDecision ResolveCrossTenantGrant(
            TenantId sourceTenant,
            TenantId targetTenant,
            string scope,
            TenantGrantOperations operation) => TenantAccessDecision.Deny("no");
    }
}
