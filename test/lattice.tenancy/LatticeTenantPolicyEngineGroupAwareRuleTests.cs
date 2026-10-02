using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the group-aware active-tenant rule (epic #4154, T1):
/// <see cref="LatticeTenantPolicyEngine.ValidateActiveTenant(CompiledTenantPolicy, string, IReadOnlyCollection{string}, TenantId)"/>
/// over a policy compiled with and without delegated tenant access administration.
/// A subject may act as a tenant when its id or any of its resolved groups is an
/// admin or member entry - but only when the feature is enabled; disabled, the rule
/// is exactly the exact-id admin check it always was.
/// </summary>
[TestFixture]
public sealed class LatticeTenantPolicyEngineGroupAwareRuleTests
{
    private const string ClusterGroup = "entra-sales";
    private const string TenantGroup = "t/acme/editors";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private static CompiledTenantPolicy Enabled(params TenantRecord[] records) => CompiledTenantPolicy.Compile(records, true);

    private static CompiledTenantPolicy Disabled(params TenantRecord[] records) => CompiledTenantPolicy.Compile(records, false);

    private static TenantAccessDecision Validate(CompiledTenantPolicy policy, string subject, string[] groups, TenantId tenant) =>
        LatticeTenantPolicyEngine.ValidateActiveTenant(policy, subject, groups, tenant);

    [Test]
    public void ValidateActiveTenant_member_via_a_cluster_group_may_act_as_the_tenant()
    {
        var policy = Enabled(Record("acme", admins: ["alice"], members: [ClusterGroup]));

        var decision = Validate(policy, "carol", [ClusterGroup], Acme);

        Assert.That(decision.Allowed, Is.True, $"denied with: {decision.Reason}");
    }

    [Test]
    public void ValidateActiveTenant_member_via_a_tenant_group_may_act_as_the_tenant()
    {
        var policy = Enabled(Record("acme", admins: ["alice"], members: [TenantGroup]));

        var decision = Validate(policy, "carol", ["unrelated", TenantGroup], Acme);

        Assert.That(decision.Allowed, Is.True, $"denied with: {decision.Reason}");
    }

    [Test]
    public void ValidateActiveTenant_member_by_exact_id_may_act_as_the_tenant()
    {
        var policy = Enabled(Record("acme", admins: ["alice"], members: ["carol"]));

        Assert.That(Validate(policy, "carol", [], Acme).Allowed, Is.True);
    }

    [Test]
    public void ValidateActiveTenant_group_admin_is_recognised()
    {
        var policy = Enabled(Record("acme", admins: [ClusterGroup]));

        var decision = Validate(policy, "dave", [ClusterGroup], Acme);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True, $"denied with: {decision.Reason}");
            Assert.That(policy.TryGetTenant("acme", out var tenant), Is.True);
            Assert.That(tenant!.IsAdmin("dave", [ClusterGroup]), Is.True, "the group admin entry makes the subject an admin");
            Assert.That(tenant.IsAdmin("dave", []), Is.False, "without the group the subject is not an admin");
        });
    }

    [Test]
    public void ValidateActiveTenant_subject_neither_admin_nor_member_is_denied_with_a_group_aware_reason()
    {
        var policy = Enabled(Record("acme", admins: ["alice"], members: [ClusterGroup]));

        var decision = Validate(policy, "mallory", ["other-group"], Acme);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("Subject 'mallory' is not an admin or member of tenant 'acme'"));
    }

    [Test]
    public void ValidateActiveTenant_membership_of_another_tenant_never_validates_acting_as_this_one()
    {
        var policy = Enabled(
            Record("acme", admins: ["alice"]),
            Record("beta", admins: ["bob"], members: [ClusterGroup]));

        Assert.Multiple(() =>
        {
            Assert.That(Validate(policy, "carol", [ClusterGroup], Acme).Allowed, Is.False, "beta's member is not acme's");
            Assert.That(Validate(policy, "carol", [ClusterGroup], Beta).Allowed, Is.True);
        });
    }

    [Test]
    public void ValidateActiveTenant_member_of_a_suspended_tenant_is_denied()
    {
        var policy = Enabled(Record("acme", TenantStatus.Suspended, admins: ["alice"], members: [ClusterGroup]));

        var decision = Validate(policy, "carol", [ClusterGroup], Acme);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("is not active"));
    }

    [Test]
    public void ValidateActiveTenant_with_the_flag_off_ignores_member_entries_and_group_admins()
    {
        var policy = Disabled(Record("acme", admins: ["alice", ClusterGroup], members: ["carol", TenantGroup]));

        Assert.Multiple(() =>
        {
            Assert.That(Validate(policy, "alice", [], Acme).Allowed, Is.True, "the exact-id admin still acts");
            Assert.That(Validate(policy, "carol", [], Acme).Allowed, Is.False, "an exact-id member entry is inert");
            Assert.That(Validate(policy, "dave", [ClusterGroup], Acme).Allowed, Is.False, "a group admin entry is inert");
            Assert.That(Validate(policy, "erin", [TenantGroup], Acme).Allowed, Is.False, "a group member entry is inert");
        });
    }

    [Test]
    public void ValidateActiveTenant_with_the_flag_off_keeps_the_exact_admin_denial_reason()
    {
        var policy = Disabled(Record("acme", admins: ["alice"], members: ["carol"]));

        var decision = Validate(policy, "carol", [ClusterGroup], Acme);

        Assert.That(decision.Reason, Is.EqualTo("Subject 'carol' is not an admin of tenant 'acme'."));
    }

    [Test]
    public void ValidateActiveTenant_with_the_flag_off_is_identical_to_the_groupless_overload()
    {
        var records = new[]
        {
            Record("acme", admins: ["alice", ClusterGroup], members: ["carol"]),
            Record("beta", TenantStatus.Suspended, admins: ["bob"]),
        };
        var policy = Disabled(records);
        var cases = new (string Subject, string[] Groups, TenantId Tenant)[]
        {
            ("alice", [], Acme), ("alice", [ClusterGroup], Acme), ("carol", [], Acme), ("dave", [ClusterGroup], Acme),
            ("bob", [], Beta), ("alice", [], TenantId.Parse("gamma")), ("alice", [], default),
        };

        Assert.Multiple(() =>
        {
            foreach (var (subject, groups, tenant) in cases)
            {
                var withGroups = Validate(policy, subject, groups, tenant);
                var legacy = LatticeTenantPolicyEngine.ValidateActiveTenant(policy, subject, tenant);
                Assert.That(withGroups.Allowed, Is.EqualTo(legacy.Allowed), $"{subject} as {tenant.Value ?? "<none>"}: verdict");
                Assert.That(withGroups.Reason, Is.EqualTo(legacy.Reason), $"{subject} as {tenant.Value ?? "<none>"}: reason");
            }
        });
    }

    [Test]
    public void ValidateActiveTenant_default_tenant_never_honours_members_or_group_admins()
    {
        var record = TenantRecord.CreateDefault(TestClocks.Clock(1), "test");
        record.AddAdminSubject("alice", TestClocks.Clock(2), "test");
        record.AddAdminSubject(ClusterGroup, TestClocks.Clock(3), "test");
        var policy = Enabled(record);

        Assert.Multiple(() =>
        {
            Assert.That(Validate(policy, "alice", [], TenantId.Default).Allowed, Is.True, "the exact-id admin still acts");
            Assert.That(Validate(policy, "dave", [ClusterGroup], TenantId.Default).Allowed, Is.False,
                "the default tenant stays operator-administered: a group entry is never honoured");
            Assert.That(policy.TryGetTenant(TenantId.Default.Value!, out var compiled) && !compiled!.IsGroupAware, Is.True);
        });
    }

    [Test]
    public void ValidateActiveTenant_groups_collection_shapes_all_answer_alike()
    {
        var policy = Enabled(Record("acme", admins: ["alice"], members: [ClusterGroup]));
        IReadOnlyCollection<string>[] shapes =
        [
            new HashSet<string>(StringComparer.Ordinal) { "x", ClusterGroup },
            new List<string> { "x", ClusterGroup },
            new[] { "x", ClusterGroup },
            new System.Collections.ObjectModel.ReadOnlyCollection<string>(["x", ClusterGroup]),
            new SortedSet<string>(StringComparer.Ordinal) { "x", ClusterGroup },
        ];

        Assert.Multiple(() =>
        {
            foreach (var groups in shapes)
            {
                Assert.That(
                    LatticeTenantPolicyEngine.ValidateActiveTenant(policy, "carol", groups, Acme).Allowed,
                    Is.True,
                    groups.GetType().Name);
            }
        });
    }

    [Test]
    public void ValidateActiveTenant_null_groups_throws()
    {
        Assert.That(
            () => LatticeTenantPolicyEngine.ValidateActiveTenant(Enabled(Record("acme", admins: ["alice"])), "alice", null!, Acme),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ValidateActiveTenant_null_subject_with_groups_throws()
    {
        Assert.That(
            () => LatticeTenantPolicyEngine.ValidateActiveTenant(Enabled(Record("acme", admins: ["alice"])), null!, [], Acme),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ValidateActiveTenant_null_policy_with_groups_throws()
    {
        Assert.That(
            () => LatticeTenantPolicyEngine.ValidateActiveTenant(null!, "alice", [], Acme),
            Throws.ArgumentNullException);
    }
}
