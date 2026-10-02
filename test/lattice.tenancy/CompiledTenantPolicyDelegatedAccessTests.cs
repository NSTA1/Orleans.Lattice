using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledTenantPolicy"/> and <see cref="CompiledTenant"/>
/// compiled with delegated tenant access administration enabled (epic #4154, T1):
/// the member set, the group-aware admin and member probes, and the group-aware
/// <see cref="CompiledTenantPolicy.ResolveAllowedTenants(string, IReadOnlyCollection{string})"/>.
/// The disabled compile must build no member or group index at all.
/// </summary>
[TestFixture]
public sealed class CompiledTenantPolicyDelegatedAccessTests
{
    private const string ClusterGroup = "entra-sales";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");
    private static readonly TenantId Gamma = TenantId.Parse("gamma");

    [Test]
    public void Compile_disabled_builds_no_member_index()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: ["alice"], members: ["carol", ClusterGroup])], false);

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsDelegatedAccessEnabled, Is.False);
            Assert.That(policy.SubjectCount, Is.EqualTo(1), "only the admin entry is indexed");
            Assert.That(policy.ResolveAllowedTenants("carol"), Is.Empty);
            Assert.That(policy.TryGetTenant("acme", out var tenant), Is.True);
            Assert.That(tenant!.IsGroupAware, Is.False);
            Assert.That(tenant.Members, Is.Empty);
        });
    }

    [Test]
    public void Compile_single_argument_overload_is_the_disabled_compile()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: ["alice"], members: ["carol"])]);

        Assert.That(policy.IsDelegatedAccessEnabled, Is.False);
        Assert.That(policy.ResolveAllowedTenants("carol"), Is.Empty);
    }

    [Test]
    public void Compile_enabled_indexes_members_and_counts_an_admin_member_once()
    {
        var policy = CompiledTenantPolicy.Compile(
            [Record("acme", admins: ["alice"], members: ["alice", "carol"])],
            true);

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsDelegatedAccessEnabled, Is.True);
            Assert.That(policy.SubjectCount, Is.EqualTo(2));
            Assert.That(policy.ResolveAllowedTenants("alice"), Is.EqualTo(new[] { Acme }), "one slot, not two");
            Assert.That(policy.ResolveAllowedTenants("carol"), Is.EqualTo(new[] { Acme }));
        });
    }

    [Test]
    public void Compile_enabled_with_no_records_reports_enabled()
    {
        var policy = CompiledTenantPolicy.Compile([], true);

        Assert.That(policy.IsDelegatedAccessEnabled, Is.True);
        Assert.That(policy.TenantCount, Is.Zero);
        Assert.That(CompiledTenantPolicy.Empty.IsDelegatedAccessEnabled, Is.False);
    }

    [Test]
    public void Compile_enabled_never_gives_the_default_tenant_members()
    {
        var record = TenantRecord.CreateDefault(TestClocks.Clock(1), "test");
        record.MemberSlots["carol"] = new TenantSubjectSlot { Present = true, Clock = TestClocks.Clock(2), WriterId = "replica" };

        var policy = CompiledTenantPolicy.Compile([record], true);

        Assert.That(policy.ResolveAllowedTenants("carol"), Is.Empty, "a replicated member entry on the default tenant is ignored");
    }

    [Test]
    public void CompiledTenant_IsMember_admins_are_implicitly_members()
    {
        var tenant = Compiled(Record("acme", admins: ["alice", ClusterGroup], members: ["carol"]));

        Assert.Multiple(() =>
        {
            Assert.That(tenant.IsMember("alice", []), Is.True, "exact admin");
            Assert.That(tenant.IsMember("dave", [ClusterGroup]), Is.True, "group admin");
            Assert.That(tenant.IsMember("carol", []), Is.True, "exact member");
            Assert.That(tenant.IsMember("mallory", ["nope"]), Is.False);
            Assert.That(tenant.IsAdmin("carol", []), Is.False, "a member is not an admin");
        });
    }

    [Test]
    public void CompiledTenant_group_probes_throw_on_null_arguments()
    {
        var tenant = Compiled(Record("acme", admins: ["alice"]));

        Assert.Multiple(() =>
        {
            Assert.That(() => tenant.IsAdmin(null!, []), Throws.ArgumentNullException);
            Assert.That(() => tenant.IsAdmin("alice", null!), Throws.ArgumentNullException);
            Assert.That(() => tenant.IsMember(null!, []), Throws.ArgumentNullException);
            Assert.That(() => tenant.IsMember("alice", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void CompiledTenant_group_probes_ignore_null_group_entries()
    {
        var tenant = Compiled(Record("acme", admins: ["alice"], members: [ClusterGroup]));

        Assert.That(tenant.IsMember("carol", new string[] { null!, ClusterGroup }), Is.True);
        Assert.That(tenant.IsMember("carol", new string[] { null! }), Is.False);
    }

    [Test]
    public void ResolveAllowedTenants_with_groups_unions_id_and_group_entries_in_tenant_order()
    {
        var policy = CompiledTenantPolicy.Compile(
            [
                Record("gamma", admins: ["carol"]),
                Record("beta", admins: ["owner"], members: [ClusterGroup]),
                Record("acme", admins: [ClusterGroup], members: ["t/acme/editors"]),
            ],
            true);

        var tenants = policy.ResolveAllowedTenants("carol", [ClusterGroup, "t/acme/editors", "unknown"]);

        Assert.That(tenants, Is.EqualTo(new[] { Acme, Beta, Gamma }), "deduplicated, ascending");
    }

    [Test]
    public void ResolveAllowedTenants_with_one_contributing_entry_returns_the_cached_array()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: ["owner"], members: [ClusterGroup])], true);

        var first = policy.ResolveAllowedTenants("carol", [ClusterGroup, "unknown"]);
        var second = policy.ResolveAllowedTenants("dave", [ClusterGroup]);

        Assert.That(first, Is.EqualTo(new[] { Acme }));
        Assert.That(first, Is.SameAs(second), "a single contributing entry hands back the snapshot's own projection");
    }

    [Test]
    public void ResolveAllowedTenants_with_groups_when_disabled_is_the_exact_id_answer()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: [ClusterGroup], members: ["carol"])], false);

        Assert.Multiple(() =>
        {
            Assert.That(policy.ResolveAllowedTenants("carol", [ClusterGroup]), Is.Empty);
            Assert.That(policy.ResolveAllowedTenants(ClusterGroup, []), Is.EqualTo(new[] { Acme }), "an exact-id admin entry still resolves");
        });
    }

    [Test]
    public void ResolveAllowedTenants_with_groups_and_no_match_is_empty()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: ["owner"])], true);

        Assert.That(policy.ResolveAllowedTenants("carol", ["unknown"]), Is.Empty);
    }

    [Test]
    public void ResolveAllowedTenants_with_groups_null_arguments_throw()
    {
        var policy = CompiledTenantPolicy.Compile([Record("acme", admins: ["owner"])], true);

        Assert.Multiple(() =>
        {
            Assert.That(() => policy.ResolveAllowedTenants(null!, []), Throws.ArgumentNullException);
            Assert.That(() => policy.ResolveAllowedTenants("carol", null!), Throws.ArgumentNullException);
        });
    }

    private static CompiledTenant Compiled(TenantRecord record)
    {
        var policy = CompiledTenantPolicy.Compile([record], true);
        Assert.That(policy.TryGetTenant(record.Id.Value!, out var tenant), Is.True);
        return tenant!;
    }
}
