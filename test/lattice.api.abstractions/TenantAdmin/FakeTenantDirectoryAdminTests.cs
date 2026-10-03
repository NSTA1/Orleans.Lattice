using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Fakes;

namespace Orleans.Lattice.Api.Abstractions.Tests.TenantAdmin;

/// <summary>
/// Proves the shared <see cref="FakeTenantDirectoryAdmin"/> honours the
/// <see cref="ILatticeTenantDirectoryAdmin"/> contract the Explorer pages and
/// bindings are tested against: the guards, idempotency, ordering, paging, the
/// group-removal cascade, and subject resolution.
/// </summary>
[TestFixture]
public sealed class FakeTenantDirectoryAdminTests
{
    private const string Tenant = "acme";

    private static async Task<FakeTenantDirectoryAdmin> WithGroupsAsync(params string[] names)
    {
        var directory = new FakeTenantDirectoryAdmin();
        foreach (var name in names)
        {
            await directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = name });
        }

        return directory;
    }

    [Test]
    public void Every_operation_refuses_the_default_tenant()
    {
        var directory = new FakeTenantDirectoryAdmin();

        Assert.ThrowsAsync<ReservedTenantOperationException>(() => directory.ListGroupsAsync("default", new TenantAccessPageRequest()));
    }

    [Test]
    public void Every_operation_refuses_while_the_feature_is_disabled()
    {
        var directory = new FakeTenantDirectoryAdmin();
        directory.Gate.Enabled = false;

        var exception = Assert.ThrowsAsync<TenantAccessAdministrationDisabledException>(
            () => directory.AddMemberAsync(Tenant, "bob"));
        Assert.That(exception!.TenantId, Is.EqualTo(Tenant));
    }

    [Test]
    public void A_scripted_denial_refuses_the_call()
    {
        var directory = new FakeTenantDirectoryAdmin();
        directory.Gate.Denied = true;

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => directory.GetGroupAsync(Tenant, "readers"));
    }

    [Test]
    public void A_scripted_failure_is_raised_once()
    {
        var directory = new FakeTenantDirectoryAdmin();
        directory.Gate.NextFailure = new LatticeQuotaExceededException("cap");

        Assert.ThrowsAsync<LatticeQuotaExceededException>(() => directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "g" }));
        Assert.DoesNotThrowAsync(() => directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "g" }));
    }

    [Test]
    public void An_invalid_tenant_id_is_an_argument_error()
    {
        var directory = new FakeTenantDirectoryAdmin();

        Assert.ThrowsAsync<ArgumentException>(() => directory.GetGroupAsync("Not A Tenant!", "g"));
    }

    [Test]
    public async Task Groups_are_listed_in_ordinal_pages_and_confined_to_their_tenant()
    {
        var directory = await WithGroupsAsync("readers", "admins", "writers");
        await directory.UpsertGroupAsync("globex", new TenantGroupDescriptor { Name = "spies" });

        var first = await directory.ListGroupsAsync(Tenant, new TenantAccessPageRequest { PageSize = 2 });
        var second = await directory.ListGroupsAsync(Tenant, new TenantAccessPageRequest { PageSize = 2, PageToken = first.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries.Select(g => g.Name), Is.EqualTo(new[] { "admins", "readers" }));
            Assert.That(first.NextPageToken, Is.EqualTo("readers"));
            Assert.That(second.Entries.Select(g => g.Name), Is.EqualTo(new[] { "writers" }));
            Assert.That(second.NextPageToken, Is.Null);
        });
        Assert.That(await directory.GetGroupAsync(Tenant, "spies"), Is.Null, "another tenant's group reads as not found");
    }

    [Test]
    public async Task Upsert_replaces_the_display_name()
    {
        var directory = await WithGroupsAsync("readers");

        var stored = await directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "readers", DisplayName = "Readers" });

        Assert.That(await directory.GetGroupAsync(Tenant, "readers"), Is.EqualTo(stored));
    }

    [Test]
    public void Upsert_rejects_a_null_group() =>
        Assert.ThrowsAsync<ArgumentNullException>(() => new FakeTenantDirectoryAdmin().UpsertGroupAsync(Tenant, null!));

    [Test]
    public async Task Adding_and_removing_a_group_member_is_idempotent()
    {
        var directory = await WithGroupsAsync("readers");

        var added = await directory.AddGroupMemberAsync(Tenant, "readers", "bob");
        var again = await directory.AddGroupMemberAsync(Tenant, "readers", "bob");
        var members = await directory.ListGroupMembersAsync(Tenant, "readers");
        var removed = await directory.RemoveGroupMemberAsync(Tenant, "readers", "bob");
        var removedAgain = await directory.RemoveGroupMemberAsync(Tenant, "readers", "bob");

        Assert.Multiple(() =>
        {
            Assert.That(added.Changed, Is.True);
            Assert.That(added.GroupName, Is.EqualTo("readers"));
            Assert.That(again.Changed, Is.False);
            Assert.That(members, Is.EqualTo(new[] { new TenantGroupMember { MemberId = "bob" } }));
            Assert.That(removed.Changed, Is.True);
            Assert.That(removedAgain.Changed, Is.False);
        });
    }

    [Test]
    public async Task A_local_group_and_a_cluster_group_of_the_same_name_are_distinct_members()
    {
        var directory = await WithGroupsAsync("readers", "eng");

        await directory.AddGroupMemberAsync(Tenant, "readers", "eng", TenantSubjectKind.TenantGroup);
        var cluster = await directory.AddGroupMemberAsync(Tenant, "readers", "eng", TenantSubjectKind.ClusterGroup);

        Assert.Multiple(async () =>
        {
            Assert.That(cluster.Changed, Is.True);
            Assert.That(await directory.ListGroupMembersAsync(Tenant, "readers"), Has.Count.EqualTo(2));
        });
    }

    [Test]
    public async Task Adding_to_or_nesting_a_missing_group_is_an_argument_error()
    {
        var directory = await WithGroupsAsync("readers");

        Assert.ThrowsAsync<ArgumentException>(() => directory.AddGroupMemberAsync(Tenant, "missing", "bob"));
        Assert.ThrowsAsync<ArgumentException>(() => directory.AddGroupMemberAsync(Tenant, "readers", "missing", TenantSubjectKind.TenantGroup));
    }

    [Test]
    public async Task Naming_another_tenants_group_is_a_confinement_failure()
    {
        var directory = await WithGroupsAsync("readers");

        var edge = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => directory.AddGroupMemberAsync(Tenant, "readers", "t/globex/spies", TenantSubjectKind.ClusterGroup));
        var member = Assert.ThrowsAsync<TenantAccessConfinementException>(
            () => directory.AddMemberAsync(Tenant, "t/globex/spies", TenantSubjectKind.ClusterGroup));

        Assert.Multiple(() =>
        {
            Assert.That(edge!.Rule, Is.EqualTo(TenantAccessConfinementRule.ForeignTenantGroup));
            Assert.That(member!.Rule, Is.EqualTo(TenantAccessConfinementRule.ForeignTenantGroup));
        });
    }

    [Test]
    public async Task The_member_set_is_idempotent_ordered_and_paged()
    {
        var directory = new FakeTenantDirectoryAdmin();

        await directory.AddMemberAsync(Tenant, "carol");
        var first = await directory.AddMemberAsync(Tenant, "alice");
        var again = await directory.AddMemberAsync(Tenant, "alice");
        var page = await directory.ListMembersAsync(Tenant, new TenantAccessPageRequest { PageSize = 1 });
        var removed = await directory.RemoveMemberAsync(Tenant, "alice");

        Assert.Multiple(() =>
        {
            Assert.That(first.Changed, Is.True);
            Assert.That(first.GroupName, Is.Null);
            Assert.That(again.Changed, Is.False);
            Assert.That(page.Entries.Single().SubjectId, Is.EqualTo("alice"));
            Assert.That(page.NextPageToken, Is.EqualTo("alice"));
            Assert.That(removed.Changed, Is.True);
        });
    }

    [Test]
    public async Task Removing_a_group_cascades_to_edges_sets_and_rules()
    {
        var policy = new FakeTenantPolicyAdmin();
        var directory = new FakeTenantDirectoryAdmin(policy.Gate, policy);
        await directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "readers" });
        await directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "staff" });
        await directory.AddGroupMemberAsync(Tenant, "readers", "bob");
        await directory.AddGroupMemberAsync(Tenant, "staff", "readers", TenantSubjectKind.TenantGroup);
        await directory.AddMemberAsync(Tenant, "readers", TenantSubjectKind.TenantGroup);
        directory.SeedAdmin(Tenant, new TenantMemberEntry { SubjectId = "alice" });
        directory.SeedAdmin(Tenant, new TenantMemberEntry { SubjectId = "readers", Kind = TenantSubjectKind.TenantGroup });
        await policy.PutRuleAsync(Tenant, new TenantRuleDraft
        {
            RuleId = "r1", SubjectId = "readers", SubjectKind = TenantSubjectKind.TenantGroup,
            TreeName = "orders", Operations = LatticeOperation.Read,
        });

        var result = await directory.RemoveGroupAsync(Tenant, "readers");

        Assert.Multiple(async () =>
        {
            Assert.That(result.Removed, Is.True);
            Assert.That(result.EdgesRemoved, Is.EqualTo(2));
            Assert.That(result.RemovedFromMemberSet, Is.True);
            Assert.That(result.RemovedFromAdminSet, Is.True);
            Assert.That(result.RemovedRuleIds, Is.EqualTo(new[] { "r1" }));
            Assert.That(await directory.GetGroupAsync(Tenant, "readers"), Is.Null);
            Assert.That(await directory.ListGroupMembersAsync(Tenant, "staff"), Is.Empty);
            Assert.That(directory.AdminEntries(Tenant).Select(e => e.SubjectId), Is.EqualTo(new[] { "alice" }));
            Assert.That(await policy.GetRuleAsync(Tenant, "r1"), Is.Null);
        });
    }

    [Test]
    public async Task Removing_a_missing_group_is_a_no_op()
    {
        var result = await new FakeTenantDirectoryAdmin().RemoveGroupAsync(Tenant, "missing");

        Assert.Multiple(() =>
        {
            Assert.That(result.Removed, Is.False);
            Assert.That(result.EdgesRemoved, Is.Zero);
            Assert.That(result.RemovedRuleIds, Is.Empty);
        });
    }

    [Test]
    public async Task Removing_the_last_admin_entry_is_refused()
    {
        var directory = await WithGroupsAsync("admins");
        directory.SeedAdmin(Tenant, new TenantMemberEntry { SubjectId = "admins", Kind = TenantSubjectKind.TenantGroup });

        Assert.ThrowsAsync<TenantLastAdminSubjectException>(() => directory.RemoveGroupAsync(Tenant, "admins"));
        Assert.That(await directory.GetGroupAsync(Tenant, "admins"), Is.Not.Null);
    }

    [Test]
    public async Task Resolution_reports_admin_and_member_entries_through_nested_groups()
    {
        var directory = await WithGroupsAsync("admins", "leads", "staff");
        await directory.AddGroupMemberAsync(Tenant, "leads", "bob");
        await directory.AddGroupMemberAsync(Tenant, "admins", "leads", TenantSubjectKind.TenantGroup);
        directory.SeedAdmin(Tenant, new TenantMemberEntry { SubjectId = "admins", Kind = TenantSubjectKind.TenantGroup });
        await directory.AddMemberAsync(Tenant, "carol");

        var bob = await directory.ResolveSubjectAsync(Tenant, "bob");
        var carol = await directory.ResolveSubjectAsync(Tenant, "carol");
        var dave = await directory.ResolveSubjectAsync(Tenant, "dave");

        Assert.Multiple(() =>
        {
            Assert.That(bob.IsAdmin, Is.True);
            Assert.That(bob.IsMember, Is.True, "admins are implicitly members");
            Assert.That(bob.AdminEntries.Single().SubjectId, Is.EqualTo("admins"));
            Assert.That(bob.MemberEntries, Is.Empty);
            Assert.That(carol.IsAdmin, Is.False);
            Assert.That(carol.IsMember, Is.True);
            Assert.That(carol.MemberEntries.Single().SubjectId, Is.EqualTo("carol"));
            Assert.That(dave.IsMember, Is.False);
        });
    }

    [Test]
    public async Task Every_call_is_recorded_in_order()
    {
        var directory = new FakeTenantDirectoryAdmin();

        await directory.ListMembersAsync(Tenant, new TenantAccessPageRequest());
        await directory.ResolveSubjectAsync(Tenant, "bob");

        Assert.That(directory.Gate.Calls, Is.EqualTo(new[] { "ListMembersAsync", "ResolveSubjectAsync" }));
    }
}
