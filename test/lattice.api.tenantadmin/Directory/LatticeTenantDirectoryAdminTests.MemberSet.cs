using Orleans.Lattice;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>The tenant member set (add, remove, list, cap, confinement) and subject resolution.</summary>
public sealed partial class LatticeTenantDirectoryAdminTests
{
    [Test]
    public async Task AddMemberAsync_adds_a_user_to_the_member_set_once()
    {
        var harness = Build(Alice);

        var first = await harness.Admin.AddMemberAsync(Tenant, "bob");
        var second = await harness.Admin.AddMemberAsync(Tenant, "bob");

        Assert.Multiple(() =>
        {
            Assert.That(first.Changed, Is.True);
            Assert.That(first.GroupName, Is.Null);
            Assert.That(second.Changed, Is.False);
            Assert.That(harness.Committed(Tenant).MemberSubjects, Is.EqualTo(new[] { "bob" }));
            Assert.That(harness.Registry.Puts, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task AddMemberAsync_stores_a_tenant_group_under_its_composed_id()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");

        await harness.Admin.AddMemberAsync(Tenant, "eng", TenantSubjectKind.TenantGroup);

        Assert.That(harness.Committed(Tenant).MemberSubjects, Is.EqualTo(new[] { "t/acme/eng" }));
    }

    [Test]
    public void AddMemberAsync_refuses_a_tenant_group_that_does_not_exist()
    {
        var harness = Build(Alice);

        Assert.That(
            async () => await harness.Admin.AddMemberAsync(Tenant, "ghost", TenantSubjectKind.TenantGroup),
            Throws.ArgumentException);
        Assert.That(harness.Registry.Puts, Is.Zero);
    }

    [Test]
    public void AddMemberAsync_refuses_another_tenants_group_named_as_a_cluster_group()
    {
        var harness = Build(Alice);

        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(
            async () => await harness.Admin.AddMemberAsync(Tenant, "t/globex/eng", TenantSubjectKind.ClusterGroup));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.ForeignTenantGroup));
            Assert.That(ex.ParamName, Is.EqualTo("subjectId"));
            Assert.That(harness.Registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task AddMemberAsync_refuses_an_entry_beyond_the_member_cap()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxMemberSubjects = 1 }, Stamp(60), "seed");
        await harness.Admin.AddMemberAsync(Tenant, "bob");

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(async () => await harness.Admin.AddMemberAsync(Tenant, "carol"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Dimension, Is.EqualTo(TenantAccessCaps.MemberSubjectsDimension));
            Assert.That(ex.TenantId, Is.EqualTo(Tenant));
            Assert.That(harness.Committed(Tenant).MemberSubjects, Is.EqualTo(new[] { "bob" }));
        });
    }

    [Test]
    public void AddMemberAsync_validates_a_cluster_group_against_the_identity_directory_when_required()
    {
        var principal = new DirectoryPrincipal("bob", "Bob", DirectoryPrincipalKind.User);
        var harness = Build(Alice, identityDirectory: new FakeIdentityDirectory(principal), validationRequired: true);

        Assert.That(
            async () => await harness.Admin.AddMemberAsync(Tenant, "bob", TenantSubjectKind.ClusterGroup),
            Throws.TypeOf<LatticeDirectoryValidationException>());
    }

    [Test]
    public async Task AddMemberAsync_does_not_resolve_a_tenant_group_upstream()
    {
        var directory = new FakeIdentityDirectory(principal: null);
        var harness = Build(Alice, identityDirectory: directory, validationRequired: true);
        harness.Store.SeedGroup("t/acme/eng");

        await harness.Admin.AddMemberAsync(Tenant, "eng", TenantSubjectKind.TenantGroup);

        Assert.That(directory.Resolved, Is.Empty);
    }

    [Test]
    public async Task RemoveMemberAsync_removes_an_entry_once()
    {
        var harness = Build(Alice);
        await harness.Admin.AddMemberAsync(Tenant, "bob");

        var first = await harness.Admin.RemoveMemberAsync(Tenant, "bob");
        var second = await harness.Admin.RemoveMemberAsync(Tenant, "bob");

        Assert.Multiple(() =>
        {
            Assert.That(first.Changed, Is.True);
            Assert.That(second.Changed, Is.False);
            Assert.That(harness.Committed(Tenant).MemberSubjects, Is.Empty);
        });
    }

    [Test]
    public async Task ListMembersAsync_pages_the_member_set_with_kinds_and_hides_foreign_entries()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("entra-devs");
        var record = harness.Committed(Tenant);
        record.AddMemberSubject("zoe", Stamp(60), "seed");
        record.AddMemberSubject("t/acme/eng", Stamp(61), "seed");
        record.AddMemberSubject("entra-devs", Stamp(62), "seed");
        record.AddMemberSubject("t/globex/eng", Stamp(63), "seed");

        var first = await harness.Admin.ListMembersAsync(Tenant, new TenantAccessPageRequest { PageSize = 2 });
        var second = await harness.Admin.ListMembersAsync(Tenant, new TenantAccessPageRequest { PageSize = 2, PageToken = first.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries, Is.EqualTo(new[]
            {
                new TenantMemberEntry { SubjectId = "entra-devs", Kind = TenantSubjectKind.ClusterGroup },
                new TenantMemberEntry { SubjectId = "eng", Kind = TenantSubjectKind.TenantGroup },
            }));
            Assert.That(first.NextPageToken, Is.EqualTo("t/acme/eng"));
            Assert.That(second.Entries, Is.EqualTo(new[] { new TenantMemberEntry { SubjectId = "zoe", Kind = TenantSubjectKind.User } }));
            Assert.That(second.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListMembersAsync_does_not_repeat_the_admin_set()
    {
        var harness = Build(Alice);

        var page = await harness.Admin.ListMembersAsync(Tenant, new TenantAccessPageRequest());

        Assert.That(page.Entries, Is.Empty);
    }

    [Test]
    public async Task ResolveSubjectAsync_reports_membership_through_a_tenant_group()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.SeedEdge("t/acme/eng", "bob");
        harness.Committed(Tenant).AddMemberSubject("t/acme/eng", Stamp(60), "seed");

        var resolution = await harness.Admin.ResolveSubjectAsync(Tenant, "bob");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.IsAdmin, Is.False);
            Assert.That(resolution.IsMember, Is.True);
            Assert.That(resolution.AdminEntries, Is.Empty);
            Assert.That(resolution.MemberEntries, Is.EqualTo(new[] { new TenantMemberEntry { SubjectId = "eng", Kind = TenantSubjectKind.TenantGroup } }));
        });
    }

    [Test]
    public async Task ResolveSubjectAsync_reports_an_admin_through_a_nested_cluster_group_as_a_member_too()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/admins");
        harness.Store.SeedEdge("entra-ops", "carol");
        harness.Store.SeedEdge("t/acme/admins", "entra-ops", MembershipMemberKind.Group);
        harness.Committed(Tenant).AddAdminSubject("t/acme/admins", Stamp(60), "seed");

        var resolution = await harness.Admin.ResolveSubjectAsync(Tenant, "carol");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.IsAdmin, Is.True);
            Assert.That(resolution.IsMember, Is.True);
            Assert.That(resolution.AdminEntries.Select(e => e.SubjectId), Is.EqualTo(new[] { "admins" }));
            Assert.That(resolution.MemberEntries, Is.Empty);
        });
    }

    [Test]
    public async Task ResolveSubjectAsync_resolves_a_tenant_group_subject_by_local_name()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");
        harness.Committed(Tenant).AddMemberSubject("t/acme/eng", Stamp(60), "seed");

        var resolution = await harness.Admin.ResolveSubjectAsync(Tenant, "eng", TenantSubjectKind.TenantGroup);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.SubjectId, Is.EqualTo("eng"));
            Assert.That(resolution.SubjectKind, Is.EqualTo(TenantSubjectKind.TenantGroup));
            Assert.That(resolution.IsMember, Is.True);
        });
    }

    [Test]
    public async Task ResolveSubjectAsync_ignores_a_foreign_entry_that_names_one_of_the_subjects_groups()
    {
        var harness = Build(Alice);
        harness.Store.SeedEdge("t/globex/eng", "bob");
        harness.Committed(Tenant).AddMemberSubject("t/globex/eng", Stamp(60), "seed");

        var resolution = await harness.Admin.ResolveSubjectAsync(Tenant, "bob");

        Assert.That(resolution.IsMember, Is.False);
    }

    [Test]
    public async Task ResolveSubjectAsync_of_an_unknown_subject_is_neither()
    {
        var harness = Build(Alice);

        var resolution = await harness.Admin.ResolveSubjectAsync(Tenant, "stranger");

        Assert.Multiple(() =>
        {
            Assert.That(resolution.IsAdmin, Is.False);
            Assert.That(resolution.IsMember, Is.False);
        });
    }

    [Test]
    public async Task ResolveSubjectAsync_reports_an_exact_id_admin()
    {
        var harness = Build(Operator);

        var resolution = await harness.Admin.ResolveSubjectAsync(Tenant, Alice);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.IsAdmin, Is.True);
            Assert.That(resolution.AdminEntries, Is.EqualTo(new[] { new TenantMemberEntry { SubjectId = Alice, Kind = TenantSubjectKind.User } }));
        });
    }
}
