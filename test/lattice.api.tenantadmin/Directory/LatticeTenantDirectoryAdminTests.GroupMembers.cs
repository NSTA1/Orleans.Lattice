using Orleans.Lattice;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>A tenant group's direct members: add, remove, list, confinement, validation and the edge cap.</summary>
public sealed partial class LatticeTenantDirectoryAdminTests
{
    [Test]
    public async Task AddGroupMemberAsync_adds_a_user_edge_once()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");

        var first = await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "bob");
        var second = await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "bob");

        Assert.Multiple(() =>
        {
            Assert.That(first.Changed, Is.True);
            Assert.That(first.GroupName, Is.EqualTo("eng"));
            Assert.That(first.SubjectId, Is.EqualTo("bob"));
            Assert.That(first.SubjectKind, Is.EqualTo(TenantSubjectKind.User));
            Assert.That(second.Changed, Is.False);
            Assert.That(harness.Store.Edges, Is.EqualTo(new[] { ("t/acme/eng", "bob", MembershipMemberKind.User) }));
        });
    }

    [Test]
    public async Task AddGroupMemberAsync_adds_a_cluster_group_as_a_group_edge()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");

        await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "entra-devs", TenantSubjectKind.ClusterGroup);

        Assert.That(harness.Store.Edges, Is.EqualTo(new[] { ("t/acme/eng", "entra-devs", MembershipMemberKind.Group) }));
    }

    [Test]
    public async Task AddGroupMemberAsync_nests_one_of_the_tenants_own_groups_by_local_name()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/all");
        harness.Store.SeedGroup("t/acme/eng");

        await harness.Admin.AddGroupMemberAsync(Tenant, "all", "eng", TenantSubjectKind.TenantGroup);

        Assert.That(harness.Store.Edges, Is.EqualTo(new[] { ("t/acme/all", "t/acme/eng", MembershipMemberKind.Group) }));
    }

    [Test]
    public void AddGroupMemberAsync_refuses_a_nested_tenant_group_that_does_not_exist()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/all");

        Assert.That(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "all", "ghost", TenantSubjectKind.TenantGroup),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("memberId"));
        Assert.That(harness.Store.Edges, Is.Empty);
    }

    [Test]
    public void AddGroupMemberAsync_refuses_a_group_that_does_not_exist()
    {
        var harness = Build(Alice);

        Assert.That(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "ghost", "bob"),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("groupName"));
    }

    [TestCase(TenantSubjectKind.User)]
    [TestCase(TenantSubjectKind.ClusterGroup)]
    public void AddGroupMemberAsync_refuses_a_raw_tenant_group_id_named_as_a_user_or_cluster_group(TenantSubjectKind kind)
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.SeedGroup("t/globex/eng");

        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "t/globex/eng", kind));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.ForeignTenantGroup));
            Assert.That(ex.TenantId, Is.EqualTo(Tenant));
            Assert.That(harness.Store.Edges, Is.Empty);
        });
    }

    [Test]
    public void AddGroupMemberAsync_maps_a_nesting_violation_to_a_confinement_refusal()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.NextAddMemberFailure = new LatticeTenantGroupNestingException("nesting refused");

        var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "bob"));

        Assert.That(ex!.Rule, Is.EqualTo(TenantAccessConfinementRule.GroupNesting));
    }

    [Test]
    public async Task AddGroupMemberAsync_refuses_an_edge_beyond_the_tenant_cap_but_admits_an_existing_one()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxMembershipEdges = 1 }, Stamp(60), "seed");
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.SeedEdge("t/acme/eng", "bob");
        harness.Store.SeedEdge("cluster", "carol");

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "dave"));
        var existing = await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "bob");

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Dimension, Is.EqualTo(TenantAccessCaps.MembershipEdgesDimension));
            Assert.That(existing.Changed, Is.False);
            Assert.That(harness.Store.Edges, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void AddGroupMemberAsync_validates_a_user_against_the_identity_directory_when_required()
    {
        var harness = Build(Alice, identityDirectory: new FakeIdentityDirectory(principal: null), validationRequired: true);
        harness.Store.SeedGroup("t/acme/eng");

        Assert.That(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "typo"),
            Throws.TypeOf<LatticeDirectoryValidationException>());
        Assert.That(harness.Store.Edges, Is.Empty);
    }

    [Test]
    public void AddGroupMemberAsync_refuses_a_cluster_group_that_resolves_to_a_user()
    {
        var principal = new DirectoryPrincipal("bob", "Bob", DirectoryPrincipalKind.User);
        var harness = Build(Alice, identityDirectory: new FakeIdentityDirectory(principal), validationRequired: true);
        harness.Store.SeedGroup("t/acme/eng");

        var ex = Assert.ThrowsAsync<LatticeDirectoryValidationException>(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "bob", TenantSubjectKind.ClusterGroup));

        Assert.That(ex!.ResolvedKind, Is.EqualTo(DirectoryPrincipalKind.User));
    }

    [Test]
    public async Task AddGroupMemberAsync_skips_directory_validation_when_it_is_not_required()
    {
        var directory = new FakeIdentityDirectory(principal: null);
        var harness = Build(Alice, identityDirectory: directory, validationRequired: false);
        harness.Store.SeedGroup("t/acme/eng");

        await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "bob");

        Assert.That(directory.Resolved, Is.Empty);
    }

    [Test]
    public async Task ListGroupMembersAsync_lists_direct_members_in_ordinal_order_with_their_kinds()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.SeedGroup("t/acme/sub");
        harness.Store.SeedGroup("entra-devs");
        harness.Store.SeedEdge("t/acme/eng", "zoe");
        harness.Store.SeedEdge("t/acme/eng", "t/acme/sub", MembershipMemberKind.Group);
        harness.Store.SeedEdge("t/acme/eng", "entra-devs", MembershipMemberKind.Group);

        var members = await harness.Admin.ListGroupMembersAsync(Tenant, "eng");

        Assert.That(members, Is.EqualTo(new[]
        {
            new TenantGroupMember { MemberId = "entra-devs", Kind = TenantSubjectKind.ClusterGroup },
            new TenantGroupMember { MemberId = "sub", Kind = TenantSubjectKind.TenantGroup },
            new TenantGroupMember { MemberId = "zoe", Kind = TenantSubjectKind.User },
        }));
    }

    [Test]
    public async Task ListGroupMembersAsync_classifies_a_directory_group_without_a_record_as_a_cluster_group()
    {
        var principal = new DirectoryPrincipal("entra-devs", "Devs", DirectoryPrincipalKind.Group);
        var harness = Build(Alice, identityDirectory: new FakeIdentityDirectory(principal));
        harness.Store.SeedEdge("t/acme/eng", "entra-devs", MembershipMemberKind.Group);

        var members = await harness.Admin.ListGroupMembersAsync(Tenant, "eng");

        Assert.That(members.Single().Kind, Is.EqualTo(TenantSubjectKind.ClusterGroup));
    }

    [Test]
    public async Task ListGroupMembersAsync_of_a_missing_group_is_empty()
    {
        var harness = Build(Alice);

        Assert.That(await harness.Admin.ListGroupMembersAsync(Tenant, "ghost"), Is.Empty);
    }

    [Test]
    public async Task RemoveGroupMemberAsync_removes_an_edge_once()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.SeedGroup("t/acme/sub");
        harness.Store.SeedEdge("t/acme/eng", "t/acme/sub", MembershipMemberKind.Group);

        var first = await harness.Admin.RemoveGroupMemberAsync(Tenant, "eng", "sub", TenantSubjectKind.TenantGroup);
        var second = await harness.Admin.RemoveGroupMemberAsync(Tenant, "eng", "sub", TenantSubjectKind.TenantGroup);

        Assert.Multiple(() =>
        {
            Assert.That(first.Changed, Is.True);
            Assert.That(second.Changed, Is.False);
            Assert.That(harness.Store.Edges, Is.Empty);
        });
    }

    [Test]
    public void RemoveGroupMemberAsync_refuses_a_raw_tenant_group_id_named_as_a_user()
    {
        var harness = Build(Alice);

        Assert.That(
            async () => await harness.Admin.RemoveGroupMemberAsync(Tenant, "eng", "t/globex/x"),
            Throws.TypeOf<TenantAccessConfinementException>());
    }
}
