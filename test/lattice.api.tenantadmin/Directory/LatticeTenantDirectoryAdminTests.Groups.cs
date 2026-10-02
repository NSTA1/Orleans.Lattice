using Orleans.Lattice;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>Tenant group create, read, list and the removal cascade.</summary>
public sealed partial class LatticeTenantDirectoryAdminTests
{
    [Test]
    public async Task UpsertGroupAsync_stores_the_group_under_the_composed_tenant_group_id()
    {
        var harness = Build(Alice);

        var stored = await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "eng", DisplayName = "Engineering" });

        Assert.Multiple(() =>
        {
            Assert.That(stored, Is.EqualTo(new TenantGroupDescriptor { Name = "eng", DisplayName = "Engineering" }));
            Assert.That(harness.Store.HasGroup("t/acme/eng"), Is.True);
        });
    }

    [Test]
    public async Task UpsertGroupAsync_replaces_the_display_name_of_an_existing_group()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng", "Old");

        await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "eng", DisplayName = "New" });

        Assert.That((await harness.Admin.GetGroupAsync(Tenant, "eng"))!.DisplayName, Is.EqualTo("New"));
    }

    [Test]
    public void UpsertGroupAsync_rejects_a_null_group() =>
        Assert.That(async () => await Build(Alice).Admin.UpsertGroupAsync(Tenant, null!), Throws.ArgumentNullException);

    [Test]
    public void UpsertGroupAsync_refuses_a_new_group_beyond_the_tenant_cap()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxGroups = 1 }, Stamp(60), "seed");
        harness.Store.SeedGroup("t/acme/one");

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "two" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Dimension, Is.EqualTo(TenantAccessCaps.GroupsDimension));
            Assert.That(harness.Store.HasGroup("t/acme/two"), Is.False);
        });
    }

    [Test]
    public void UpsertGroupAsync_counts_only_the_tenants_own_groups_against_its_cap()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxGroups = 1 }, Stamp(60), "seed");
        harness.Store.SeedGroup("t/globex/one");
        harness.Store.SeedGroup("cluster-group");

        Assert.That(
            async () => await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "one" }),
            Throws.Nothing);
    }

    [Test]
    public void UpsertGroupAsync_replacing_an_existing_group_at_the_cap_is_admitted()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxGroups = 1 }, Stamp(60), "seed");
        harness.Store.SeedGroup("t/acme/one");

        Assert.That(
            async () => await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "one", DisplayName = "x" }),
            Throws.Nothing);
    }

    [Test]
    public async Task UpsertGroupAsync_applies_the_default_cap_when_the_tenant_sets_none()
    {
        var harness = Build(Alice);
        for (var i = 0; i < TenantQuotas.DefaultMaxGroups; i++)
        {
            harness.Store.SeedGroup($"t/acme/g{i:D4}");
        }

        Assert.That(
            async () => await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "overflow" }),
            Throws.TypeOf<LatticeQuotaExceededException>());
        Assert.That(await harness.Admin.GetGroupAsync(Tenant, "overflow"), Is.Null);
    }

    [Test]
    public async Task GetGroupAsync_returns_null_for_a_missing_group_and_never_reads_another_tenants_group()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/globex/eng", "Theirs");

        Assert.That(await harness.Admin.GetGroupAsync(Tenant, "eng"), Is.Null);
    }

    [Test]
    public async Task ListGroupsAsync_pages_the_tenants_own_groups_by_local_name()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/c");
        harness.Store.SeedGroup("t/acme/a", "A");
        harness.Store.SeedGroup("t/acme/b");
        harness.Store.SeedGroup("t/globex/a");
        harness.Store.SeedGroup("cluster");

        var first = await harness.Admin.ListGroupsAsync(Tenant, new TenantAccessPageRequest { PageSize = 2 });
        var second = await harness.Admin.ListGroupsAsync(Tenant, new TenantAccessPageRequest { PageSize = 2, PageToken = first.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries.Select(e => e.Name), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(first.Entries[0].DisplayName, Is.EqualTo("A"));
            Assert.That(first.NextPageToken, Is.EqualTo("b"));
            Assert.That(second.Entries.Select(e => e.Name), Is.EqualTo(new[] { "c" }));
            Assert.That(second.NextPageToken, Is.Null);
        });
    }

    [Test]
    public void ListGroupsAsync_rejects_a_page_token_that_is_not_a_local_group_name()
    {
        var harness = Build(Alice);

        Assert.That(
            async () => await harness.Admin.ListGroupsAsync(Tenant, new TenantAccessPageRequest { PageToken = "t/globex/a" }),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RemoveGroupAsync_of_a_missing_group_is_an_idempotent_no_op()
    {
        var harness = Build(Alice);

        var result = await harness.Admin.RemoveGroupAsync(Tenant, "ghost");

        Assert.Multiple(() =>
        {
            Assert.That(result.Removed, Is.False);
            Assert.That(result.EdgesRemoved, Is.Zero);
            Assert.That(result.RemovedRuleIds, Is.Empty);
            Assert.That(harness.Rules.Calls, Is.Zero);
            Assert.That(harness.Store.Cascades, Is.Zero);
            Assert.That(harness.Registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task RemoveGroupAsync_cascades_edges_member_and_admin_entries_and_tenant_rules()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.SeedGroup("t/acme/all");
        harness.Store.SeedEdge("t/acme/eng", "bob");
        harness.Store.SeedEdge("t/acme/eng", "entra-devs", Orleans.Lattice.Membership.MembershipMemberKind.Group);
        harness.Store.SeedEdge("t/acme/all", "t/acme/eng", Orleans.Lattice.Membership.MembershipMemberKind.Group);
        var record = harness.Committed(Tenant);
        record.AddMemberSubject("t/acme/eng", Stamp(60), "seed");
        record.AddAdminSubject("t/acme/eng", Stamp(61), "seed");
        harness.Rules.Seed("tenant:acme:read-eng", "t/acme/eng");
        harness.Rules.Seed("tenant:acme:read-all", "t/acme/all");

        var result = await harness.Admin.RemoveGroupAsync(Tenant, "eng");

        var committed = harness.Committed(Tenant);
        Assert.Multiple(() =>
        {
            Assert.That(result.Removed, Is.True);
            Assert.That(result.TenantId, Is.EqualTo(Tenant));
            Assert.That(result.GroupName, Is.EqualTo("eng"));
            Assert.That(result.EdgesRemoved, Is.EqualTo(3));
            Assert.That(result.RemovedFromMemberSet, Is.True);
            Assert.That(result.RemovedFromAdminSet, Is.True);
            Assert.That(result.RemovedRuleIds, Is.EqualTo(new[] { "read-eng" }));
            Assert.That(harness.Store.HasGroup("t/acme/eng"), Is.False);
            Assert.That(harness.Store.Edges.Any(e => e.GroupId == "t/acme/eng" || e.MemberId == "t/acme/eng"), Is.False);
            Assert.That(committed.HasMemberSubject("t/acme/eng"), Is.False);
            Assert.That(committed.HasAdminSubject("t/acme/eng"), Is.False);
            Assert.That(committed.HasAdminSubject(Alice), Is.True);
            Assert.That(harness.Rules.Rules.Select(r => r.RuleId), Is.EqualTo(new[] { "tenant:acme:read-all" }));
        });
    }

    [Test]
    public async Task RemoveGroupAsync_of_a_group_in_neither_set_does_not_write_the_registry()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");

        var result = await harness.Admin.RemoveGroupAsync(Tenant, "eng");

        Assert.Multiple(() =>
        {
            Assert.That(result.Removed, Is.True);
            Assert.That(result.RemovedFromMemberSet, Is.False);
            Assert.That(result.RemovedFromAdminSet, Is.False);
            Assert.That(harness.Registry.Puts, Is.Zero);
        });
    }

    [Test]
    public void RemoveGroupAsync_refuses_to_remove_the_last_admin_entry_and_writes_nothing()
    {
        var harness = Build(Operator);
        harness.Store.SeedGroup("t/acme/admins");
        harness.Rules.Seed("tenant:acme:r", "t/acme/admins");
        var record = harness.Committed(Tenant);
        record.RemoveAdminSubject(Alice, Stamp(70), "seed");
        record.AddAdminSubject("t/acme/admins", Stamp(71), "seed");

        var ex = Assert.ThrowsAsync<TenantLastAdminSubjectException>(async () => await harness.Admin.RemoveGroupAsync(Tenant, "admins"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.SubjectId, Is.EqualTo("t/acme/admins"));
            Assert.That(harness.Store.HasGroup("t/acme/admins"), Is.True);
            Assert.That(harness.Rules.Calls, Is.Zero);
            Assert.That(harness.Store.Cascades, Is.Zero);
            Assert.That(harness.Registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task RemoveGroupAsync_rerun_after_success_reports_nothing_left_to_remove()
    {
        var harness = Build(Alice);
        harness.Store.SeedGroup("t/acme/eng");
        await harness.Admin.RemoveGroupAsync(Tenant, "eng");

        var again = await harness.Admin.RemoveGroupAsync(Tenant, "eng");

        Assert.That(again.Removed, Is.False);
    }
}
