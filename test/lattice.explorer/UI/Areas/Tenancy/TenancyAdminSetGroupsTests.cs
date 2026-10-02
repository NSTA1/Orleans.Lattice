using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// Issue #4164: groups as tenant admins. When the tenant's delegated access
/// administration is open to the caller, the admin-set editor takes users, the
/// tenant's own groups and cluster groups through the tenant-aware picker, shows
/// what each entry names, and gives the cluster's last-admin and foreign-group
/// refusals their reasons. When it is not, groups are not offered at all.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyAdminSetGroupsTests : TenancyTestContext
{
    [Test]
    public void Each_entry_shows_what_it_names()
    {
        TenantFacades.AsTenantAdmin();
        Directory.WithGroup("ops-team", "Operations");
        Cluster.Tenants["acme"].Admins.Add("t/acme/owners");
        Cluster.Tenants["acme"].Admins.Add("ops-team");
        Cluster.Tenants["acme"].Admins.Add("t/globex/rivals");

        var cut = RenderMembers();

        cut.WaitUntil(() => Assert.That(Rows(cut), Is.EqualTo(new[]
        {
            new[] { "ops-team", "Cluster group" },
            new[] { FakeTenancyCluster.Caller, "User" },
            new[] { "t/acme/owners", "Tenant group" },
            new[] { "t/globex/rivals", "Another tenant's group, not counted" },
        })));
        Assert.That(cut.Find("section[data-lt-admin-set]").GetAttribute("data-lt-admin-set"), Is.EqualTo("groups"));
    }

    [Test]
    public void One_of_the_tenants_own_groups_is_added_by_name_and_recorded_by_its_full_id()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "owners", "Owners");
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        TenancyForms.Choose(cut, "Admin subject kind", AccessSubjectPicker.TenantGroupValue);
        Assert.That(SuggestionFields.Offers(cut, "Admin subject", "own"), Is.EqualTo(new[] { "owners" }));
        cut.FindAll("[role=option]").Single(option => option.QuerySelector(".lt-combobox__value")!.TextContent == "owners").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-access-provenance__id").TextContent, Is.EqualTo("t/acme/owners")));
        cut.Find("form.lt-tenancy-add").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Admins, Does.Contain("t/acme/owners"));
            Assert.That(Rows(cut), Has.Some.EqualTo(new[] { "t/acme/owners", "Tenant group" }));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("t/acme/owners can now administer tenant acme."));
        });
    }

    [Test]
    public void A_cluster_group_is_added_and_shown_as_one()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        TenancyForms.Choose(cut, "Admin subject kind", AccessSubjectPicker.ClusterGroupValue);
        TenancyForms.Type(cut, "Admin subject", "ops-team");
        cut.Find("form.lt-tenancy-add").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Admins, Does.Contain("ops-team"));
            Assert.That(Rows(cut), Has.Some.EqualTo(new[] { "ops-team", "Cluster group" }));
        });
    }

    [Test]
    public void Another_tenants_group_typed_as_a_cluster_group_is_refused_before_anything_is_sent()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        TenancyForms.Choose(cut, "Admin subject kind", AccessSubjectPicker.ClusterGroupValue);
        TenancyForms.Type(cut, "Admin subject", "t/globex/rivals");
        cut.Find("form.lt-tenancy-add").Submit();

        cut.WaitUntil(() => Assert.That(TenancyForms.ErrorOf(cut, "Admin subject"), Is.EqualTo(AccessSubjectPicker.ForeignTenantGroupMessage)));
        Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.AddAdminSubjectAsync)));
    }

    [Test]
    public void The_clusters_foreign_group_refusal_is_given_its_reason()
    {
        TenantFacades.AsTenantAdmin();
        Cluster.Fail(nameof(FakeTenancyCluster.AddAdminSubjectAsync), new TenantAccessConfinementException(
            "acme", TenantAccessConfinementRule.ForeignTenantGroup, "raw server text", "subjectId"));
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        TenancyForms.Type(cut, "Admin subject", "alice");
        cut.Find("form.lt-tenancy-add").Submit();

        cut.WaitUntil(() => Assert.That(TenancyForms.ErrorOf(cut, "Admin subject"), Is.EqualTo(TenancyFailure.ForeignTenantGroupMessage)));
    }

    [Test]
    public void The_last_admin_refusal_names_the_entry_and_why_a_group_cannot_be_removed()
    {
        TenantFacades.AsTenantAdmin();
        Cluster.Tenants["acme"].Admins.Add("t/acme/owners");
        Cluster.Fail(nameof(FakeTenancyCluster.RemoveAdminSubjectAsync), new TenantLastAdminSubjectException("acme", "t/acme/owners"));
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2)));

        cut.Find("button[aria-label='Remove t/acme/owners']").Click();
        TenancyForms.Type(cut, "Subject name", "t/acme/owners");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() => Assert.That(
            cut.Find("[data-lt-admin-refusal]").TextContent,
            Is.EqualTo("Tenant acme keeps at least one admin subject, so the tenant group t/acme/owners cannot be removed: it is the last one. "
                + "A group counts as one admin subject, however many members it has. Add the replacement before removing it.")));
        Assert.That(cut.Find("[data-lt-admin-refusal]").GetAttribute("role"), Is.EqualTo("alert"));
    }

    [Test]
    public void With_the_feature_off_groups_are_not_offered_and_entries_carry_no_kind()
    {
        Directory.WithPrincipal("alice@example.com", "Alice", DirectoryPrincipalKind.User)
            .WithPrincipal("ops", "Operations", DirectoryPrincipalKind.Group);

        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        Assert.Multiple(() =>
        {
            Assert.That(SuggestionFields.Offers(cut, "Subject id", "o"), Does.Contain("alice@example.com").And.Not.Contain("ops"));
            Assert.That(cut.FindAll("label").Select(label => label.TextContent.Trim()), Does.Not.Contain("Admin subject kind"));
            Assert.That(cut.FindAll("thead th").Select(header => header.TextContent.Trim()), Does.Not.Contain("Kind"));
            Assert.That(cut.Find("section[data-lt-admin-set]").GetAttribute("data-lt-admin-set"), Is.EqualTo("users"));
        });
    }

    [Test]
    public void A_caller_the_feature_is_not_delegated_to_keeps_the_user_only_editor()
    {
        TenantFacades.AsMember();

        var cut = RenderMembers();

        cut.WaitUntil(() => Assert.That(cut.Find("section[data-lt-admin-set]").GetAttribute("data-lt-admin-set"), Is.EqualTo("users")));
        Assert.That(cut.FindAll("label").Select(label => label.TextContent.Trim()), Does.Contain("Subject id"));
    }

    [Test]
    public void A_refusal_naming_another_tenants_group_or_the_feature_off_has_its_own_sentence()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyFailure.From(new TenantAccessConfinementException("acme", TenantAccessConfinementRule.ForeignTenantGroup, "raw")),
                Is.EqualTo(new TenancyFailure(TenancyFailureKind.Refused, TenancyFailure.ForeignTenantGroupMessage)));
            Assert.That(TenancyFailure.From(new TenantAccessAdministrationDisabledException("acme")),
                Is.EqualTo(new TenancyFailure(TenancyFailureKind.Refused, TenancyFailure.DelegatedAccessOffMessage)));
            Assert.That(TenancyFailure.From(new TenantAccessConfinementException("acme", TenantAccessConfinementRule.GroupNesting, "raw"))!.Kind,
                Is.EqualTo(TenancyFailureKind.Invalid), "another confinement rule keeps the argument sentence");
        });
    }

    [Test]
    public void The_grammar_alone_names_tenant_groups()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenancyAdminEntries.KindFromGrammar("acme", "t/acme/owners"), Is.EqualTo(TenancyAdminEntryKind.TenantGroup));
            Assert.That(TenancyAdminEntries.KindFromGrammar("acme", "t/globex/owners"), Is.EqualTo(TenancyAdminEntryKind.OtherTenantGroup));
            Assert.That(TenancyAdminEntries.KindFromGrammar("acme", "t/acme/Not Valid"), Is.EqualTo(TenancyAdminEntryKind.OtherTenantGroup));
            Assert.That(TenancyAdminEntries.KindFromGrammar("acme", "alice"), Is.Null);
            Assert.That(TenancyAdminEntry.Label(TenancyAdminEntryKind.Unknown), Is.EqualTo("User or group"));
            Assert.That(new TenancyAdminEntry("t/acme/x", TenancyAdminEntryKind.TenantGroup).IsGroup, Is.True);
            Assert.That(new TenancyAdminEntry("alice", TenancyAdminEntryKind.User).IsGroup, Is.False);
        });
    }

    [Test]
    public async Task Without_an_auth_facade_an_entry_that_is_not_a_tenant_group_is_not_guessed()
    {
        var entries = await TenancyAdminEntries.ClassifyAsync(null, "acme", ["alice", "t/acme/owners"], CancellationToken.None);

        Assert.That(entries.Select(entry => entry.Kind), Is.EqualTo(new[] { TenancyAdminEntryKind.Unknown, TenancyAdminEntryKind.TenantGroup }));
    }

    [Test]
    public async Task A_directory_group_is_a_cluster_group_and_a_failed_read_is_not_guessed()
    {
        Directory.WithPrincipal("dir-group", "Directory group", DirectoryPrincipalKind.Group);

        var entries = await TenancyAdminEntries.ClassifyAsync(Directory, "acme", ["dir-group", "bob"], CancellationToken.None);
        Directory.Fail(nameof(Access.FakeAuthAdmin.ListGroupsAsync), new TimeoutException());
        Directory.Fail(nameof(Access.FakeAuthAdmin.ResolveDirectoryPrincipalAsync), new TimeoutException());
        var unread = await TenancyAdminEntries.ClassifyAsync(Directory, "acme", ["bob"], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.Kind), Is.EqualTo(new[] { TenancyAdminEntryKind.ClusterGroup, TenancyAdminEntryKind.User }));
            Assert.That(unread.Single().Kind, Is.EqualTo(TenancyAdminEntryKind.Unknown));
        });
    }

    private IRenderedComponent<TenancyMembers> RenderMembers() =>
        RenderSection<TenancyMembers>(parameters => parameters.Add(members => members.TenantId, "acme"));

    private static string[][] Rows(IRenderedComponent<TenancyMembers> cut) =>
        [.. cut.FindAll("tbody tr").Select(row => new[] { row.QuerySelector("th")!.TextContent.Trim(), row.QuerySelectorAll("td")[0].TextContent.Trim() })];
}
