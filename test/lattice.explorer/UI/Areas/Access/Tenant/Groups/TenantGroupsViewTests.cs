using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// Issue #4162: the tenant's own Groups list (<c>/t/acme/access/groups</c>) - a
/// booktabs table of each group's name, display name, direct members, and the rules
/// and app role bindings that name it; create with the local name grammar checked as
/// typed; the tenant's group cap, with create disabled at it; delete confirmed with
/// the cascade it will apply; and every loading, empty, error and feature-off state.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantGroupsViewTests : TenantAccessPagesTestContext
{
    [Test]
    public async Task The_table_shows_each_groups_members_rules_and_app_roles()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops", "Operators").WithGroup("acme", "eng").WithGroup("globex", "finance");
        await AddGroupMemberAsync("ops", "alice");
        await AddGroupMemberAsync("ops", "eng", TenantSubjectKind.TenantGroup);
        await NameInTenantRuleAsync("readers", "ops");
        NameInPlatformRule("platform-deny", "ops");
        NameInAppRole("app:crm:viewer:0123", "ops");

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            var rows = Rows(cut);
            Assert.That(rows, Is.EqualTo(new[]
            {
                new[] { "eng", string.Empty, "0", "0", "0" },
                new[] { "ops", "Operators", "2", "2", "1" },
            }));
            Assert.That(cut.Find("thead").TextContent, Does.Contain("Direct members").And.Contain("Rules").And.Contain("App roles"));
            Assert.That(cut.Markup, Does.Not.Contain("finance"));
            Assert.That(cut.Find("[data-lt-tenant-view=groups] a[href]").GetAttribute("href"), Does.EndWith("t/acme/access/groups/eng"));
        });
    }

    [Test]
    public void A_tenant_with_no_groups_shows_the_empty_state_and_offers_create()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty-state, [data-lt-tenant-view=groups]").TextContent, Does.Contain("Tenant acme has no groups of its own yet."));
            Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public void While_the_groups_are_read_a_skeleton_is_shown()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TaskCompletionSource<TenantGroupPage>().Task);
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=groups]").InnerHtml, Does.Contain("Loading groups"));
            Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void A_failed_read_shows_the_error_state_and_tries_again()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(
            _ => Task.FromException<TenantGroupPage>(new InvalidOperationException("down")),
            _ => Task.FromResult(new TenantGroupPage()));
        UseDirectory(directory);

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=groups]").TextContent, Does.Contain("Groups could not be read")));
        AccessForms.Button(cut, "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=groups]").TextContent, Does.Contain("no groups of its own yet")));
    }

    [Test]
    public void The_cap_usage_is_shown_from_the_posture()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        TenantFacades.PolicyFake.Groups = new TenantQuotaDimensionUsage { Usage = 1, Limit = 500 };

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            var cap = cut.Find("[data-lt-cap=groups]");
            Assert.That(cap.TextContent, Is.EqualTo("1 of 500 groups"));
            Assert.That(cap.GetAttribute("data-lt-at-cap"), Is.EqualTo("false"));
            Assert.That(cut.FindAll("[data-lt-cap-reason]"), Is.Empty);
        });
    }

    [Test]
    public void At_the_cap_create_is_disabled_with_the_reason()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops").WithGroup("acme", "eng");
        TenantFacades.PolicyFake.Groups = new TenantQuotaDimensionUsage { Usage = 2, Limit = 2 };

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("[data-lt-cap=groups]").GetAttribute("data-lt-at-cap"), Is.EqualTo("true"));
            Assert.That(cut.Find("[data-lt-cap-reason=groups]").TextContent, Does.Contain("Tenant acme is at its cap of 2 groups"));
        });
    }

    [Test]
    public void The_name_is_checked_against_the_grammar_as_it_is_typed()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.False));

        AccessForms.Button(cut, "New group").Click();
        AccessForms.Type(cut, "Group name", "Bad Name");

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Group name"), Is.EqualTo(TenantGroupFormat.NameGrammarMessage)));
        AccessForms.Type(cut, "Group name", "eng-team");
        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Group name"), Is.Null));
    }

    [Test]
    public void Create_writes_the_group_and_opens_it()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.False));

        AccessForms.Button(cut, "New group").Click();
        AccessForms.Type(cut, "Group name", "eng-team");
        AccessForms.Type(cut, "Display name (optional)", "Engineering");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(TenantFacades.DirectoryFake.GetGroupAsync("acme", "eng-team").Result?.DisplayName, Is.EqualTo("Engineering"));
            Assert.That(Navigation.Uri, Does.EndWith("/t/acme/access/groups/eng-team"));
        });
    }

    [Test]
    public void Create_refuses_a_name_the_tenant_already_has()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(1)));

        AccessForms.Button(cut, "New group").Click();
        AccessForms.Type(cut, "Group name", "ops");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Group name"), Does.Contain("ops"));
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.UpsertGroupAsync)));
        });
    }

    [Test]
    public void A_cap_refusal_on_create_is_shown_in_the_dialog()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListGroupsAsync(default!, default!, default).ReturnsForAnyArgs(new TenantGroupPage());
        directory.GetGroupAsync(default!, default!, default).ReturnsForAnyArgs((TenantGroupDescriptor?)null);
        directory.UpsertGroupAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantGroupDescriptor>(new Orleans.Lattice.LatticeQuotaExceededException("cap", string.Empty, "MaxGroups", 500, 500, "acme")));
        UseDirectory(directory);
        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.False));
        AccessForms.Button(cut, "New group").Click();
        AccessForms.Type(cut, "Group name", "eng");

        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-field=form]").TextContent, Does.Contain("at its cap of groups")));
    }

    [Test]
    public async Task Delete_is_confirmed_with_the_cascade_it_will_apply()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops").WithGroup("acme", "eng");
        await AddGroupMemberAsync("ops", "alice");
        await AddGroupMemberAsync("ops", "bob");
        await AddGroupMemberAsync("ops", "eng", TenantSubjectKind.TenantGroup);
        await NameInTenantRuleAsync("readers", "ops");
        await NameInTenantRuleAsync("writers", "ops");
        NameInAppRole("app:crm:viewer:0123", "ops");
        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(2)));

        cut.Find("button[aria-label='Delete ops']").Click();

        cut.WaitUntil(() => Assert.That(
            cut.Find("[data-lt-cascade-text]").TextContent,
            Is.EqualTo("Deleting ops removes 3 member entries, 2 rules, 1 app binding.")));
        Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.RemoveGroupAsync)), "nothing is written before the confirmation");

        AccessForms.Type(cut, "Group name", "ops");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut).Select(row => row[0]), Is.EqualTo(new[] { "eng" }));
            Assert.That(TenantFacades.PolicyFake.GetRuleAsync("acme", "readers").Result, Is.Null);
            Assert.That(ToastMessages, Does.Contain("Group ops deleted, with 3 membership entries, 2 rules."));
        });
    }

    [Test]
    public void With_the_feature_off_the_tenant_groups_are_not_reachable()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        TenantFacades.Gate.Enabled = false;

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-access]").GetAttribute("data-lt-tenant-access"), Is.EqualTo("off"));
            Assert.That(cut.FindAll("[data-lt-tenant-view]"), Is.Empty);
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.ListGroupsAsync)));
        });
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void An_operator_and_a_tenant_admin_both_administer_the_groups(bool operatorCaller)
    {
        if (operatorCaller)
        {
            TenantFacades.AsOperator().WithGroup("acme", "ops");
        }
        else
        {
            TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        }

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut).Select(row => row[0]), Is.EqualTo(new[] { "ops" }));
            Assert.That(AccessForms.Button(cut, "New group").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public void A_caller_who_does_not_administer_the_tenant_is_not_shown_its_groups()
    {
        TenantFacades.AsMember().WithGroup("acme", "ops");

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-access]").GetAttribute("data-lt-tenant-access"), Is.EqualTo("not-permitted"));
            Assert.That(cut.Markup, Does.Not.Contain("data-lt-tenant-view"));
        });
    }

    private static List<string[]> Rows(IRenderedComponent<AccessGroupsPage> cut) =>
        [.. cut.FindAll("[data-lt-tenant-view=groups] tbody tr")
            .Select(row => row.QuerySelectorAll("th, td").Take(5).Select(cell => cell.TextContent.Trim()).ToArray())];
}
