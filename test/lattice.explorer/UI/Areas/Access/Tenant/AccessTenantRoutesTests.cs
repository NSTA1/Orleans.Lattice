using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// Issue #4158: the tenant Access routes. Each is declared in
/// <see cref="AccessRoutes"/>, completes on the address line, and - once the
/// posture probe reports the tenant's access administration delegated to the
/// caller - renders its page shell (title, navigation, loading, empty and error
/// states); otherwise the same address keeps its cluster-wide page.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessTenantRoutesTests : AccessTestContext
{
    private int _notFound;

    [SetUp]
    public void UseAcme()
    {
        UseTenancy("acme");
        Navigation.OnNotFound += (_, _) => _notFound++;
    }

    [Test]
    public void The_tenant_routes_are_declared_rooted_at_the_tenant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessRoutes.TenantRoot("acme").Format(), Is.EqualTo("/t/acme/access"));
            Assert.That(AccessRoutes.TenantGroups("acme").Format(), Is.EqualTo("/t/acme/access/groups"));
            Assert.That(AccessRoutes.TenantGroup("acme", "ops").Format(), Is.EqualTo("/t/acme/access/groups/ops"));
            Assert.That(AccessRoutes.TenantMembers("acme").Format(), Is.EqualTo("/t/acme/access/members"));
            Assert.That(AccessRoutes.TenantRules("acme").Format(), Is.EqualTo("/t/acme/access/rules"));
            Assert.That(AccessRoutes.TenantRule("acme", "readers").Format(), Is.EqualTo("/t/acme/access/rules/readers"));
            Assert.That(AccessRoutes.TenantExplain("acme").Format(), Is.EqualTo("/t/acme/access/explain"));
            Assert.That(AccessRoutes.TenantSections("acme").Select(section => section.Label), Is.EqualTo(new[] { "Groups", "Members", "Rules", "Explain" }));
            Assert.That(AccessRoutes.TenantMembersRoute, Is.EqualTo("/t/{tenant}/access/members"));
        });
    }

    [Test]
    public void The_tenant_route_builders_refuse_a_missing_tenant_or_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => AccessRoutes.TenantGroups(string.Empty), Throws.ArgumentException);
            Assert.That(() => AccessRoutes.TenantRoot(null!), Throws.ArgumentNullException);
            Assert.That(() => AccessRoutes.TenantGroup("acme", string.Empty), Throws.ArgumentException);
            Assert.That(() => AccessRoutes.TenantRule("acme", null!), Throws.ArgumentNullException);
            Assert.That(() => AccessRoutes.TenantMembers(string.Empty), Throws.ArgumentException);
            Assert.That(() => AccessRoutes.TenantRules(string.Empty), Throws.ArgumentException);
            Assert.That(() => AccessRoutes.TenantExplain(string.Empty), Throws.ArgumentException);
        });
    }

    [Test]
    public void Every_tenant_route_is_answered_by_a_page()
    {
        (Type Page, string Address)[] routes =
        [
            (typeof(AccessGroupsPage), "/t/acme/access/groups"),
            (typeof(AccessGroupPage), "/t/acme/access/groups/ops"),
            (typeof(AccessMembersPage), "/t/acme/access/members"),
            (typeof(AccessRulesPage), "/t/acme/access/rules"),
            (typeof(AccessRulePage), "/t/acme/access/rules/readers"),
            (typeof(AccessExplainPage), "/t/acme/access/explain"),
        ];

        Assert.Multiple(() =>
        {
            foreach (var (page, address) in routes)
            {
                Assert.That(ExplorerPageRoutes.For(page).Answers(ExplorerAddress.Parse(address)), Is.True, address);
            }

            Assert.That(ExplorerPageRoutes.For(typeof(AccessMembersPage)).Answers(ExplorerAddress.Parse("/access/members")), Is.False, "a member set has no cluster-wide address");
        });
    }

    [Test]
    public void The_groups_route_renders_its_shell_with_the_empty_state()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=groups]").TextContent, Does.Contain("Tenant acme has no groups of its own yet."));
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Access"));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.ListGroupsAsync)));
        });
    }

    [Test]
    public void The_groups_route_counts_the_tenants_own_groups()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops").WithGroup("acme", "eng").WithGroup("globex", "finance");

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=groups] .lt-access-count").TextContent, Is.EqualTo("2 groups")));
    }

    [Test]
    public void The_groups_route_shows_its_error_state_when_the_directory_cannot_be_read()
    {
        TenantFacades.AsTenantAdmin();
        TenantFacades.ServesDirectory = false;

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=groups]").TextContent, Does.Contain("Groups could not be read"));
            Assert.That(AccessForms.Button(cut, "Try again"), Is.Not.Null);
        });
    }

    [Test]
    public void The_group_route_renders_its_shell_with_the_groups_full_id()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops", "Operators");

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=group] .lt-access-provenance__id").TextContent, Is.EqualTo("t/acme/ops"));
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("ops"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Is.EqualTo("Operators"));
            Assert.That(_notFound, Is.Zero);
        });
    }

    [Test]
    public void The_group_route_of_another_tenants_group_is_not_found()
    {
        TenantFacades.AsTenantAdmin().WithGroup("globex", "ops");

        RenderAt<AccessGroupPage>("t/acme/access/groups/ops").WaitUntil(() => Assert.That(_notFound, Is.EqualTo(1)));
    }

    [Test]
    public void The_members_route_renders_its_shell_with_the_empty_state()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=members]").TextContent, Does.Contain("no members besides its administrators"));
            Assert.That(cut.FindAll(".lt-access-nav__link[aria-current=page]").Select(link => link.TextContent), Is.EqualTo(new[] { "Members" }));
        });
    }

    [Test]
    [TestCase(false, "off", "Delegated tenant access administration is off")]
    [TestCase(true, "not-permitted", "administered by its administrators")]
    public void The_members_route_says_why_when_it_is_not_delegated(bool enabled, string standing, string sentence)
    {
        TenantFacades.AsMember();
        TenantFacades.Gate.Enabled = enabled;

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            var reason = cut.Find("[data-lt-tenant-access]");
            Assert.That(reason.GetAttribute("data-lt-tenant-access"), Is.EqualTo(standing));
            Assert.That(reason.TextContent, Does.Contain(sentence));
            Assert.That(cut.FindAll("[data-lt-tenant-view]"), Is.Empty);
        });
    }

    [Test]
    public void The_members_page_at_a_cluster_wide_address_is_not_found()
    {
        RenderAt<AccessMembersPage>("access/members");

        Assert.That(_notFound, Is.EqualTo(1));
    }

    [Test]
    [TestCase("t/acme/access/rules")]
    [TestCase("t/acme/access")]
    public void The_rules_route_renders_its_shell_instead_of_the_cluster_listing(string address)
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessRulesPage>(address);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=rules]").TextContent, Does.Contain("No rules govern tenant acme's trees yet"));
            Assert.That(Admin.RuleRequests, Is.Empty, "the cluster's rule store is not read");
        });
    }

    [Test]
    public async Task The_rule_route_renders_its_shell_for_a_tenant_rule()
    {
        TenantFacades.AsTenantAdmin();
        await TenantFacades.PolicyFake.PutRuleAsync("acme", new TenantRuleDraft
        {
            RuleId = "readers",
            SubjectId = "ops",
            SubjectKind = TenantSubjectKind.TenantGroup,
            ScopeKind = TenantRuleScopeKind.TenantWide,
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Allow,
        });

        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/readers");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=rule]").TextContent, Does.Contain("tenant acme's own tier"));
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("readers"));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.GetRuleAsync)));
        });
    }

    [Test]
    public void The_rule_route_of_a_rule_the_tenant_does_not_have_is_not_found()
    {
        TenantFacades.AsTenantAdmin();

        RenderAt<AccessRulePage>("t/acme/access/rules/missing").WaitUntil(() => Assert.That(_notFound, Is.EqualTo(1)));
    }

    [Test]
    public void The_explain_route_renders_its_shell()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessExplainPage>("t/acme/access/explain");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=explain]").TextContent, Does.Contain("Nothing explained yet"));
            Assert.That(cut.FindAll(".lt-access-nav__link[aria-current=page]").Select(link => link.TextContent), Is.EqualTo(new[] { "Explain" }));
        });
    }

    [Test]
    public void Without_delegation_the_explain_route_keeps_the_cluster_explain()
    {
        var cut = RenderAt<AccessExplainPage>("t/acme/access/explain");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[data-lt-tenant-view]"), Is.Empty);
            Assert.That(AccessForms.HasField(cut, "Subject"), Is.True);
        });
    }

    [Test]
    public void A_shells_reads_are_cancelled_when_it_is_left()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-tenant-view=groups]"), Has.Count.EqualTo(1)));

        var view = cut.FindComponent<TenantGroupsView>().Instance;
        view.Dispose();

        Assert.That(view.Lifetime.IsLeft, Is.True);
    }
}
