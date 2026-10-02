using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// Issue #4158: the Access navigation at a tenant-rooted address. It shows the
/// tenant's Groups, Members, Rules and Explain only when the posture probe
/// reports delegated tenant access administration enabled and the caller an
/// admin of the tenant or a platform operator; a member, or the feature off,
/// keeps today's Rules, Groups and Explain, and the tenant Groups page keeps its
/// cluster message, reworded to say why.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessTenantNavTests : AccessTestContext
{
    private static readonly string[] DelegatedSections = ["Groups", "Members", "Rules", "Explain"];
    private static readonly string[] ClusterSections = ["Rules", "Groups", "Explain"];

    [SetUp]
    public void UseAcme() => UseTenancy("acme");

    [Test]
    public void An_operator_sees_the_tenants_groups_members_rules_and_explain()
    {
        TenantFacades.AsOperator();

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(Sections(cut), Is.EqualTo(DelegatedSections));
            Assert.That(Hrefs(cut), Is.EqualTo(new[] { "t/acme/access/groups", "t/acme/access/members", "t/acme/access/rules", "t/acme/access/explain" }));
            Assert.That(Marked(cut), Is.EqualTo(new[] { "Groups" }));
            Assert.That(cut.FindAll("[data-lt-tenant-view=groups]"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void A_tenant_admin_sees_the_tenants_groups_members_rules_and_explain()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(Sections(cut), Is.EqualTo(DelegatedSections));
            Assert.That(cut.Find("nav").GetAttribute("data-lt-access-scope"), Is.EqualTo("tenant"));
            Assert.That(cut.FindAll("[data-lt-cluster-wide-groups]"), Is.Empty);
        });
    }

    [Test]
    public void A_member_keeps_the_cluster_navigation_and_is_told_why_no_group_is_listed()
    {
        TenantFacades.AsMember();

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(Sections(cut), Is.EqualTo(ClusterSections));
            var caveat = cut.Find("[data-lt-cluster-wide-groups]");
            Assert.That(caveat.GetAttribute("data-lt-tenant-access"), Is.EqualTo("not-permitted"));
            Assert.That(caveat.TextContent, Does.Contain("administered by its administrators"));
            Assert.That(cut.FindAll("[data-lt-tenant-view]"), Is.Empty);
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain("ListGroupsAsync"), "no tenant group is read for a member");
        });
    }

    [Test]
    public void With_the_feature_off_the_cluster_message_says_delegated_administration_is_off()
    {
        TenantFacades.AsTenantAdmin();
        TenantFacades.Gate.Enabled = false;

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(Sections(cut), Is.EqualTo(ClusterSections));
            var caveat = cut.Find("[data-lt-cluster-wide-groups]");
            Assert.That(caveat.GetAttribute("data-lt-tenant-access"), Is.EqualTo("off"));
            Assert.That(caveat.TextContent, Does.StartWith("Delegated tenant access administration is off"));
            Assert.That(caveat.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("access/groups"));
            Assert.That(TenantFacades.Gate.Calls, Is.EqualTo(new[] { "GetPostureAsync" }));
        });
    }

    [Test]
    public void A_head_that_serves_no_tenant_policy_keeps_the_cluster_navigation()
    {
        TenantFacades.AsTenantAdmin();
        TenantFacades.ServesPolicy = false;

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(Sections(cut), Is.EqualTo(ClusterSections));
            Assert.That(cut.Find("[data-lt-cluster-wide-groups]").GetAttribute("data-lt-tenant-access"), Is.EqualTo("unavailable"));
        });
    }

    [Test]
    public void The_cluster_wide_groups_page_never_asks_the_posture_probe()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessGroupsPage>("access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(Sections(cut), Is.EqualTo(ClusterSections));
            Assert.That(TenantFacades.Gate.Calls, Is.Empty);
        });
    }

    [Test]
    public void The_navigation_draws_the_tenant_sections_only_for_a_delegated_tenant_page()
    {
        var delegated = Render<AccessNav>(parameters => parameters
            .Add(nav => nav.Tenant, "acme")
            .Add(nav => nav.Delegated, true)
            .Add(nav => nav.Current, AccessRoutes.MembersSegment));
        var withoutTenant = Render<AccessNav>(parameters => parameters.Add(nav => nav.Delegated, true));

        Assert.Multiple(() =>
        {
            Assert.That(Sections(delegated), Is.EqualTo(DelegatedSections));
            Assert.That(Marked(delegated), Is.EqualTo(new[] { "Members" }));
            Assert.That(delegated.FindAll("[data-lt-command]").Select(link => link.TextContent), Is.EqualTo(new[] { "Explain" }));
            Assert.That(Sections(withoutTenant), Is.EqualTo(ClusterSections));
        });
    }

    private static string[] Sections<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        [.. cut.FindAll(".lt-access-nav__link").Select(link => link.TextContent)];

    private static string?[] Hrefs<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        [.. cut.FindAll(".lt-access-nav__link").Select(link => link.GetAttribute("href"))];

    private static string[] Marked<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        [.. cut.FindAll(".lt-access-nav__link").Where(link => link.GetAttribute("aria-current") == "page").Select(link => link.TextContent)];
}
