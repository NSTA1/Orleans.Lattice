using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// Issue #4164: app role bindings can name the installing tenant's own groups. The
/// binding picker offers them, labelled Tenant, beside the cluster's groups, never
/// another tenant's; a binding naming one is refused before anything is sent, and
/// the cluster's own refusal is given its reason. The role-holding warning's fix
/// joins a tenant group on that tenant's group page.
/// </summary>
public sealed partial class AppReviewPageTests
{
    private const string TenantReview = "/t/acme/apps/catalogue/in-image/task-board";

    [Test]
    public void The_binding_picker_offers_the_tenants_own_groups_beside_the_clusters_and_never_another_tenants()
    {
        UseTenancy("acme");
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops", "Tenant operations").WithGroup("globex", "ops-rivals");
        ClusterGroups("operators", "t/globex/ops-rivals");
        Offer(AppsTestData.TaskBoard());
        var cut = AtTenantBindRoles();

        var offered = SuggestionFields.Offers(cut, "Group for viewer", "op", atLeast: 2);

        Assert.Multiple(() =>
        {
            Assert.That(offered, Is.EqualTo(new[] { "t/acme/ops", "operators" }));
            Assert.That(OfferedDetails(cut), Is.EqualTo(new[] { "Tenant - Tenant operations", "Cluster" }));
            Assert.That(cut.Markup, Does.Not.Contain("ops-rivals"));
        });
    }

    [Test]
    public void With_the_feature_off_only_the_clusters_groups_are_offered()
    {
        UseTenancy("acme");
        TenantFacades.WithGroup("acme", "ops");
        ClusterGroups("operators", "t/acme/ops");
        Offer(AppsTestData.TaskBoard());
        var cut = AtTenantBindRoles();

        var offered = SuggestionFields.Offers(cut, "Group for viewer", "op");

        Assert.That(offered, Is.EqualTo(new[] { "operators" }));
    }

    [Test]
    public void A_binding_to_another_tenants_group_is_refused_before_anything_is_sent()
    {
        UseTenancy("acme");
        Offer(AppsTestData.TaskBoard());
        var cut = AtTenantBindRoles();

        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[0].Input("t/globex/ops");
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[1].Input("operators");

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Group for viewer"), Is.EqualTo(AppBindingGroups.MismatchMessage)));
        Assert.Multiple(() =>
        {
            Assert.That(SuggestionFields.ErrorOf(cut, "Group for editor"), Is.Null);
            Assert.That(Button(cut, "Continue").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void The_clusters_tenant_mismatch_refusal_is_given_its_reason()
    {
        UseTenancy("acme");
        Control.Failures["install"] = new InvalidOperationException(
            "The install of app 'task-board' failed (AppRoleBindingTenantMismatch): t-acme/a/task-board/tasks");
        Offer(AppsTestData.TaskBoard());
        var cut = AtTenantBindRoles();
        BindBoth(cut, "t/acme/ops");
        Button(cut, "Continue").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling]"), Has.Count.EqualTo(1)));

        Button(cut, "Install v1.0.0").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-error").TextContent, Does.Contain(AppsFailureMessages.TenantMismatchSentence).And.Not.Contain("t-acme")));
    }

    [Test]
    public void The_add_me_fix_for_a_tenant_group_opens_that_tenants_group_page()
    {
        UseTenancy("acme");
        SignInAs(Admin, "admins");
        Offer(AppsTestData.TaskBoard());
        var cut = AtTenantBindRoles();

        BindBoth(cut, "t/acme/ops");

        cut.WaitUntil(() => Assert.That(cut.FindAll("a[data-lt-join='t/acme/ops']"), Has.Count.EqualTo(2)));
        Assert.That(cut.Find("a[data-lt-join='t/acme/ops']").GetAttribute("href"), Is.EqualTo("t/acme/access/groups/ops"));
    }

    private IRenderedComponent<AppReviewPage> AtTenantBindRoles()
    {
        var cut = RenderReady(TenantReview);
        Button(cut, "Install...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        return cut;
    }

    private void ClusterGroups(params string[] groups) =>
        Auth.ListGroupsAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AuthGroupPage { Entries = [.. groups.Select(group => new AuthGroup { GroupId = group })] }));

    private static IReadOnlyList<string> OfferedDetails(IRenderedComponent<AppReviewPage> cut) =>
        [.. cut.FindAll("[role=option]").Select(option => option.QuerySelector(".lt-combobox__detail")?.TextContent.Trim() ?? string.Empty)];
}
