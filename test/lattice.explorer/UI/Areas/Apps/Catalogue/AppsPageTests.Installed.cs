using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// An <c>AppInstall</c> holder's view of the apps installed in the tenant (issue #3984, finding 2):
/// "Your apps" lists only apps the caller holds a role in, so an operator who holds none still
/// sees what is installed, with Manage, Disable and Uninstall, and the spine badge counts it.
/// </summary>
public sealed partial class AppsPageTests
{
    [Test]
    public void An_app_install_holder_sees_the_apps_installed_in_the_tenant_with_manage_disable_and_uninstall()
    {
        UseTenancy("acme");
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);

        var cut = RenderAt<AppsPage>("/t/acme/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-installed-app]"), Has.Count.EqualTo(1)));
        var section = cut.Find("section[aria-labelledby=lt-apps-installed]");
        var row = section.QuerySelector("tbody tr.lt-table__row")!;
        Assert.Multiple(() =>
        {
            Assert.That(section.QuerySelector("h2")!.TextContent, Is.EqualTo("Installed in tenant acme"));
            Assert.That(row.QuerySelector("[data-lt-installed-app]")!.GetAttribute("data-lt-installed-app"), Is.EqualTo("task-board"));
            Assert.That(row.TextContent, Does.Contain("1.0.0").And.Contain("Enabled"));
            Assert.That(row.QuerySelector("a")!.TextContent, Is.EqualTo("Manage"));
            Assert.That(row.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("t/acme/apps/catalogue/in-image/task-board%401.0.0"));
            Assert.That(row.QuerySelectorAll("button").Select(button => button.TextContent.Trim()), Is.EqualTo(new[] { "Disable", "Uninstall" }));
            Assert.That(cut.FindAll(".lt-empty h2").Select(heading => heading.TextContent), Does.Not.Contain("No apps yet"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("installed in tenant acme"));
        });
    }

    [Test]
    public void The_installed_apps_are_named_from_the_catalogue_when_it_offers_them()
    {
        var app = AppsTestData.TaskBoard();
        Control.Install(app, AppLifecycleState.Enabled);
        Catalog.Offers.Add(AppsTestData.Offer(app, "in-image"));

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-installed-app]").TextContent, Is.EqualTo("Task board")));
        Assert.That(cut.Find("section[aria-labelledby=lt-apps-installed] h2").TextContent, Is.EqualTo("Installed apps"));
    }

    [TestCase("Disable", "Disable")]
    [TestCase("Uninstall", "Uninstall")]
    public void A_lifecycle_action_opens_the_manage_page_with_its_confirmation_waiting(string button, string verb)
    {
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);
        var cut = RenderAt<AppsPage>("/apps");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-installed-app]"), Has.Count.EqualTo(1)));

        cut.Find("section[aria-labelledby=lt-apps-installed]").QuerySelectorAll("tbody button").Single(candidate => candidate.TextContent.Trim() == button).Click();

        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Does.EndWith("apps/catalogue/in-image/task-board%401.0.0"));
            Assert.That(Services.GetRequiredService<AppsLifecycleIntents>().TryTake("task-board", Enum.Parse<AppLifecycleVerb>(verb)), Is.True);
            Assert.That(Control.Calls, Is.Empty, "nothing changes before the confirmation on the manage page");
        });
    }

    [Test]
    public void A_disabled_app_offers_no_disable_and_a_caller_who_may_not_uninstall_sees_no_uninstall()
    {
        Control.Capabilities = Control.Capabilities with { CanUninstall = false };
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Disabled);

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-installed-app]"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find("section[aria-labelledby=lt-apps-installed]").QuerySelectorAll("tbody button"), Is.Empty);
    }

    [Test]
    public async Task The_spine_badge_counts_the_installed_apps_the_page_lists_and_never_an_uninstalled_one()
    {
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);
        Control.Install(AppsTestData.TaskBoard() with { Slug = "notes" }, AppLifecycleState.Uninstalled);
        var area = Services.GetServices<IExplorerArea>().OfType<AppsArea>().Single();

        var cut = RenderAt<AppsPage>("/apps");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-installed-app]"), Has.Count.EqualTo(1)));
        var badge = await area.GetDirectoryBadgeAsync(CancellationToken.None);
        var status = await area.GetHomeStatusAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(badge, Is.EqualTo("1"));
            Assert.That(status, Does.StartWith("1 app installed"));
        });
    }

    [Test]
    public void A_restricted_identity_sees_no_installed_section()
    {
        Restrict();
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-empty"), Has.Count.EqualTo(1)));
        Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-installed]"), Is.Empty);
    }

    [Test]
    public void An_operator_in_a_tenant_with_nothing_installed_is_told_so()
    {
        UseTenancy("acme");

        var cut = RenderAt<AppsPage>("/t/acme/apps");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain("No app is installed in tenant acme yet")));
    }
}
