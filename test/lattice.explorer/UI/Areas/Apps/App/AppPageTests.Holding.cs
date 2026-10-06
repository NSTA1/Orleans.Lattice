using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Transport;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// Issue #4150 on the app page: an <c>AppInstall</c> holder who holds no role in the app is
/// told so, with its bound groups and a visible, explained fix - joining a bound group or
/// re-binding - where the Open entry would otherwise just be missing.
/// </summary>
public sealed partial class AppPageTests
{
    [Test]
    public void An_app_install_holder_without_a_role_is_told_why_with_the_bound_groups_and_the_fixes()
    {
        CallerIn("admins");
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding]"), Has.Count.EqualTo(1)));
        var notice = cut.Find("[data-lt-holding]");
        Assert.Multiple(() =>
        {
            Assert.That(notice.GetAttribute("data-lt-holding"), Is.EqualTo("none"));
            Assert.That(notice.TextContent, Does.Contain("You hold no role in CRM, so you cannot open it."));
            Assert.That(notice.TextContent, Does.Contain("Its roles are bound to groups: viewer to grp-crm-viewers, editor to grp-crm-editors."));
            Assert.That(notice.TextContent, Does.Contain("administrator rights do not grant one"));
            Assert.That(notice.TextContent, Does.Contain("You are not in grp-crm-viewers or grp-crm-editors."));
            Assert.That(notice.QuerySelectorAll("a[data-lt-join]").Select(link => link.TextContent), Is.EqualTo(new[] { "Add me to grp-crm-viewers", "Add me to grp-crm-editors" }));
            Assert.That(notice.QuerySelector("a[data-lt-rebind-fix]")!.GetAttribute("href"), Is.EqualTo("apps/catalogue/in-image/crm"));
            Assert.That(notice.QuerySelectorAll("button").Select(button => button.TextContent.Trim()), Is.EqualTo(new[] { "Check again" }));
            Assert.That(cut.FindAll(".lt-app-actions a").Select(link => link.TextContent), Has.None.StartsWith("Open"));
        });
    }

    [Test]
    public void Checking_again_after_joining_a_bound_group_shows_the_open_entry()
    {
        var auth = CallerIn("admins");
        Control.Administer(Admin(), CoveringConsent());
        var cut = RenderAt("apps/crm");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=none]"), Has.Count.EqualTo(1)));

        auth.ListSubjectGroupsAsync("explorer-admin", Arg.Any<CancellationToken>()).Returns(Task.FromResult<IReadOnlyList<string>>(["grp-crm-viewers"]));
        Workspace.Grant(Workspace());
        cut.FindAll("[data-lt-holding] button").Single(button => button.TextContent.Trim() == "Check again").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-app-actions a").Select(link => link.TextContent), Does.Contain("Open CRM (opens in a new window)")));
        Assert.That(cut.FindAll("[data-lt-holding]"), Is.Empty);
    }

    [Test]
    public void A_role_holder_sees_no_holding_notice()
    {
        CallerIn("grp-crm-viewers");
        Workspace.Grant(Workspace());
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-app-actions a").Select(link => link.TextContent), Does.Contain("Open CRM (opens in a new window)")));
        Assert.That(cut.FindAll("[data-lt-holding]"), Is.Empty);
    }

    [Test]
    public void Without_readable_membership_the_notice_says_unknown_and_offers_no_join_but_still_the_rebind()
    {
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=unknown]"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-holding]").TextContent, Does.Contain("Whether you are in these groups is unknown: your group membership could not be read."));
            Assert.That(cut.FindAll("a[data-lt-join]"), Is.Empty);
            Assert.That(cut.FindAll("a[data-lt-rebind-fix]"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void An_app_that_is_not_enabled_says_nobody_holds_its_roles_yet()
    {
        CallerIn("grp-crm-editors");
        Control.Administer(Admin(state: AppLifecycleState.Installed), CoveringConsent());

        var cut = RenderAt("apps/crm");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=member]"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find("[data-lt-holding]").TextContent, Does.Contain("nobody holds its roles until it is enabled").And.Contain("You are in grp-crm-editors, so you will hold editor once it is enabled."));
    }

    [Test]
    public void The_open_section_of_an_app_the_caller_holds_no_role_in_explains_why_and_how_to_fix_it()
    {
        CallerIn("admins");
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm/window");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=none]"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM is not open to you yet"));
            Assert.That(cut.FindAll("a[data-lt-join]"), Has.Count.EqualTo(2));
        });
    }

    private ILatticeAuthAdmin CallerIn(params string[] groups)
    {
        var auth = Substitute.For<ILatticeAuthAdmin>();
        auth.ListSubjectGroupsAsync("explorer-admin", Arg.Any<CancellationToken>()).Returns(Task.FromResult<IReadOnlyList<string>>(groups));
        Services.AddKeyedSingleton(ShellFacades.Key, auth);
        ((FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>()).SignIn("explorer-admin", ExplorerAuthSchemes.Basic);
        return auth;
    }
}
