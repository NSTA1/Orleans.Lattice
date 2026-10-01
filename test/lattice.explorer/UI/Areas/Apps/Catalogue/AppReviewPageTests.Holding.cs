using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// Issue #4150: since an app role is held only by binding (issue #3902), the installer is
/// told at bind time, and again on the install's confirmation, whether they will hold a
/// role - from their own group membership, read under their own credential, never guessed.
/// </summary>
public sealed partial class AppReviewPageTests
{
    private const string Admin = "explorer-admin";

    [Test]
    public void Binding_roles_to_a_group_the_caller_is_not_in_warns_and_offers_the_fixes_without_blocking()
    {
        SignInAs(Admin, "admins");
        Offer(AppsTestData.TaskBoard());
        var cut = AtBindRoles();

        BindBoth(cut, "operators");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=not-member]"), Has.Count.EqualTo(2)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-holding=not-member]").TextContent, Does.Contain("You are not in operators, so you will not hold the viewer role."));
            Assert.That(cut.Find("[data-lt-holding-summary]").TextContent, Does.Contain("You will hold no role in Task board, so you won't be able to open it."));
            Assert.That(cut.FindAll("a[data-lt-join=operators]").Select(link => link.TextContent).Distinct(), Is.EqualTo(new[] { "Add me to operators" }));
            Assert.That(cut.Find("a[data-lt-join=operators]").GetAttribute("href"), Is.EqualTo("access/groups/operators"));
            Assert.That(cut.Find("section[aria-labelledby=lt-apps-bind]").TextContent, Does.Contain("or bind it to a group you are in"));
            Assert.That(Button(cut, "Continue").HasAttribute("disabled"), Is.False, "the warning is advisory and never blocks");
        });
        Auth.Received().ListSubjectGroupsAsync(Admin, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Binding_a_role_to_a_group_the_caller_is_in_says_so_and_warns_of_nothing()
    {
        SignInAs(Admin, "operators");
        Offer(AppsTestData.TaskBoard());
        var cut = AtBindRoles();

        BindBoth(cut, "operators");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=member]"), Has.Count.EqualTo(2)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-holding=member]").TextContent, Does.Contain("You are in operators, the group viewer is bound to."));
            Assert.That(cut.FindAll("[data-lt-holding-summary]"), Is.Empty);
            Assert.That(cut.FindAll("a[data-lt-join]"), Is.Empty);
        });
    }

    [Test]
    public void A_caller_who_cannot_read_membership_is_told_it_is_unknown_and_offered_no_join()
    {
        SignInAs(Admin);
        Auth.ListSubjectGroupsAsync(Admin, Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<string>>(new LatticeAuthorizationDeniedException("not an administrator")));
        Offer(AppsTestData.TaskBoard());
        var cut = AtBindRoles();

        BindBoth(cut, "operators");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=unknown]"), Has.Count.EqualTo(2)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-holding=unknown]").TextContent, Does.Contain("Whether you are in operators is unknown: your group membership could not be read."));
            Assert.That(cut.FindAll("a[data-lt-join]"), Is.Empty);
            Assert.That(cut.FindAll("[data-lt-holding-summary]"), Is.Empty, "nothing is guessed");
        });
    }

    [Test]
    public void A_token_sign_in_names_no_subject_so_membership_is_unknown_rather_than_read_for_a_display_name()
    {
        SignInWith(ExplorerAuthSchemes.Entra, "Someone Else", "operators");
        Offer(AppsTestData.TaskBoard());
        var cut = AtBindRoles();

        BindBoth(cut, "operators");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=unknown]"), Has.Count.EqualTo(2)));
        Auth.DidNotReceive().ListSubjectGroupsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Changing_role_bindings_warns_when_the_caller_is_in_none_of_the_new_groups()
    {
        SignInAs(Admin, "admins");
        var installed = AppsTestData.TaskBoard() with
        {
            RoleBindings = [new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "admins" }, new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "admins" }],
        };
        Offer(installed);
        Control.Install(installed, AppLifecycleState.Enabled);
        var cut = RenderReady(Review);
        Button(cut, "Change role bindings...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=member]"), Has.Count.EqualTo(2)));

        BindBoth(cut, "operators");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=not-member]"), Has.Count.EqualTo(2)));
        Assert.That(cut.Find("[data-lt-holding-summary]").TextContent, Does.Contain("You will hold no role in Task board"));
    }

    [Test]
    public void The_install_confirmation_names_the_tenant_links_the_app_there_and_says_the_caller_cannot_open_it()
    {
        UseTenancy("globex");
        SignInAs(Admin, "admins");
        Offer(AppsTestData.TaskBoard());
        var cut = RenderReady("/t/globex/apps/catalogue/in-image/task-board");
        Button(cut, "Install...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        BindBoth(cut, "operators");
        Button(cut, "Continue").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling]"), Has.Count.EqualTo(1)));

        Button(cut, "Install v1.0.0").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("is installed in tenant globex and not enabled yet")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-app-address]").TextContent, Is.EqualTo("/t/globex/apps/task-board"));
            Assert.That(cut.Find("a[data-lt-app-link]").GetAttribute("href"), Is.EqualTo("t/globex/apps/task-board"));
            Assert.That(cut.Find(".lt-apps-status [data-lt-holding]").GetAttribute("data-lt-holding"), Is.EqualTo("none"));
            Assert.That(cut.Find(".lt-apps-status [data-lt-holding]").TextContent, Does.Contain("You hold no role in Task board").And.Contain("You are not in operators."));
            Assert.That(cut.Find(".lt-apps-status a[data-lt-join=operators]").TextContent, Is.EqualTo("Add me to operators"));
        });

        Button(cut, "Enable now").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("is enabled in tenant globex")));
        Assert.That(cut.Find(".lt-apps-status [data-lt-holding]").TextContent, Does.Contain("so you cannot open it"));
    }

    [Test]
    public void The_install_confirmation_says_a_member_of_a_bound_group_can_open_it()
    {
        SignInAs(Admin, "readers");
        Offer(AppsTestData.TaskBoard());
        var cut = AtBindRoles();
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[0].Input("readers");
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[1].Input("writers");
        Button(cut, "Continue").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling]"), Has.Count.EqualTo(1)));

        Button(cut, "Install v1.0.0").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("installed and not enabled yet")));
        Button(cut, "Enable now").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("is enabled")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-can-open]").GetAttribute("data-lt-can-open"), Is.EqualTo("yes"));
            Assert.That(cut.Find("[data-lt-can-open]").TextContent, Is.EqualTo("You can open it: you are in readers, the group its viewer role is bound to."));
            Assert.That(cut.FindAll(".lt-apps-status [data-lt-holding]"), Is.Empty);
            Assert.That(cut.Find("[data-lt-app-address]").TextContent, Is.EqualTo("/apps/task-board"));
        });
    }

    [Test]
    public void The_install_confirmation_says_whether_the_caller_can_open_it_is_unknown_when_membership_cannot_be_read()
    {
        Offer(AppsTestData.TaskBoard());
        var cut = AtCeiling();

        Button(cut, "Install v1.0.0").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("installed and not enabled yet")));
        Assert.That(cut.Find(".lt-apps-status [data-lt-holding]").GetAttribute("data-lt-holding"), Is.EqualTo("unknown"));
    }

    private void SignInAs(string user, params string[] groups) => SignInWith(ExplorerAuthSchemes.Basic, user, groups);

    private void SignInWith(string scheme, string user, params string[] groups)
    {
        ((FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>()).SignIn(user, scheme);
        Auth.ListSubjectGroupsAsync(user, Arg.Any<CancellationToken>()).Returns(Task.FromResult<IReadOnlyList<string>>(groups));
    }

    private static void BindBoth(IRenderedComponent<AppReviewPage> cut, string group)
    {
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[0].Input(group);
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[1].Input(group);
    }
}
