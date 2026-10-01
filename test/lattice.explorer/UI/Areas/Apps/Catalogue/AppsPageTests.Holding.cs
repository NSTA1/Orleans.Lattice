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
/// Issue #4150 on "Your apps": an installed app the caller holds no role in says so, with
/// its bound groups and a way to fix it, where an Open entry would otherwise just be
/// missing; and an install this circuit made in another tenant is pointed at.
/// </summary>
public sealed partial class AppsPageTests
{
    [Test]
    public void An_installed_app_the_caller_holds_no_role_in_names_its_groups_and_the_fixes_and_keeps_manage()
    {
        UseTenancy("acme");
        SignInAs("explorer-admin", "admins");
        Control.Install(BoundToOperators(), AppLifecycleState.Enabled);

        var cut = RenderAt<AppsPage>("/t/acme/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding]"), Has.Count.EqualTo(1)));
        var row = cut.Find("section[aria-labelledby=lt-apps-installed] tbody tr.lt-table__row");
        var note = row.QuerySelector("[data-lt-holding]")!;
        Assert.Multiple(() =>
        {
            Assert.That(note.GetAttribute("data-lt-holding"), Is.EqualTo("none"));
            Assert.That(note.TextContent, Does.Contain("You hold no role in it: viewer to operators, editor to operators.").And.Contain("You are not in operators."));
            Assert.That(note.QuerySelector("a[data-lt-join=operators]")!.GetAttribute("href"), Does.EndWith("access/groups/operators"));
            Assert.That(note.QuerySelector("a[data-lt-rebind-fix]")!.GetAttribute("href"), Is.EqualTo("t/acme/apps/catalogue/in-image/task-board%401.0.0"));
            Assert.That(row.QuerySelectorAll("a").Select(link => link.TextContent), Does.Contain("Manage"));
        });
    }

    [Test]
    public void An_installed_app_the_caller_holds_a_role_in_carries_no_notice()
    {
        SignInAs("explorer-admin", "operators");
        Control.Install(BoundToOperators(), AppLifecycleState.Enabled);
        Workspace.Apps.Add(AppsTestData.Mine("task-board"));

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-installed-app]"), Has.Count.EqualTo(1)));
        Assert.That(cut.FindAll("[data-lt-holding]"), Is.Empty);
    }

    [Test]
    public void A_member_of_a_bound_group_whose_access_has_not_caught_up_is_told_so_and_offered_no_join()
    {
        SignInAs("explorer-admin", "operators");
        Control.Install(BoundToOperators(), AppLifecycleState.Enabled);

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=member]"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-holding]").TextContent, Does.Contain("You are in operators"));
            Assert.That(cut.FindAll("a[data-lt-join]"), Is.Empty);
        });
    }

    [Test]
    public void When_membership_cannot_be_read_the_notice_says_unknown_and_offers_no_join()
    {
        Control.Install(BoundToOperators(), AppLifecycleState.Enabled);

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-holding=unknown]"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-holding]").TextContent, Does.Contain("Whether you are in these groups is unknown"));
            Assert.That(cut.FindAll("a[data-lt-join]"), Is.Empty);
        });
    }

    [Test]
    public async Task An_install_this_circuit_made_in_another_tenant_is_pointed_at_from_the_current_one()
    {
        UseTenancy("acme");
        var flow = await InstallThroughAFlowAsync("globex");
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Installed));

        var cut = RenderAt<AppsPage>("/t/acme/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-other-tenant]"), Has.Count.EqualTo(1)));
        var pointer = cut.Find("[data-lt-other-tenant]");
        Assert.Multiple(() =>
        {
            Assert.That(pointer.TextContent, Does.Contain("You installed Task board in tenant globex, not in tenant acme."));
            Assert.That(pointer.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("t/globex/apps/task-board"));
            Assert.That(pointer.QuerySelector("a")!.TextContent, Is.EqualTo("/t/globex/apps/task-board"));
        });
    }

    [Test]
    public async Task An_install_in_the_current_tenant_is_not_pointed_at()
    {
        UseTenancy("acme");
        await InstallThroughAFlowAsync("acme");

        var cut = RenderAt<AppsPage>("/t/acme/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll($"[data-lt-command='{AppsArea.InstallCommandId}']"), Has.Count.EqualTo(1)));
        Assert.That(cut.FindAll("[data-lt-other-tenant]"), Is.Empty);
    }

    private static AppDescriptor BoundToOperators() => AppsTestData.TaskBoard() with
    {
        RoleBindings = [new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "operators" }, new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "operators" }],
    };

    private async Task<AppInstallFlow> InstallThroughAFlowAsync(string tenant)
    {
        var app = AppsTestData.TaskBoard();
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.Descriptions[(AppsTestData.InImage.Key, app.Slug, app.Version)] = app with { SourceKey = AppsTestData.InImage.Key };
        var flow = Services.GetRequiredService<AppInstallFlowStore>()
            .GetOrCreate(new AppInstallFlowKey(tenant, AppsTestData.InImage.Key, app.Slug, null), AppsTestData.InImage);
        await flow.LoadAsync();
        flow.Begin();
        flow.Bind("viewer", "operators");
        flow.Bind("editor", "operators");
        flow.ConfirmBindings();
        await flow.CommitAsync();
        return flow;
    }

    private void SignInAs(string user, params string[] groups)
    {
        ((FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>()).SignIn(user, ExplorerAuthSchemes.Basic);
        Auth.ListSubjectGroupsAsync(user, Arg.Any<CancellationToken>()).Returns(Task.FromResult<IReadOnlyList<string>>(groups));
    }
}
