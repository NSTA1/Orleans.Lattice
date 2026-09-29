using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Membership;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>The staged install, upgrade and re-consent, driven through the page's own controls.</summary>
public sealed partial class AppReviewPageTests
{
    [Test]
    public void Install_is_review_bind_roles_confirm_ceiling_install_then_enable_now()
    {
        Offer(AppsTestData.TaskBoard());
        var cut = RenderReady(Review);
        var probes = Workspace.ListCalls;

        Button(cut, "Install...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        Assert.That(Button(cut, "Continue").HasAttribute("disabled"), Is.True, "every role must be bound first");
        Assert.That(cut.Find(".lt-apps-steps__step[aria-current]").TextContent.Trim(), Is.EqualTo("Bind roles"));

        var inputs = cut.FindAll("section[aria-labelledby=lt-apps-bind] input");
        inputs[0].Input("readers");
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[1].Input("writers");
        Button(cut, "Continue").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling]"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-activation]").GetAttribute("data-lt-activation"), Is.EqualTo("ok"));
            Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling] input[type=checkbox]:checked"), Has.Count.EqualTo(4 + 3), "four operations and three bridge grants, exactly as requested");
        });

        Button(cut, "Install v1.0.0").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("installed and not enabled yet")));
        Assert.That(Workspace.ListCalls, Is.GreaterThan(probes), "a completed install refreshes the area's probe");

        Button(cut, "Enable now").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("is enabled")));
        Assert.Multiple(() =>
        {
            Assert.That(Control.Calls.Select(call => call.Verb), Is.EqualTo(new[] { "install", "enable" }));
            Assert.That(Control.Installs.Single().RoleBindings.Select(binding => binding.GroupId), Is.EquivalentTo(new[] { "readers", "writers" }));
            Assert.That(cut.FindAll("a.lt-apps-link").Select(link => link.GetAttribute("href")), Does.Contain("apps/task-board"));
        });
    }

    [Test]
    public void Group_search_is_read_only_and_falls_back_to_listing_groups()
    {
        Offer(AppsTestData.TaskBoard());
        Auth.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>())
            .Returns(new DirectorySearchResult { Available = true, Principals = [new DirectoryPrincipalDescriptor { Id = "grp-readers", DisplayName = "Readers", Kind = DirectoryPrincipalKind.Group }] });
        var cut = AtBindRoles();

        Buttons(cut, "Find groups")[0].Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-apps-results button"), Has.Count.EqualTo(1)));
        cut.Find(".lt-apps-results button").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[0].GetAttribute("value"), Is.EqualTo("grp-readers")));
        Auth.Received().SearchDirectoryAsync(Arg.Is<DirectorySearchRequest>(request => request.Kind == DirectoryPrincipalKind.Group), Arg.Any<CancellationToken>());

        Auth.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>()).Returns(DirectorySearchResult.Unavailable);
        Auth.ListGroupsAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new AuthGroupPage { Entries = [new AuthGroup { GroupId = "writers", DisplayName = "Writers" }, new AuthGroup { GroupId = "ops" }] });
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[1].Input("writ");
        Buttons(cut, "Find groups")[1].Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-apps-results button").Select(button => button.TextContent), Has.Some.Contains("Writers")));

        Auth.SearchDirectoryAsync(Arg.Any<DirectorySearchRequest>(), Arg.Any<CancellationToken>()).Returns(Task.FromException<DirectorySearchResult>(new TimeoutException()));
        Buttons(cut, "Find groups")[0].Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-binding [role=status]").TextContent, Is.EqualTo("Group search is not available. Type the group id.")));
    }

    [Test]
    public void Editing_the_ceiling_shows_what_would_fail_activation()
    {
        Offer(AppsTestData.TaskBoard(crossApp: true));
        var cut = AtCeiling();

        Checkbox(cut, "delete").Change(false);
        Checkbox(cut, "see your display name").Change(false);

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-activation]").GetAttribute("data-lt-activation"), Is.EqualTo("fails")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[data-lt-issue]").Select(issue => issue.GetAttribute("data-lt-issue")),
                Is.EqualTo(new[] { nameof(AppActivationIssueKind.CeilingExceeded), nameof(AppActivationIssueKind.BridgeConsentRequired) }));
            Assert.That(cut.Find("[data-lt-issue=CeilingExceeded]").TextContent, Does.Contain("The role editor asks to delete"));
            Assert.That(cut.Find("legend").TextContent, Is.EqualTo("Operations"));
            Assert.That(cut.FindAll("legend").Select(legend => legend.TextContent), Does.Contain("Exception scopes outside a/task-board/"));
        });

        Button(cut, "Install v1.0.0").Click();

        cut.WaitUntil(() => Assert.That(Control.ConsentUpdates, Has.Count.EqualTo(1)));
        Assert.That(Control.Installs.Single().Ceiling.AllowedOperations.HasFlag(LatticeOperation.Delete), Is.False);
    }

    [Test]
    public void A_tree_that_cannot_be_owned_blocks_install()
    {
        Offer(AppsTestData.TaskBoard() with { Trees = [new AppTreeDescriptor { Name = "tasks", OwnershipConflict = "owned by another app" }] });

        var cut = RenderReady(Review);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("button").Select(button => button.TextContent), Has.None.EqualTo("Install..."));
            Assert.That(cut.Find(".lt-apps-flag").TextContent, Is.EqualTo("Cannot be owned: install is refused"));
            Assert.That(cut.FindAll(".lt-apps-hint").Select(hint => hint.TextContent), Has.Some.Contains("cannot be installed here"));
        });
    }

    [Test]
    public void A_refused_install_shows_a_human_error_and_goes_back_to_the_ceiling()
    {
        Offer(AppsTestData.TaskBoard());
        Control.Failures["install"] = new InvalidOperationException("Could not install app 'task-board' (TreeOwnershipConflict): t-acme/a/task-board/tasks");
        var cut = AtCeiling();

        Button(cut, "Install v1.0.0").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-apps-error[role=alert]"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-apps-error").TextContent, Does.Contain("Could not install Task board. One of its trees is already owned").And.Not.Contain("t-acme"));
            Assert.That(cut.Find(".lt-apps-steps__step[aria-current]").TextContent.Trim(), Is.EqualTo("Confirm ceiling"));
        });

        Button(cut, "Back").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling]"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_dynamic_source_renders_acquiring_then_verifying_before_the_review()
    {
        var app = AppsTestData.TaskBoard(source: "nuget-contoso", withIcon: true);
        Offer(app, AppsTestData.Feed);
        Catalog.Icons[("nuget-contoso", "task-board")] = AppsTestData.Icon;
        Catalog.DescribeGate = new TaskCompletionSource();
        Catalog.IconGate = new TaskCompletionSource();

        var cut = RenderAt<AppReviewPage>("/apps/catalogue/nuget-contoso/task-board");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.StartWith("Acquiring task-board from Contoso feed")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-apps-steps__step").Select(step => step.TextContent.Trim()), Is.EqualTo(new[] { "Acquire", "Verify", "Review", "Bind roles", "Confirm ceiling", "Install", "Enable" }));
            Assert.That(cut.Find(".lt-apps-steps__step[aria-current]").TextContent.Trim(), Is.EqualTo("Acquire"));
        });

        cut.InvokeAsync(() => Catalog.DescribeGate.SetResult());
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.StartWith("Verifying task-board")));
        Assert.That(cut.Find(".lt-apps-steps__step[aria-current]").TextContent.Trim(), Is.EqualTo("Verify"));

        cut.InvokeAsync(() => Catalog.IconGate.SetResult());
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-identity]"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find("section[aria-labelledby=lt-apps-identity]").TextContent, Does.Contain("Verified: acquired from its source and checked against its manifest digests"));
    }

    [Test]
    public void A_failed_verification_is_an_error_that_can_be_retried()
    {
        var app = AppsTestData.TaskBoard(source: "nuget-contoso", withIcon: true);
        Offer(app, AppsTestData.Feed);

        var cut = RenderAt<AppReviewPage>("/apps/catalogue/nuget-contoso/task-board");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-error").TextContent, Does.Contain("Its icon does not match its manifest digest.")));
        Catalog.Icons[("nuget-contoso", "task-board")] = AppsTestData.Icon;
        Button(cut, "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-identity]"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void The_page_is_a_resumable_status_page_for_an_install_in_flight()
    {
        Offer(AppsTestData.TaskBoard());
        var first = AtCeiling();
        Control.InstallGate = new TaskCompletionSource();
        Button(first, "Install v1.0.0").Click();
        first.WaitUntil(() => Assert.That(first.Find(".lt-apps-status").TextContent, Does.StartWith("Installing Task board")));

        var again = RenderAt<AppReviewPage>(Review);

        again.WaitUntil(() => Assert.That(again.Find(".lt-apps-status").TextContent, Does.StartWith("Installing Task board")));
        Assert.That(again.Find(".lt-apps-status").TextContent, Does.Contain("You can leave this page"));

        again.InvokeAsync(() => Control.InstallGate.SetResult());

        again.WaitUntil(() => Assert.That(again.Find(".lt-apps-status").TextContent, Does.Contain("installed and not enabled yet")));
        Assert.That(Catalog.Describes, Has.Count.EqualTo(1), "coming back resumes the flow rather than starting again");
    }

    [Test]
    public void Consent_drift_is_shown_prominently_with_the_reconsent_action()
    {
        var installed = AppsTestData.TaskBoard();
        Offer(installed);
        Control.Install(installed, AppLifecycleState.Failed, new AppConsentReport
        {
            Slug = installed.Slug,
            Version = installed.Version,
            Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = LatticeOperation.Read | LatticeOperation.RangeRead },
            BridgeGrants = [],
        });

        var cut = RenderReady(Review);

        var drift = cut.Find("[data-lt-drift]");
        Assert.Multiple(() =>
        {
            Assert.That(drift.QuerySelector(".lt-pill")!.GetAttribute("data-lt-state"), Is.EqualTo("drift"));
            Assert.That(drift.TextContent, Does.Contain("The role editor asks to write and delete").And.Contain("Its UI asks to read its own trees"));
            Assert.That(cut.FindAll(".lt-apps-steps__step").Select(step => step.TextContent.Trim()), Does.Not.Contain("Bind roles"));
        });

        Button(cut, "Re-consent").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling]"), Has.Count.EqualTo(1)));
        Button(cut, "Record consent").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("re-consented and not enabled yet")));
        Button(cut, "Enable now").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("is enabled")));
        Assert.Multiple(() =>
        {
            Assert.That(Control.Installs, Is.Empty);
            Assert.That(Control.ConsentUpdates.Single().BridgeGrants!.Value, Has.Length.EqualTo(3));
            Assert.That(Control.Calls.Select(call => call.Verb), Is.EqualTo(new[] { "consent", "enable" }));
        });
    }

    [Test]
    public void An_upgrade_shows_its_diff_flags_reconsent_and_its_control_is_the_upgrade_command()
    {
        var installed = AppsTestData.TaskBoard("1.0.0");
        Control.Install(installed, AppLifecycleState.Enabled);
        var next = AppsTestData.TaskBoard("2.0.0", bridge: [new AppUiBridgeGrantDescriptor { Operation = "data.read" }, new AppUiBridgeGrantDescriptor { Operation = "ui.notify" }]);
        Offer(next);
        Catalog.Offers.Clear();
        Catalog.Offers.Add(AppsTestData.Offer(next, "in-image", "1.0.0", AppLifecycleState.Enabled));

        var cut = RenderReady("/apps/catalogue/in-image/task-board%402.0.0");

        var diff = cut.Find("section[aria-labelledby=lt-apps-diff]");
        Assert.Multiple(() =>
        {
            Assert.That(diff.QuerySelector("h2")!.TextContent, Is.EqualTo("What changes from v1.0.0 to v2.0.0"));
            Assert.That(diff.TextContent, Does.Contain("Its UI newly asks to show you short notifications").And.Contain("needs re-consent"));
            Assert.That(diff.TextContent, Does.Contain("Its UI no longer asks to write its tree tasks"));
            Assert.That(diff.QuerySelector("[data-lt-reconsent]"), Is.Not.Null);
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Is.EqualTo("v1.0.0 is installed; In-image apps offers v2.0.0."));
            Assert.That(cut.FindAll(".lt-apps-steps__step").Select(step => step.TextContent.Trim()), Does.Contain("Upgrade"));
        });

        var area = Services.GetServices<IExplorerArea>().OfType<AppsArea>().Single();
        cut.WaitUntil(() => Assert.That(area.Commands.Select(command => command.Id), Does.Contain(AppsArea.UpgradeCommandId("task-board"))));
        ExplorerCommandControls.AssertVisibleControl(cut, area.Commands.Single(command => command.Id == AppsArea.UpgradeCommandId("task-board")));
    }

    [Test]
    public void The_upgrade_command_starts_the_upgrade_flow_on_its_page()
    {
        var installed = AppsTestData.TaskBoard("1.0.0") with
        {
            RoleBindings = [new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "readers" }, new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "writers" }],
        };
        Control.Install(installed, AppLifecycleState.Enabled);
        var next = AppsTestData.TaskBoard("2.0.0");
        Offer(next);
        Catalog.Offers.Clear();
        Catalog.Offers.Add(AppsTestData.Offer(next, "in-image", "1.0.0", AppLifecycleState.Enabled));
        var cut = RenderReady("/apps/catalogue/in-image/task-board%402.0.0");
        var area = Services.GetServices<IExplorerArea>().OfType<AppsArea>().Single();
        cut.WaitUntil(() => Assert.That(area.Commands.Select(command => command.Id), Does.Contain(AppsArea.UpgradeCommandId("task-board"))));

        cut.InvokeAsync(() => area.Commands.Single(command => command.Id == AppsArea.UpgradeCommandId("task-board")).InvokeAsync!(default).AsTask());

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        Button(cut, "Continue").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling]"), Has.Count.EqualTo(1)));
        Button(cut, "Upgrade to v2.0.0").Click();

        cut.WaitUntil(() => Assert.That(Control.Installs.Single().Version, Is.EqualTo("2.0.0")));
    }

    private IRenderedComponent<AppReviewPage> AtBindRoles()
    {
        var cut = RenderReady(Review);
        Button(cut, "Install...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        return cut;
    }

    private IRenderedComponent<AppReviewPage> AtCeiling()
    {
        var cut = AtBindRoles();
        foreach (var input in cut.FindAll("section[aria-labelledby=lt-apps-bind] input").ToArray().Select((_, index) => index))
        {
            cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[input].Input("group-" + input);
        }

        Button(cut, "Continue").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-ceiling]"), Has.Count.EqualTo(1)));
        return cut;
    }

    private static AngleSharp.Dom.IElement Button<T>(IRenderedComponent<T> cut, string text)
        where T : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    private static IReadOnlyList<AngleSharp.Dom.IElement> Buttons<T>(IRenderedComponent<T> cut, string text)
        where T : Microsoft.AspNetCore.Components.IComponent =>
        [.. cut.FindAll("button").Where(button => button.TextContent.Trim() == text)];

    private static AngleSharp.Dom.IElement Checkbox<T>(IRenderedComponent<T> cut, string label)
        where T : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll("label").Single(element => element.TextContent.Trim().StartsWith(label, StringComparison.Ordinal)).QuerySelector("input[type=checkbox]")
            ?? cut.Find($"#{cut.FindAll("label").Single(element => element.TextContent.Trim().StartsWith(label, StringComparison.Ordinal)).GetAttribute("for")}");
}
