using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>Enable, disable and uninstall, each behind a confirmation stating its consequences.</summary>
public sealed partial class AppReviewPageTests
{
    [Test]
    public void Disable_asks_first_states_its_consequences_and_is_the_disable_command()
    {
        var cut = RenderInstalled(AppLifecycleState.Enabled);
        var area = Services.GetServices<IExplorerArea>().OfType<AppsArea>().Single();
        var command = area.Commands.Single(candidate => candidate.Id == AppsArea.DisableCommandId("task-board"));
        ExplorerCommandControls.AssertVisibleControl(cut, command);

        Button(cut, "Disable").Click();

        var dialog = cut.Find("[role=dialog]");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.QuerySelector("h2")!.TextContent, Is.EqualTo("Disable Task board?"));
            Assert.That(dialog.TextContent, Does.Contain("access rules are withdrawn").And.Contain("trees and data are kept unchanged"));
            Assert.That(Control.Calls, Is.Empty, "nothing happens before confirmation");
        });

        dialog.QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Disable").Click();

        cut.WaitUntil(() => Assert.That(Control.Calls, Is.EqualTo(new[] { ("disable", "task-board") })));
        Assert.That(Services.GetRequiredService<LtToastService>().Toasts.Single().Message, Is.EqualTo("Task board is disabled."));
    }

    [Test]
    public void Cancelling_a_confirmation_changes_nothing()
    {
        var cut = RenderInstalled(AppLifecycleState.Enabled);

        Button(cut, "Disable").Click();
        cut.Find("[role=dialog]").QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Cancel").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=dialog]"), Is.Empty);
            Assert.That(Control.Calls, Is.Empty);
        });
    }

    [Test]
    public void The_disable_command_opens_the_same_confirmation()
    {
        var cut = RenderInstalled(AppLifecycleState.Enabled);
        var area = Services.GetServices<IExplorerArea>().OfType<AppsArea>().Single();

        cut.InvokeAsync(() => area.Commands.Single(command => command.Id == AppsArea.DisableCommandId("task-board")).InvokeAsync!(default).AsTask());

        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog] h2").TextContent, Is.EqualTo("Disable Task board?")));
        Assert.That(Control.Calls, Is.Empty);
    }

    [Test]
    public void Enable_asks_first_and_explains_activation()
    {
        var cut = RenderInstalled(AppLifecycleState.Disabled);

        Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Does.Not.Contain("Disable"));
        Button(cut, "Enable").Click();

        var dialog = cut.Find("[role=dialog]");
        Assert.That(dialog.TextContent, Does.Contain("intersected with the consented ceiling"));
        dialog.QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Enable").Click();

        cut.WaitUntil(() => Assert.That(Control.Calls, Is.EqualTo(new[] { ("enable", "task-board") })));
    }

    [Test]
    public void Uninstall_requires_typing_the_slug_and_states_what_is_soft_deleted_and_what_survives()
    {
        var cut = RenderInstalled(AppLifecycleState.Enabled, adopted: true);

        Button(cut, "Uninstall").Click();

        var dialog = cut.Find("[role=alertdialog]");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.QuerySelector("h2")!.TextContent, Is.EqualTo("Uninstall Task board?"));
            Assert.That(dialog.TextContent, Does.Contain("a/task-board/tasks").And.Contain("recoverable for 7 days"));
            Assert.That(dialog.TextContent, Does.Contain("Adopted trees survive: legacy-archive."));
            Assert.That(dialog.QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Uninstall").HasAttribute("disabled"), Is.True);
        });

        cut.Find("[role=alertdialog] input").Input("task-board");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(Control.Calls, Is.EqualTo(new[] { ("uninstall", "task-board") })));
    }

    [Test]
    public void A_refused_lifecycle_change_is_a_human_error()
    {
        Control.Failures["disable"] = new LatticeAuthorizationDeniedException("denied for subject 7 on t-acme/a/task-board");
        var cut = RenderInstalled(AppLifecycleState.Enabled);

        Button(cut, "Disable").Click();
        cut.Find("[role=dialog]").QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Disable").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("section[aria-labelledby=lt-apps-lifecycle] [role=alert]").TextContent,
            Is.EqualTo("Could not disable Task board. You are not allowed to disable apps here.")));
    }

    [Test]
    public void A_caller_who_may_not_change_the_lifecycle_sees_no_lifecycle_control()
    {
        Control.Capabilities = new LatticeAppsCapabilities { CanInstall = true, CanList = true, CanDescribe = true, CanGetConsent = true };
        var cut = RenderInstalled(AppLifecycleState.Enabled);

        Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle] button"), Is.Empty);
    }

    [Test]
    public void An_uninstall_the_cluster_is_still_applying_is_shown_in_progress_and_followed_to_its_end()
    {
        Control.Pending.Add("uninstall");
        var cut = RenderInstalled(AppLifecycleState.Disabled);
        var toasts = Services.GetRequiredService<LtToastService>();

        Button(cut, "Uninstall").Click();
        cut.Find("[role=alertdialog] input").Input("task-board");
        cut.Find("[role=alertdialog] form").Submit();

        // The status is drawn after the follow armed its first wait, so the advance below cannot overtake it.
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-app-lifecycle-pending=uninstall]").TextContent,
            Does.StartWith("Uninstalling Task board. The cluster accepted the change")));
        Assert.Multiple(() =>
        {
            Assert.That(toasts.Toasts, Is.Empty, "nothing is reported done while the change runs");
            Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle] [role=alert]"), Is.Empty, "a running change is not an error");
            Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle] button"), Is.Empty, "no other change is offered while one runs");
        });

        Control.Settle("task-board", AppLifecycleState.Uninstalled);
        cut.InvokeAsync(() => Time.Advance(AppLifecycleFollow.PollInterval));

        cut.WaitUntil(() => Assert.That(toasts.Toasts.Single().Message, Is.EqualTo("Task board is uninstalled.")));
        Assert.That(cut.FindAll("[data-lt-app-lifecycle-pending]"), Is.Empty);
    }

    [TestCase("Uninstall", AppLifecycleState.Disabled, "uninstall", "Uninstalling...", "Disabled")]
    [TestCase("Disable", AppLifecycleState.Enabled, "disable", "Disabling...", "Enabled")]
    [TestCase("Enable", AppLifecycleState.Disabled, "enable", "Enabling...", "Disabled")]
    public void A_change_this_page_started_badges_the_state_pill_while_it_runs(string button, AppLifecycleState prior, string verb, string pill, string stale)
    {
        // Issue #5482: the pill read the state before the run while the status line said the change was running.
        Control.Pending.Add(verb);
        var cut = RenderInstalled(prior);

        Button(cut, button).Click();
        if (verb == "uninstall")
        {
            cut.Find("[role=alertdialog] input").Input("task-board");
            cut.Find("[role=alertdialog] form").Submit();
        }
        else
        {
            cut.Find("[role=dialog]").QuerySelectorAll("button").Single(candidate => candidate.TextContent.Trim() == button).Click();
        }

        cut.WaitUntil(() => Assert.That(cut.FindAll($"[data-lt-app-lifecycle-pending={verb}]"), Has.Count.EqualTo(1)));
        var pillText = cut.Find("section[aria-labelledby=lt-apps-lifecycle] > p").TextContent.Trim();
        Assert.Multiple(() =>
        {
            Assert.That(pillText, Is.EqualTo(pill), "the pill shows the change running");
            Assert.That(cut.Find("section[aria-labelledby=lt-apps-lifecycle]").TextContent, Does.Not.Contain(stale), "never the state before the run");
        });
    }

    [Test]
    public void A_page_loaded_while_an_uninstall_runs_badges_it_offers_no_actions_and_follows_it_to_its_end()
    {
        // The uninstall was started elsewhere (another tab, or a reload) and is still running.
        var toasts = Services.GetRequiredService<LtToastService>();

        var cut = RenderInstalled(AppLifecycleState.Disabled, inFlight: AppLifecycleState.Uninstalled);

        // The status is drawn once the follow has armed its first wait, so the advance below cannot overtake it.
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-app-lifecycle-pending=uninstall]").TextContent,
            Does.StartWith("Uninstalling Task board. A change started earlier is still being applied")));
        var section = cut.Find("section[aria-labelledby=lt-apps-lifecycle]");
        Assert.Multiple(() =>
        {
            Assert.That(section.TextContent, Does.Contain("Uninstalling..."), "the state pill shows the change, not the stale state");
            Assert.That(section.TextContent, Does.Not.Contain("Disabled"));
            Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle] button"), Is.Empty, "no lifecycle action is offered until it settles");
            Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle] [role=alert]"), Is.Empty);
            Assert.That(Control.Calls, Is.Empty, "following a change starts none");
        });

        Control.Settle("task-board", AppLifecycleState.Uninstalled);
        cut.InvokeAsync(() => Time.Advance(AppLifecycleFollow.PollInterval));

        cut.WaitUntil(() => Assert.That(toasts.Toasts.Single().Message, Is.EqualTo("Task board is uninstalled.")));
        Assert.That(cut.FindAll("[data-lt-app-lifecycle-pending]"), Is.Empty);
    }

    [Test]
    public void A_page_loaded_while_a_run_ends_failed_reports_the_failure_and_offers_the_actions_again()
    {
        var cut = RenderInstalled(AppLifecycleState.Disabled, inFlight: AppLifecycleState.Enabled);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-app-lifecycle-pending=enable]"), Has.Count.EqualTo(1)));

        Control.Settle("task-board", AppLifecycleState.Failed);
        cut.InvokeAsync(() => Time.Advance(AppLifecycleFollow.PollInterval));

        cut.WaitUntil(() => Assert.That(cut.Find("section[aria-labelledby=lt-apps-lifecycle] [role=alert]").TextContent,
            Is.EqualTo("The enable of Task board failed. Its status records why.")));
        Assert.That(cut.FindAll("[data-lt-app-lifecycle-pending]"), Is.Empty);
        Assert.That(Button(cut, "Enable"), Is.Not.Null, "a failed install can be enabled again");
    }

    private IRenderedComponent<AppReviewPage> RenderInstalled(AppLifecycleState state, bool adopted = false, string? conflict = null, AppLifecycleState? inFlight = null)
    {
        var app = AppsTestData.TaskBoard(adopted: adopted);
        if (conflict is not null)
        {
            app = app with { Trees = [.. app.Trees.Select(tree => tree.Name == "tasks" ? tree with { OwnershipConflict = conflict } : tree)] };
        }

        Offer(app);
        Control.Install(app, state, new AppConsentReport
        {
            Slug = app.Slug,
            Version = app.Version,
            Ceiling = AppConsentDraft.Requested(app).ToCeiling(),
            BridgeGrants = app.Ui!.Bridge,
        });
        if (inFlight is { } target)
        {
            Control.InFlight(app.Slug, target);
        }

        var cut = RenderReady(Review);
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle]"), Has.Count.EqualTo(1)));
        return cut;
    }
}
