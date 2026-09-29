using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Apps.Catalogue;

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

    private IRenderedComponent<AppReviewPage> RenderInstalled(AppLifecycleState state, bool adopted = false)
    {
        var app = AppsTestData.TaskBoard(adopted: adopted);
        Offer(app);
        Control.Install(app, state, new AppConsentReport
        {
            Slug = app.Slug,
            Version = app.Version,
            Ceiling = AppConsentDraft.Requested(app).ToCeiling(),
            BridgeGrants = app.Ui!.Bridge,
        });

        var cut = RenderReady(Review);
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle]"), Has.Count.EqualTo(1)));
        return cut;
    }
}
