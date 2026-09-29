using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// Changing the installed version's role bindings (issue #3884) from its review: the install
/// flow's own role-binding step, the recorded and proposed groups side by side, and an Apply
/// behind a confirmation; the Apps area's probe is refreshed once it applies.
/// </summary>
public sealed partial class AppReviewPageTests
{
    [Test]
    public void Change_role_bindings_is_offered_on_the_installed_versions_review()
    {
        var cut = RenderBound(AppLifecycleState.Enabled);

        Assert.That(cut.Find("section[aria-labelledby=lt-apps-rebind]").TextContent, Does.Contain("Which membership group holds each role of Task board"));
        Assert.That(Buttons(cut, "Change role bindings..."), Has.Count.EqualTo(1));
    }

    [Test]
    public void Change_role_bindings_is_not_offered_before_install()
    {
        Offer(AppsTestData.TaskBoard());

        var cut = RenderReady(Review);

        Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind]"), Is.Empty, "a version that is not installed has no bindings to change");
    }

    [Test]
    public void Change_role_bindings_is_not_offered_when_the_head_serves_no_rebinding()
    {
        Services.RemoveAllKeyed<ILatticeAppRoleBindings>(ShellFacades.Key);
        var app = AppsTestData.TaskBoard();
        Offer(app);
        Control.Install(app, AppLifecycleState.Enabled);

        var cut = RenderReady(Review);
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle]"), Has.Count.EqualTo(1)));

        Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind]"), Is.Empty);
    }

    [Test]
    public void Change_role_bindings_is_not_offered_to_a_caller_without_app_install()
    {
        var app = AppsTestData.TaskBoard();
        Offer(app);
        Control.Install(app, AppLifecycleState.Enabled);
        Control.Capabilities = new LatticeAppsCapabilities { CanList = true, CanDescribe = true, CanGetConsent = true };
        Catalog.Capabilities = new LatticeAppCatalogCapabilities { CanListSources = true, CanListAvailable = true, CanDescribeFromSource = true, CanGetIcon = true };

        var cut = RenderReady(Review);
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-lifecycle]"), Has.Count.EqualTo(1)));

        Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind]"), Is.Empty);
    }

    [Test]
    public void Changing_a_binding_shows_old_and_new_asks_first_then_applies_and_refreshes_the_area()
    {
        var cut = RenderBound(AppLifecycleState.Enabled);
        var probes = Workspace.ListCalls;

        Button(cut, "Change role bindings...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("#lt-apps-bind").TextContent, Is.EqualTo("Change role bindings"));
            Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind] input").Select(input => input.GetAttribute("value")), Is.EqualTo(new[] { "g-viewers", "g-editors" }),
                "the step starts from the recorded bindings");
            Assert.That(cut.Find(".lt-apps-steps").GetAttribute("aria-label"), Is.EqualTo("Role binding steps"));
            Assert.That(cut.FindAll(".lt-apps-steps__step").Select(step => step.TextContent.Trim()), Is.EqualTo(new[] { "Review", "Bind roles", "Confirm bindings", "Apply" }));
        });

        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[1].Input("g-new-editors");
        Button(cut, "Continue").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind-confirm]"), Has.Count.EqualTo(1)));
        var viewer = cut.Find("[data-lt-binding=viewer]");
        var editor = cut.Find("[data-lt-binding=editor]");
        Assert.Multiple(() =>
        {
            Assert.That(viewer.GetAttribute("data-lt-changed"), Is.EqualTo("false"));
            Assert.That(viewer.TextContent, Does.Contain("Unchanged."));
            Assert.That(editor.GetAttribute("data-lt-changed"), Is.EqualTo("true"));
            Assert.That(editor.TextContent, Does.Contain("g-editors").And.Contain("g-new-editors").And.Contain("Moves to another group."));
            Assert.That(cut.Find(".lt-apps-steps__step[aria-current]").TextContent.Trim(), Is.EqualTo("Confirm bindings"));
        });

        Button(cut, "Apply bindings...").Click();
        var dialog = cut.Find("[role=dialog]");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.QuerySelector("h2")!.TextContent, Is.EqualTo("Change the role bindings of Task board?"));
            Assert.That(dialog.TextContent, Does.Contain("access rules are replaced at once").And.Contain("lose that role"));
            Assert.That(Control.RoleBindingUpdates, Is.Empty, "nothing is sent before confirmation");
        });

        dialog.QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Apply").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Contain("role bindings of Task board v1.0.0 are changed and its access rules replaced")));
        var update = Control.RoleBindingUpdates.Single();
        Assert.Multiple(() =>
        {
            Assert.That(update.Slug, Is.EqualTo("task-board"));
            Assert.That(update.Version, Is.EqualTo("1.0.0"), "pinned to the installed version");
            Assert.That(update.RoleBindings.Select(binding => (binding.RoleName, binding.GroupId)),
                Is.EqualTo(new[] { ("viewer", "g-viewers"), ("editor", "g-new-editors") }));
            Assert.That(Control.Calls.Select(call => call.Verb), Is.EqualTo(new[] { "rebind" }), "no consent, install or enable");
            Assert.That(Workspace.ListCalls, Is.GreaterThan(probes), "your apps and the app pages re-read after it applies");
            Assert.That(cut.FindAll("a.lt-apps-link").Select(link => link.GetAttribute("href")), Does.Contain("apps/task-board"));
        });

        Button(cut, "Back to the review").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind]"), Has.Count.EqualTo(1)));
        Button(cut, "Change role bindings...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[1].GetAttribute("value"), Is.EqualTo("g-new-editors")));
    }

    [Test]
    public void A_role_may_be_left_unbound_and_a_disabled_app_says_it_stays_disabled()
    {
        var cut = RenderBound(AppLifecycleState.Disabled);

        Button(cut, "Change role bindings...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[0].Input(string.Empty);

        cut.WaitUntil(() => Assert.That(cut.Find("section[aria-labelledby=lt-apps-bind]").TextContent, Does.Contain("Held by nobody: viewer")));
        Assert.That(Button(cut, "Continue").HasAttribute("disabled"), Is.False, "re-binding may leave a role unbound");
        Button(cut, "Continue").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-binding=viewer]").TextContent, Does.Contain("Unbound: nobody holds it any more.")));
        Button(cut, "Apply bindings...").Click();
        var dialog = cut.Find("[role=dialog]");
        Assert.That(dialog.TextContent, Does.Contain("take effect when Task board is enabled").And.Contain("It stays disabled"));
        dialog.QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Apply").Click();

        cut.WaitUntil(() => Assert.That(Control.RoleBindingUpdates, Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(Control.RoleBindingUpdates.Single().RoleBindings.Select(binding => binding.RoleName), Is.EqualTo(new[] { "editor" }));
            Assert.That(cut.Find(".lt-apps-status").TextContent, Does.Not.Contain("access rules replaced"));
        });
    }

    [Test]
    public void Apply_is_disabled_until_something_changes_and_cancel_sends_nothing()
    {
        var cut = RenderBound(AppLifecycleState.Enabled);

        Button(cut, "Change role bindings...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        Button(cut, "Continue").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind-confirm]"), Has.Count.EqualTo(1)));
        Assert.That(Button(cut, "Apply bindings...").HasAttribute("disabled"), Is.True);

        Button(cut, "Back").Click();
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[0].Input("g-other");
        Button(cut, "Continue").Click();
        Button(cut, "Apply bindings...").Click();
        cut.Find("[role=dialog]").QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Cancel").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=dialog]"), Is.Empty);
            Assert.That(Control.RoleBindingUpdates, Is.Empty);
        });
    }

    [Test]
    public void A_refused_rebinding_says_why_and_returns_to_the_confirmation()
    {
        Control.Failures["rebind"] = new InvalidOperationException("Could not change the role bindings of app 'task-board' (ConcurrencyConflict).");
        var cut = RenderBound(AppLifecycleState.Enabled);

        Button(cut, "Change role bindings...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        cut.FindAll("section[aria-labelledby=lt-apps-bind] input")[1].Input("g-new-editors");
        Button(cut, "Continue").Click();
        Button(cut, "Apply bindings...").Click();
        cut.Find("[role=dialog]").QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Apply").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-error").TextContent, Does.Contain("Could not change the role bindings of Task board. It changed while you were working.")));
        Assert.That(cut.Find(".lt-apps-steps__step[aria-current]").TextContent.Trim(), Is.EqualTo("Confirm bindings"));

        Button(cut, "Back").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind-confirm]"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void The_rebinding_flow_fits_a_phone_width()
    {
        var cut = RenderBound(AppLifecycleState.Enabled, LtBreakpoint.Compact);

        Button(cut, "Change role bindings...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-bind]"), Has.Count.EqualTo(1)));
        Button(cut, "Continue").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-binding]"), Has.Count.EqualTo(2)));
        Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind-confirm] table"), Is.Empty, "the comparison is a stacked list, never a wide table");
    }

    private IRenderedComponent<AppReviewPage> RenderBound(AppLifecycleState state, LtBreakpoint breakpoint = LtBreakpoint.Expanded)
    {
        var app = AppsTestData.TaskBoard();
        Offer(app);
        Control.Install(app with
        {
            RoleBindings =
            [
                new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "g-viewers" },
                new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "g-editors" },
            ],
        }, state);

        var cut = RenderReady(Review, breakpoint);
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-rebind]"), Has.Count.EqualTo(1)));
        return cut;
    }
}
