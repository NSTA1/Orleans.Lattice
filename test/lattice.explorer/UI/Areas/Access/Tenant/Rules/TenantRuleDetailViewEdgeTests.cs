using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The tenant rule page's edges (<c>/t/acme/access/rules/{localId}</c>): a head
/// with no tenant policy, a refused or empty read, reads answered after the view
/// moved to another rule or was left, a read-only platform rule and the Explain
/// link of each subject shape, the editor's context reads failing, and every
/// refusal and late answer the delete can meet.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantRuleDetailViewEdgeTests : TenantRulesTestContext
{
    private ILatticeTenantPolicyAdmin _policy = null!;
    private int _notFound;

    private IReadOnlyList<string> ToastMessages =>
        [.. Services.GetRequiredService<LtToastService>().Toasts.Select(toast => toast.Message)];

    /// <summary>Puts a scripted tenant policy in front of the page.</summary>
    [SetUp]
    public void UseScriptedPolicy()
    {
        _policy = Substitute.For<ILatticeTenantPolicyAdmin>();
        TenantFacades.PolicyOverride = _policy;
    }

    [Test]
    public void At_the_compact_breakpoint_the_editor_opens_as_an_end_sheet()
    {
        Serve(Rule("r"));
        var cut = RenderView("r", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Edit rule"), Is.Not.Null));

        AccessForms.Button(cut, "Edit rule").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog--end form[data-lt-tenant-rule-editor]"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void Cancelling_the_editor_closes_it()
    {
        Serve(Rule("r"));
        var cut = RenderView("r");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Edit rule"), Is.Not.Null));
        AccessForms.Button(cut, "Edit rule").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form[data-lt-tenant-rule-editor]"), Has.Count.EqualTo(1)));

        cut.Find("form[data-lt-tenant-rule-editor]").QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Cancel").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("form[data-lt-tenant-rule-editor]"), Is.Empty));
    }

    [Test]
    public void Dismissing_the_delete_confirmation_deletes_nothing()
    {
        Serve(Rule("r"));
        var cut = RenderView("r");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Delete rule"), Is.Not.Null));
        AccessForms.Button(cut, "Delete rule").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-confirm"), Has.Count.EqualTo(1)));

        cut.Find(".lt-confirm").QuerySelectorAll("button").Single(button => button.TextContent.Trim() == "Cancel").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-confirm"), Is.Empty));
        _policy.DidNotReceiveWithAnyArgs().RemoveRuleAsync(default!, default!, default);
    }

    [Test]
    public void Without_a_tenant_policy_the_rule_could_not_be_read()
    {
        TenantFacades.ServesPolicy = false;

        var cut = RenderView("r");

        cut.WaitUntil(() =>
        {
            var view = cut.Find("[data-lt-tenant-view=rule]");
            Assert.That(view.TextContent, Does.Contain("The rule could not be read"));
            Assert.That(view.TextContent, Does.Not.Contain("Not permitted"));
        });
        Assert.That(_notFound, Is.Zero);
    }

    [Test]
    public void A_denied_read_is_titled_not_permitted_and_try_again_reads_again()
    {
        _policy.GetRuleAsync(Acme, "r", Arg.Any<CancellationToken>()).Returns(
            Task.FromException<TenantRuleView?>(new LatticeAuthorizationDeniedException("no")),
            Task.FromResult<TenantRuleView?>(Rule("r")));
        var cut = RenderView("r");
        cut.WaitUntil(() =>
        {
            var view = cut.Find("[data-lt-tenant-view=rule]");
            Assert.That(view.TextContent, Does.Contain("Not permitted"));
            Assert.That(view.TextContent, Does.Contain(TenantAccessFailure.DeniedMessage(Acme)));
        });

        AccessForms.Button(cut, "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-rule-layer=tenant]"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_rule_the_tenant_does_not_have_is_not_found()
    {
        _policy.GetRuleAsync(Acme, "r", Arg.Any<CancellationToken>()).Returns(Task.FromResult<TenantRuleView?>(null));

        var cut = RenderView("r");

        cut.WaitUntil(() => Assert.That(_notFound, Is.EqualTo(1)));
        Assert.That(cut.FindAll("[data-lt-rule-layer]"), Is.Empty);
    }

    [Test]
    public void Rendering_again_for_the_same_rule_does_not_read_again()
    {
        Serve(Rule("r"));
        var cut = RenderView("r");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-rule-layer]"), Has.Count.EqualTo(1)));

        cut.Render(parameters => parameters.Add(view => view.Tenant, Acme).Add(view => view.RuleId, "r"));

        _policy.ReceivedWithAnyArgs(1).GetRuleAsync(default!, default!, default);
    }

    [Test]
    public void A_read_answered_after_the_view_moved_to_another_rule_is_dropped()
    {
        var first = new TaskCompletionSource<TenantRuleView?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.GetRuleAsync(Acme, "r", Arg.Any<CancellationToken>()).Returns(first.Task);
        _policy.GetRuleAsync(Acme, "s", Arg.Any<CancellationToken>()).Returns(Task.FromResult<TenantRuleView?>(Rule("s", subject: "second")));
        var cut = RenderView("r");

        cut.Render(parameters => parameters.Add(view => view.Tenant, Acme).Add(view => view.RuleId, "s"));
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=rule]").TextContent, Does.Contain("tenant-group:second")));
        first.SetResult(Rule("r", subject: "first"));

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=rule]").TextContent, Does.Contain("tenant-group:second").And.Not.Contain("tenant-group:first")));
    }

    [Test]
    public async Task A_read_answered_after_the_view_was_left_is_dropped()
    {
        var pending = new TaskCompletionSource<TenantRuleView?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.GetRuleAsync(Acme, "r", Arg.Any<CancellationToken>()).Returns(pending.Task);
        RenderView("r");

        await DisposeComponentsAsync();
        pending.SetResult(null);

        Assert.That(await TestPoll.TryUntilAsync(() => _notFound > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public async Task A_read_cancelled_by_leaving_the_view_is_quiet()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.GetRuleAsync(Acme, "r", Arg.Any<CancellationToken>()).Returns(call =>
        {
            started.TrySetResult();
            return Never<TenantRuleView?>(call.ArgAt<CancellationToken>(2));
        });
        RenderView("r");
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await DisposeComponentsAsync();

        Assert.That(await TestPoll.TryUntilAsync(() => _notFound > 0 || ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public void Rules_that_cannot_be_read_still_show_the_rule()
    {
        Serve(Rule("r"));
        _policy.ListRulesAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromException<TenantRulePage>(new InvalidOperationException("down")));

        var cut = RenderView("r");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[data-lt-rule-layer=tenant]"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("[data-lt-tenant-view=rule]").TextContent, Does.Not.Contain("could not be read"));
        });
        _policy.ReceivedWithAnyArgs(1).ListRulesAsync(default!, default!, default);
    }

    [Test]
    public async Task Leaving_while_the_rules_are_read_is_quiet()
    {
        Serve(Rule("r"));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.ListRulesAsync(default!, default!, default).ReturnsForAnyArgs(call =>
        {
            started.TrySetResult();
            return Never<TenantRulePage>(call.ArgAt<CancellationToken>(2));
        });
        RenderView("r");
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await DisposeComponentsAsync();

        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public void Without_an_access_catalogue_the_rule_still_shows()
    {
        // The area builds its own catalogue from the same container; the area falls back to its own, and only the view's resolution is refused.
        var refused = 0;
        Services.AddTransient<AccessCatalog>(_ =>
        {
            if (new System.Diagnostics.StackTrace().GetFrames().Any(frame => frame.GetMethod()?.DeclaringType == typeof(AccessArea)))
            {
                return null!;
            }

            Interlocked.Increment(ref refused);
            throw new InvalidOperationException("not registered");
        });
        Serve(Rule("r"));

        var cut = RenderView("r");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-rule-layer=tenant]"), Has.Count.EqualTo(1)));
        Assert.That(refused, Is.GreaterThanOrEqualTo(1), "the view asked for the catalogue and was refused");
    }

    [Test]
    public void A_platform_rule_is_read_only_and_explains_its_user_subject_without_a_tree_when_tenant_wide()
    {
        Serve(Rule("r", editable: false, subject: "alice", kind: TenantSubjectKind.User, scope: TenantRuleScopeKind.TenantWide, tree: null));

        var cut = RenderView("r");

        cut.WaitUntil(() =>
        {
            var view = cut.Find("[data-lt-tenant-view=rule]");
            Assert.That(view.QuerySelector("[data-lt-rule-layer]")!.GetAttribute("data-lt-rule-layer"), Is.EqualTo("platform"));
            Assert.That(view.TextContent, Does.Contain("A platform rule, read-only here."));
            Assert.That(view.QuerySelector("a[href$='t/acme/access/groups/alice']"), Is.Null, "a user subject is no group link");
            Assert.That(cut.FindAll("button").Where(button => button.TextContent is "Edit rule" or "Delete rule"), Is.Empty);
            Assert.That(cut.FindAll("[data-lt-rule-history]"), Is.Empty);
            var explain = cut.FindAll("a").Single(link => link.TextContent == "Explain for this subject").GetAttribute("href");
            Assert.That(explain, Does.Contain("subject=alice").And.Contain("kind=user").And.Not.Contain("tree="));
        });
    }

    [Test]
    public void A_rule_whose_subject_is_withheld_has_no_explain()
    {
        Serve(Rule("r", editable: false, subject: null));

        var cut = RenderView("r");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[data-lt-rule-layer=platform]"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("a").Where(link => link.TextContent == "Explain for this subject"), Is.Empty);
        });
    }

    [Test]
    public void A_refused_delete_is_told_and_keeps_the_rule()
    {
        Serve(Rule("r"));
        _policy.RemoveRuleAsync(Acme, "r", Arg.Any<CancellationToken>()).Returns(Task.FromException<bool>(new LatticeAuthorizationDeniedException("no")));
        var cut = RenderView("r");
        var before = Navigation.Uri;

        ConfirmDelete(cut, "r");

        cut.WaitUntil(() => Assert.That(ToastMessages, Does.Contain(TenantAccessFailure.DeniedMessage(Acme))));
        Assert.That(ToastMessages, Does.Not.Contain("Rule r deleted."));
        Assert.That(Navigation.Uri, Is.EqualTo(before));
    }

    [Test]
    public async Task Leaving_during_a_delete_is_quiet()
    {
        Serve(Rule("r"));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.RemoveRuleAsync(Acme, "r", Arg.Any<CancellationToken>()).Returns(call =>
        {
            started.TrySetResult();
            return Never<bool>(call.ArgAt<CancellationToken>(2));
        });
        var cut = RenderView("r");
        var before = Navigation.Uri;
        ConfirmDelete(cut, "r");
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await DisposeComponentsAsync();

        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0 || Navigation.Uri != before, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    [Test]
    public async Task A_delete_answered_after_the_view_was_left_is_not_announced()
    {
        Serve(Rule("r"));
        var pending = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.RemoveRuleAsync(Acme, "r", Arg.Any<CancellationToken>()).Returns(pending.Task);
        var cut = RenderView("r");
        var before = Navigation.Uri;
        ConfirmDelete(cut, "r");
        cut.WaitUntil(() => _policy.ReceivedWithAnyArgs(1).RemoveRuleAsync(default!, default!, default));

        await DisposeComponentsAsync();
        pending.SetResult(true);

        Assert.That(await TestPoll.TryUntilAsync(() => ToastMessages.Count > 0 || Navigation.Uri != before, TimeSpan.FromMilliseconds(500)), Is.False);
    }

    private static void ConfirmDelete(IRenderedComponent<TenantRuleDetailView> cut, string ruleId)
    {
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Delete rule"), Is.Not.Null));
        AccessForms.Button(cut, "Delete rule").Click();
        cut.Find(".lt-confirm input").Input(ruleId);
        cut.Find(".lt-confirm").Submit();
    }

    private IRenderedComponent<TenantRuleDetailView> RenderView(string ruleId, LtBreakpoint? band = null)
    {
        Navigation.OnNotFound += (_, _) => _notFound++;
        return Render<TenantRuleDetailView>(parameters =>
        {
            parameters.Add(view => view.Tenant, Acme).Add(view => view.RuleId, ruleId);
            if (band is { } value)
            {
                parameters.AddCascadingValue(LtBreakpointCascade.Name, value);
            }
        });
    }

    private void Serve(TenantRuleView rule) =>
        _policy.GetRuleAsync(Acme, rule.RuleId, Arg.Any<CancellationToken>()).Returns(Task.FromResult<TenantRuleView?>(rule));

    private static TenantRuleView Rule(
        string id,
        bool editable = true,
        string? subject = "eng",
        TenantSubjectKind kind = TenantSubjectKind.TenantGroup,
        TenantRuleScopeKind scope = TenantRuleScopeKind.Tree,
        string? tree = "orders") => new()
        {
            RuleId = id,
            Editable = editable,
            Layer = editable ? TenantRuleLayer.Tenant : TenantRuleLayer.Platform,
            Origin = editable ? TenantRuleOrigin.Tenant : TenantRuleOrigin.PlatformWide,
            SubjectId = subject,
            SubjectKind = kind,
            ScopeKind = scope,
            TreeName = tree,
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Allow,
        };

    private static async Task<T> Never<T>(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        return default!;
    }
}
