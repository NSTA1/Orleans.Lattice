using Bunit;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Areas.Access;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// The explain page: the server's verdict rendered as it arrives, matched rules
/// in precedence order, effective permissions, a question carried in the
/// address, validation, and the palette command's visible control.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessExplainPageTests : AccessTestContext
{
    [Test]
    public void Explain_renders_the_servers_verdict_reason_groups_and_matched_rules_in_precedence_order()
    {
        var wide = new LatticeAuthorizationRule("all-read", LatticeSubjectSelector.Group("ops"), LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow);
        var prefix = new LatticeAuthorizationRule("drafts-deny", LatticeSubjectSelector.Group("ops"), LatticeScope.Prefix("orders", "draft/"), LatticeOperation.Read, LatticeEffect.Deny);
        Admin.Explain = (subject, operation, scope, kind) => new AuthExplanation
        {
            SubjectId = subject,
            GroupIds = ["ops"],
            Operation = operation,
            Scope = scope,
            Allowed = true,
            Reason = "Allowed by all-read.",
            DefaultEffect = LatticeEffect.Deny,
            MatchedRules = [wide, prefix],
        };
        var cut = RenderAt<AccessExplainPage>("access/explain");

        AccessForms.Type(cut, "Subject", "alice");
        AccessForms.Choose(cut, "Operation", "read");
        AccessForms.Type(cut, "Tree", "orders");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-access-verdict__title").TextContent, Is.EqualTo("Allowed"));
            Assert.That(cut.Find(".lt-access-verdict .lt-pill").GetAttribute("data-lt-state"), Is.EqualTo("enabled"));
            Assert.That(cut.FindAll(".lt-dl__value").Select(value => value.TextContent.Trim()), Does.Contain("Allowed by all-read."));
            Assert.That(cut.Find(".lt-access-links a").GetAttribute("href"), Is.EqualTo("access/groups/ops"));
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent), Is.EqualTo(new[] { "drafts-deny", "all-read" }));
            Assert.That(Navigation.Uri, Does.EndWith("/access/explain?subject=alice&kind=user&operation=read&tree=orders"));
        });
    }

    [Test]
    public void A_denied_verdict_is_drawn_as_denied()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&kind=user&operation=write&tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-access-verdict__title").TextContent, Is.EqualTo("Denied"));
            Assert.That(cut.Find(".lt-access-verdict .lt-pill").GetAttribute("data-lt-state"), Is.EqualTo("failed"));
            Assert.That(cut.Find(".lt-access-caveat").TextContent, Does.Contain("token"));
        });
    }

    [Test]
    public void A_question_in_the_address_is_read_into_the_form_and_asked()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=ops&kind=group&operation=delete&tree=orders&prefix=draft%2F");

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Calls, Does.Contain(nameof(FakeAuthAdmin.ExplainAsync)));
            Assert.That(AccessForms.Field(cut, "Subject").GetAttribute("value"), Is.EqualTo("ops"));
            Assert.That(AccessForms.Field(cut, "Key prefix").GetAttribute("value"), Is.EqualTo("draft/"));
            Assert.That(cut.FindAll(".lt-dl__value").Select(value => value.TextContent.Trim()), Does.Contain("orders prefix draft/"));
        });
    }

    [Test]
    public void A_scopeless_operation_in_the_address_is_asked_cluster_wide()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=ops&kind=group&operation=appinstall");

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.HasField(cut, "Tree"), Is.False);
            Assert.That(cut.FindAll(".lt-dl__value").Select(value => value.TextContent.Trim()), Does.Contain("all trees (cluster-wide)"));
        });
    }

    [Test]
    public void Effective_permissions_list_the_subjects_groups_and_rules()
    {
        Admin.WithGroup("ops", null, "alice").WithRule(Rule("orders-read", group: "ops"));
        var cut = RenderAt<AccessExplainPage>("access/explain");

        AccessForms.Type(cut, "Subject", "alice");
        AccessForms.Button(cut, "Effective permissions").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("#lt-access-permissions").TextContent, Does.Contain("alice"));
            Assert.That(cut.Find(".lt-access-links a").TextContent, Is.EqualTo("group:ops"));
            Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("orders-read"));
            Assert.That(Navigation.Uri, Does.EndWith("/access/explain?subject=alice&kind=user&view=permissions"));
        });
    }

    [Test]
    public void The_permissions_view_in_the_address_is_asked_on_open()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=ops&kind=group&view=permissions");

        cut.WaitUntil(() => Assert.That(Admin.Calls, Does.Contain(nameof(FakeAuthAdmin.EffectivePermissionsAsync))));
    }

    [Test]
    public void A_missing_subject_and_tree_are_named_and_nothing_is_asked()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain");

        cut.Find("form.lt-access-form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.EqualTo("Choose the user or group to explain."));
            Assert.That(AccessForms.ErrorOf(cut, "Tree"), Is.EqualTo("Enter the tree to ask about."));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.ExplainAsync)));
        });

        AccessForms.Choose(cut, "Scope", AccessRuleDraft.KeyScope);
        AccessForms.Type(cut, "Subject", "alice");
        AccessForms.Type(cut, "Tree", "orders");
        cut.Find("form.lt-access-form").Submit();
        Assert.That(AccessForms.ErrorOf(cut, "Key"), Is.EqualTo("Enter the key."));
    }

    [Test]
    public void While_the_cluster_answers_a_skeleton_shows()
    {
        var hold = Admin.Hold(nameof(FakeAuthAdmin.ExplainAsync));

        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&operation=read&tree=orders");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));
        cut.InvokeAsync(hold.SetResult);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-access-verdict"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_restricted_identity_is_told_it_is_not_permitted()
    {
        Admin.Fail(nameof(FakeAuthAdmin.ExplainAsync), new LatticeAuthorizationDeniedException("_lattice_policy", LatticeOperation.Admin, "ops", "no"));

        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&operation=read&tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Not permitted"));
            Assert.That(cut.FindAll(".lt-access-verdict"), Is.Empty);
        });
    }

    [Test]
    public void The_explain_command_has_a_visible_control_at_its_target()
    {
        var command = new AccessArea(Services).Commands.Single(candidate => candidate.Id == AccessArea.ExplainCommandId);

        var cut = RenderAt<AccessExplainPage>(command.Target!.ToHref());

        Assert.Multiple(() =>
        {
            Assert.That(command.Title, Is.EqualTo("Explain access..."));
            ExplorerCommandControls.AssertVisibleControl(cut, command);
            Assert.That(cut.Find("[data-lt-command=\"access.explain\"]").GetAttribute("aria-current"), Is.EqualTo("page"));
        });
    }
}
