using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The rule editor: the operation checklist with its scopeless cluster-wide
/// capabilities, client-side refusals, the server's refusals placed on their
/// fields, and the scopes the access model allows.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessRuleEditorTests : AccessTestContext
{
    /// <summary>Fills the id, a group subject and a tree scope.</summary>
    internal static void Fill<TComponent>(IRenderedComponent<TComponent> cut, string ruleId, string group, string tree)
        where TComponent : IComponent
    {
        AccessForms.Type(cut, "Rule id", ruleId);
        AccessForms.Choose(cut, "Subject kind", "group");
        AccessForms.Type(cut, "Subject", group);
        AccessForms.Choose(cut, "Scope", AccessRuleDraft.TreeScope);
        AccessForms.Type(cut, "Tree", tree);
    }

    [Test]
    public void The_checklist_offers_every_operation_in_three_groups()
    {
        var cut = RenderEditor();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("legend").Select(legend => legend.TextContent), Is.EqualTo(new[] { "Data operations", "Administration", "Cluster-wide capabilities" }));
            Assert.That(cut.FindAll("[data-lt-operation]"), Has.Count.EqualTo(Enum.GetValues<LatticeOperation>().Length - 1));
            Assert.That(cut.FindAll("fieldset")[2].QuerySelectorAll("label").Select(label => label.TextContent), Is.EqualTo(new[] { "Telemetry", "App install" }));
        });
    }

    [Test]
    public void App_install_and_telemetry_cannot_be_ticked_with_a_narrower_scope_but_can_with_the_cluster_wide_one()
    {
        var cut = RenderEditor();

        AccessForms.Choose(cut, "Scope", AccessRuleDraft.TreeScope);
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-operation=\"appinstall\"]").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("[data-lt-operation=\"telemetry\"]").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("[data-lt-operation=\"read\"]").HasAttribute("disabled"), Is.False);
        });

        AccessForms.Choose(cut, "Scope", AccessRuleDraft.ClusterScope);
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-operation=\"appinstall\"]").HasAttribute("disabled"), Is.False);
            Assert.That(AccessForms.HasField(cut, "Tree"), Is.False, "the cluster-wide scope names no tree");
        });
    }

    [Test]
    public void The_editor_refuses_to_pair_a_scopeless_capability_with_a_narrower_scope()
    {
        var cut = RenderEditor();
        AccessForms.Type(cut, "Rule id", "telemetry-readers");
        AccessForms.Type(cut, "Subject", "ops");
        AccessForms.Choose(cut, "Scope", AccessRuleDraft.ClusterScope);
        cut.Find("[data-lt-operation=\"telemetry\"]").Change(true);
        AccessForms.Choose(cut, "Scope", AccessRuleDraft.TreeScope);
        AccessForms.Type(cut, "Tree", "orders");

        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-field=\"operations\"]").TextContent, Is.EqualTo(AccessRuleDraft.ScopelessMessage));
            Assert.That(cut.Find("[data-lt-operation=\"telemetry\"]").HasAttribute("disabled"), Is.False, "a ticked capability stays clearable");
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.PutRuleAsync)));
        });

        AccessForms.Choose(cut, "Scope", AccessRuleDraft.ClusterScope);
        Assert.That(cut.FindAll("[data-lt-field=\"operations\"]"), Is.Empty, "choosing the cluster-wide scope clears the refusal");
    }

    [Test]
    public void A_cluster_wide_capability_is_saved_at_the_cluster_wide_scope()
    {
        LatticeAuthorizationRule? saved = null;
        var cut = RenderEditor(onSaved: rule => saved = rule);
        AccessForms.Type(cut, "Rule id", "installers");
        AccessForms.Choose(cut, "Subject kind", "group");
        AccessForms.Type(cut, "Subject", "platform");
        AccessForms.Choose(cut, "Scope", AccessRuleDraft.ClusterScope);
        cut.Find("[data-lt-operation=\"appinstall\"]").Change(true);

        cut.Find("form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(saved, Is.Not.Null);
            Assert.That(saved!.Scope, Is.EqualTo(LatticeScope.ClusterWide()));
            Assert.That(saved.Operations, Is.EqualTo(LatticeOperation.AppInstall));
            Assert.That(saved.Subject, Is.EqualTo(LatticeSubjectSelector.Group("platform")));
            Assert.That(Admin.Rules, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Missing_fields_are_each_named_and_nothing_is_sent()
    {
        var cut = RenderEditor();
        AccessForms.Choose(cut, "Scope", AccessRuleDraft.PrefixScope);

        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Rule id"), Is.EqualTo("Enter a rule id."));
            Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.EqualTo("Choose who the rule is about."));
            Assert.That(AccessForms.ErrorOf(cut, "Tree"), Is.EqualTo("Enter the tree the rule governs."));
            Assert.That(AccessForms.ErrorOf(cut, "Key prefix"), Is.EqualTo("Enter the prefix."));
            Assert.That(cut.Find("[data-lt-field=\"operations\"]").TextContent, Is.EqualTo("Choose at least one operation."));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.PutRuleAsync)));
        });
    }

    [Test]
    public void A_new_id_under_the_app_prefix_is_refused_before_it_is_sent()
    {
        var cut = RenderEditor();
        Fill(cut, "app:crm:viewer:x", "ops", "orders");
        cut.Find("[data-lt-operation=\"read\"]").Change(true);

        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Rule id"), Does.StartWith("Ids starting with app: belong to installed apps"));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.PutRuleAsync)));
        });
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void A_directory_validation_failure_is_shown_beside_the_subject(bool typed)
    {
        Exception failure = typed
            ? LatticeDirectoryValidationException.Unresolved("ghost", DirectoryPrincipalKind.Group, "subject")
            : new ArgumentException("Directory validation failed: the Group id 'ghost' does not resolve to any principal in the configured identity directory.");
        Admin.Fail(nameof(FakeAuthAdmin.PutRuleAsync), failure);
        var cut = RenderEditor();
        Fill(cut, "orders-read", "ghost", "orders");
        cut.Find("[data-lt-operation=\"read\"]").Change(true);

        cut.Find("form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Subject"), Does.StartWith("Directory validation failed:"));
            Assert.That(AccessForms.Field(cut, "Subject").GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(cut.FindAll("[data-lt-field=\"form\"]"), Is.Empty);
        });
    }

    [Test]
    public void The_servers_app_owned_refusal_is_shown_beside_the_rule_id()
    {
        Admin.Fail(nameof(FakeAuthAdmin.PutRuleAsync), LatticeAppOwnedRuleException.Rejected("app:crm:x", "rule"));
        var cut = RenderEditor();
        Fill(cut, "orders-read", "ops", "orders");
        cut.Find("[data-lt-operation=\"read\"]").Change(true);

        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Rule id"), Does.Contain("is owned by an installed app")));
    }

    [Test]
    public void Any_other_refusal_is_the_forms_error()
    {
        Admin.Fail(nameof(FakeAuthAdmin.PutRuleAsync), new ArgumentException("All-trees grants are disabled."));
        var cut = RenderEditor();
        Fill(cut, "orders-read", "ops", "orders");
        cut.Find("[data-lt-operation=\"read\"]").Change(true);

        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-field=\"form\"]").TextContent, Is.EqualTo("All-trees grants are disabled.")));
    }

    [Test]
    public void Editing_an_existing_rule_fixes_its_id_and_scope_and_saves_the_change()
    {
        Admin.WithRule(Rule("orders-read"));
        LatticeAuthorizationRule? saved = null;
        var cut = RenderEditor(Rule("orders-read"), rule => saved = rule);

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.Field(cut, "Rule id").HasAttribute("readonly"), Is.True);
            Assert.That(AccessForms.Field(cut, "Tree").HasAttribute("readonly"), Is.True);
            Assert.That(AccessForms.Field(cut, "Scope").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("[data-lt-operation=\"read\"]").HasAttribute("checked"), Is.True);
        });

        cut.Find("[data-lt-operation=\"write\"]").Change(true);
        AccessForms.Choose(cut, "Effect", "deny");
        cut.Find("form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(saved!.Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
            Assert.That(saved.Effect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(Admin.Rules.Single().Effect, Is.EqualTo(LatticeEffect.Deny));
        });
    }

    [Test]
    public void The_access_administration_scope_is_offered_only_when_delegation_is_on()
    {
        var off = RenderEditor();
        var on = RenderEditor(model: Admin.Model with { AccessAdministrationDelegationEnabled = true });

        Assert.Multiple(() =>
        {
            Assert.That(ScopeValues(off), Does.Not.Contain(AccessRuleDraft.AccessAdministrationScope));
            Assert.That(ScopeValues(on), Does.Contain(AccessRuleDraft.AccessAdministrationScope));
        });
    }

    [Test]
    public void A_cluster_wide_scope_warns_when_all_trees_grants_are_off()
    {
        var cut = RenderEditor();

        AccessForms.Choose(cut, "Scope", AccessRuleDraft.ClusterScope);

        Assert.That(AccessForms.Field(cut, "Scope").ParentElement!.QuerySelector(".lt-field__hint")!.TextContent, Does.StartWith("All-trees grants are off"));
    }

    [Test]
    public void Cancel_raises_the_callback()
    {
        var cancelled = false;
        var cut = Render<AccessRuleEditor>(parameters => parameters.Add(editor => editor.OnCancel, () => cancelled = true));

        AccessForms.Button(cut, "Cancel").Click();

        Assert.That(cancelled, Is.True);
    }

    private static string[] ScopeValues(IRenderedComponent<AccessRuleEditor> cut) =>
        [.. AccessForms.Field(cut, "Scope").QuerySelectorAll("option").Select(option => option.GetAttribute("value")!)];

    private IRenderedComponent<AccessRuleEditor> RenderEditor(
        LatticeAuthorizationRule? rule = null,
        Action<LatticeAuthorizationRule>? onSaved = null,
        AccessModelDescriptor? model = null) =>
        Render<AccessRuleEditor>(parameters => parameters
            .Add(editor => editor.Rule, rule)
            .Add(editor => editor.Model, model ?? Admin.Model)
            .Add(editor => editor.OnSaved, saved => onSaved?.Invoke(saved)));
}
