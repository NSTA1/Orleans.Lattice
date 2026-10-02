using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// Issue #4163: the tenant rule editor - scopes over the tenant's own trees or
/// every tree in it, the data-plane operations only, a tree field that offers no
/// app-owned or other tenant's tree and refuses one typed, a suggested local id,
/// the shadow warning before save, and the server's confinement refusals placed
/// beside their fields with their typed reasons.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantRuleEditorTests : TenantRulesTestContext
{
    [Test]
    public void The_scopes_are_the_tenants_trees_and_every_tree_in_it_never_the_cluster()
    {
        var cut = RenderEditor();

        var scopes = AccessForms.Field(cut, "Scope").QuerySelectorAll("option").Select(option => option.TextContent.Trim()).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(scopes, Is.EqualTo(new[] { "Whole tree", "Key prefix in a tree", "Single key in a tree", "Every tree in this tenant" }));
            Assert.That(scopes, Has.None.Contains("cluster").And.None.Contains("Access administration"));
        });
    }

    [Test]
    public void Only_the_data_plane_operations_are_offered()
    {
        var cut = RenderEditor();

        var offered = cut.FindAll("[data-lt-operation]").Select(box => box.GetAttribute("data-lt-operation")).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(offered, Is.EquivalentTo(TenantRuleFormat.DataPlaneOperations.Select(option => option.Value)));
            Assert.That(offered, Has.None.EqualTo("telemetry").And.None.EqualTo("appinstall").And.None.EqualTo("replication").And.None.EqualTo("treelifecycle"));
            Assert.That(cut.FindAll("legend").Select(legend => legend.TextContent), Is.EqualTo(new[] { "Data operations", "Administration" }));
        });
    }

    [Test]
    public async Task The_tree_field_offers_only_the_tenants_own_ordinary_trees()
    {
        UseTrees("t/acme/orders", "t/acme/billing", "t/acme/a/crm/contacts", "t/globex/ledger", "legacy");
        var source = new TenantTreeSuggestionSource(Services, () => Acme);

        var answer = await source.SuggestAsync(string.Empty, 20, CancellationToken.None);

        var none = await new TenantTreeSuggestionSource(Services, () => null).SuggestAsync(string.Empty, 20, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(none.Items, Is.Empty, "with no tenant, nothing is offered");
            Assert.That(answer.Items.Select(item => item.Value), Is.EquivalentTo(new[] { "billing", "orders" }));
            Assert.That(answer.Items.Select(item => item.Detail), Is.All.EqualTo(TenantTreeSuggestionSource.Detail));
            Assert.That(answer.UnavailableReason, Is.Null);
        });
    }

    [Test]
    public async Task The_tree_field_says_when_the_trees_cannot_be_listed()
    {
        UseTrees("t/acme/orders").Fault = call => call == "ListTreesAsync" ? new InvalidOperationException("down") : null;

        var down = await new TenantTreeSuggestionSource(Services, () => Acme).SuggestAsync(string.Empty, 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(down.UnavailableReason, Is.EqualTo(TenantTreeSuggestionSource.UnavailableReason));
            Assert.That(() => new TenantTreeSuggestionSource(null!, () => Acme), Throws.ArgumentNullException);
            Assert.That(() => new TenantTreeSuggestionSource(Services, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_shell_registers_one_tree_source_per_circuit()
    {
        using var scope = Services.CreateScope();

        Assert.That(scope.ServiceProvider.GetRequiredService<TenantTreeSuggestionSource>(), Is.SameAs(scope.ServiceProvider.GetRequiredService<TenantTreeSuggestionSource>()));
    }

    [Test]
    [TestCase("a/crm/contacts", "App-owned")]
    [TestCase("t/globex/ledger", "another tenant's tree")]
    [TestCase("sys-auth-policy", "Reserved and system")]
    public void A_tree_the_tenant_may_not_govern_is_refused_as_it_is_typed_and_never_sent(string tree, string reason)
    {
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderEditor();
        AccessForms.Type(cut, "Rule id", "r");
        AccessForms.Type(cut, "Subject", "eng");

        AccessForms.Type(cut, "Tree", tree);
        Assert.That(AccessForms.ErrorOf(cut, "Tree"), Does.Contain(reason));

        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Tree"), Does.Contain(reason));
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantPolicyAdmin.PutRuleAsync)));
        });
    }

    [Test]
    public void A_new_rule_on_one_of_the_tenants_trees_is_saved_through_the_tenant_policy()
    {
        TenantFacades.WithGroup(Acme, "eng");
        TenantRuleView? saved = null;
        var cut = RenderEditor(onSaved: rule => saved = rule);
        AccessForms.Type(cut, "Rule id", "eng-orders");
        AccessForms.Type(cut, "Subject", "eng");
        AccessForms.Choose(cut, "Scope", TenantRuleFormat.PrefixScope);
        AccessForms.Type(cut, "Tree", "orders");
        AccessForms.Type(cut, "Key prefix", "eu/");
        cut.Find("[data-lt-operation=\"write\"]").Change(true);

        cut.Find("form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(saved, Is.Not.Null);
            Assert.That(saved!.Layer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(saved.SubjectKind, Is.EqualTo(TenantSubjectKind.TenantGroup));
            Assert.That(saved.ScopeKind, Is.EqualTo(TenantRuleScopeKind.Prefix));
            Assert.That(saved.TreeName, Is.EqualTo("orders"));
            Assert.That(saved.KeyOrPrefix, Is.EqualTo("eu/"));
            Assert.That(saved.Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
        });
    }

    [Test]
    public void A_rule_over_every_tree_in_the_tenant_names_no_tree()
    {
        TenantFacades.WithGroup(Acme, "eng");
        TenantRuleView? saved = null;
        var cut = RenderEditor(onSaved: rule => saved = rule);
        AccessForms.Type(cut, "Rule id", "eng-everywhere");
        AccessForms.Type(cut, "Subject", "eng");

        AccessForms.Choose(cut, "Scope", TenantRuleFormat.TenantWideScope);
        Assert.That(AccessForms.HasField(cut, "Tree"), Is.False);
        cut.Find("form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(saved?.ScopeKind, Is.EqualTo(TenantRuleScopeKind.TenantWide));
            Assert.That(saved!.TreeName, Is.Null);
        });
    }

    [Test]
    public void A_local_id_is_suggested_and_can_be_used()
    {
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderEditor();
        Assert.That(cut.FindAll("[data-lt-suggested-rule-id]"), Is.Empty, "nothing to suggest from yet");

        AccessForms.Type(cut, "Subject", "eng");
        AccessForms.Type(cut, "Tree", "orders");
        Assert.That(cut.Find("[data-lt-suggested-rule-id]").GetAttribute("data-lt-suggested-rule-id"), Is.EqualTo("allow-eng-orders"));

        AccessForms.Button(cut, "Use suggested id").Click();

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.Field(cut, "Rule id").GetAttribute("value"), Is.EqualTo("allow-eng-orders"));
            Assert.That(cut.FindAll("[data-lt-suggested-rule-id]"), Is.Empty, "a chosen id is not suggested again");
        });
    }

    [Test]
    public void A_platform_rule_that_decides_the_scope_first_is_named_before_saving()
    {
        var guard = SeedPlatformRule("guard", "orders", "eng");
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderEditor(rules: [guard]);
        AccessForms.Type(cut, "Rule id", "eng-orders");
        Assert.That(cut.FindAll("[data-lt-shadowed]"), Is.Empty);

        AccessForms.Type(cut, "Subject", "eng");
        AccessForms.Type(cut, "Tree", "orders");

        var warning = cut.Find("[data-lt-shadowed]");
        Assert.Multiple(() =>
        {
            Assert.That(warning.GetAttribute("role"), Is.EqualTo("status"));
            Assert.That(warning.QuerySelector("[data-lt-shadowed-by]")!.TextContent, Is.EqualTo("This rule will not take effect for tenant-group:eng: platform rule guard decides first."));
        });

        AccessForms.Type(cut, "Tree", "billing");
        Assert.That(cut.FindAll("[data-lt-shadowed]"), Is.Empty, "a rule on another tree is not shadowed");
    }

    [Test]
    [TestCase(TenantAccessConfinementRule.RuleTree, "Tree", "Not one of this tenant's trees")]
    [TestCase(TenantAccessConfinementRule.ForeignTenantGroup, "Subject", "Another tenant's group")]
    [TestCase(TenantAccessConfinementRule.GroupNesting, "Subject", "Group nesting is not allowed")]
    [TestCase(TenantAccessConfinementRule.ReservedRuleId, "Rule id", "A reserved rule id")]
    public void A_confinement_refusal_is_shown_beside_its_field_with_its_reason(TenantAccessConfinementRule rule, string field, string reason)
    {
        Script().Faults[nameof(ILatticeTenantPolicyAdmin.PutRuleAsync)] = new TenantAccessConfinementException(Acme, rule, "Refused by the store.", "rule");
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderFilledEditor();

        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, field), Is.EqualTo($"{reason}: Refused by the store.")));
    }

    [Test]
    public void An_operations_refusal_is_shown_beside_the_checklist()
    {
        Script().Faults[nameof(ILatticeTenantPolicyAdmin.PutRuleAsync)] = new TenantAccessConfinementException(Acme, TenantAccessConfinementRule.RuleOperations, "Too wide.", "rule");
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderFilledEditor();

        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-field=operations]").TextContent, Is.EqualTo("Not a data-plane operation: Too wide.")));
    }

    [Test]
    public void A_tree_refusal_on_a_tenant_wide_rule_and_a_cap_are_the_forms_errors()
    {
        var script = Script();
        script.Faults[nameof(ILatticeTenantPolicyAdmin.PutRuleAsync)] = new TenantAccessConfinementException(Acme, TenantAccessConfinementRule.RuleTree, "No.", "rule");
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderFilledEditor();
        AccessForms.Choose(cut, "Scope", TenantRuleFormat.TenantWideScope);

        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-field=form]").TextContent, Is.EqualTo("Not one of this tenant's trees: No.")));

        script.Faults[nameof(ILatticeTenantPolicyAdmin.PutRuleAsync)] = new LatticeQuotaExceededException("Tenant 'acme' is at its MaxTenantRules cap of 1.", string.Empty, "MaxTenantRules", 1, 1, Acme);
        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-field=form]").TextContent, Does.Contain("MaxTenantRules cap")));
    }

    [Test]
    public void A_clashing_id_is_refused_before_it_replaces_another_rule()
    {
        SeedTenantRule("eng-orders", "orders", "eng");
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderFilledEditor();

        cut.Find("form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Rule id"), Does.Contain("already has a rule with this id"));
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantPolicyAdmin.PutRuleAsync)));
        });
    }

    [Test]
    public void An_existing_rule_keeps_its_id_and_scope_and_cancel_is_raised()
    {
        var rule = SeedTenantRule("eng-orders", "orders", "eng", kind: TenantSubjectKind.ClusterGroup);
        var cancelled = false;
        var cut = Render<TenantRuleEditor>(parameters => parameters
            .Add(editor => editor.Tenant, Acme)
            .Add(editor => editor.Rule, rule)
            .Add(editor => editor.OnCancel, () => cancelled = true));

        AccessForms.Button(cut, "Cancel").Click();

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.Field(cut, "Rule id").HasAttribute("readonly"), Is.True);
            Assert.That(AccessForms.Field(cut, "Scope").HasAttribute("disabled"), Is.True);
            Assert.That(AccessForms.Field(cut, "Subject kind").QuerySelector("option[selected]")?.GetAttribute("value"), Is.EqualTo("cluster-group"));
            Assert.That(cut.FindAll("[data-lt-suggested-rule-id]"), Is.Empty);
            Assert.That(cancelled, Is.True);
        });
    }

    private IRenderedComponent<TenantRuleEditor> RenderFilledEditor()
    {
        var cut = RenderEditor();
        AccessForms.Type(cut, "Rule id", "eng-orders");
        AccessForms.Type(cut, "Subject", "eng");
        AccessForms.Type(cut, "Tree", "orders");
        return cut;
    }

    private IRenderedComponent<TenantRuleEditor> RenderEditor(Action<TenantRuleView>? onSaved = null, IReadOnlyList<TenantRuleView>? rules = null) =>
        Render<TenantRuleEditor>(parameters => parameters
            .Add(editor => editor.Tenant, Acme)
            .Add(editor => editor.Rules, rules ?? [])
            .Add(editor => editor.OnSaved, rule => onSaved?.Invoke(rule)));
}
