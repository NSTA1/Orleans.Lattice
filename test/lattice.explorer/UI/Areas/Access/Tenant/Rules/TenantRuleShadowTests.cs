using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// Issue #4163: the shadow check - a platform rule that already decides a tenant
/// rule's scope for its subject is found and named, and one that does not (a
/// narrower scope, another tree, subject or operation, a tenant rule) is not.
/// </summary>
[TestFixture]
public sealed class TenantRuleShadowTests
{
    [Test]
    public void A_platform_rule_over_the_whole_tree_shadows_a_rule_on_it_for_the_same_subject()
    {
        var form = Form(TenantRuleFormat.PrefixScope, keyOrPrefix: "eu/");
        var platform = Platform("guard", TenantRuleScopeKind.Tree);

        var hits = TenantRuleShadow.Find(form, [platform]);

        Assert.Multiple(() =>
        {
            Assert.That(hits, Has.Count.EqualTo(1));
            Assert.That(hits[0].Rule, Is.SameAs(platform));
            Assert.That(hits[0].Partial, Is.False);
            Assert.That(TenantRuleShadow.Message(hits[0], form), Is.EqualTo("This rule will not take effect for tenant-group:eng: platform rule guard decides first."));
        });
    }

    [Test]
    [TestCase(TenantRuleScopeKind.Prefix, "eu/", TenantRuleFormat.PrefixScope, "eu/west/", true)]
    [TestCase(TenantRuleScopeKind.Prefix, "eu/", TenantRuleFormat.KeyScope, "eu/1", true)]
    [TestCase(TenantRuleScopeKind.Prefix, "eu/west/", TenantRuleFormat.PrefixScope, "eu/", false)]
    [TestCase(TenantRuleScopeKind.Prefix, "eu/", TenantRuleFormat.TreeScope, "", false)]
    [TestCase(TenantRuleScopeKind.Key, "eu/1", TenantRuleFormat.KeyScope, "eu/1", true)]
    [TestCase(TenantRuleScopeKind.Key, "eu/1", TenantRuleFormat.KeyScope, "eu/2", false)]
    [TestCase(TenantRuleScopeKind.Key, "eu/1", TenantRuleFormat.PrefixScope, "eu/1", false)]
    public void A_narrower_platform_scope_shadows_only_what_it_contains(
        TenantRuleScopeKind platformScope, string platformKey, string scope, string keyOrPrefix, bool shadowed)
    {
        var hits = TenantRuleShadow.Find(Form(scope, keyOrPrefix: keyOrPrefix), [Platform("guard", platformScope, keyOrPrefix: platformKey)]);

        Assert.That(hits, shadowed ? Has.Count.EqualTo(1) : Is.Empty);
    }

    [Test]
    public void A_tenant_wide_rule_is_shadowed_tree_by_tree_and_the_tree_is_named()
    {
        var form = Form(TenantRuleFormat.TenantWideScope);

        var hits = TenantRuleShadow.Find(form, [Platform("guard", TenantRuleScopeKind.Tree), Platform("narrow", TenantRuleScopeKind.Prefix, keyOrPrefix: "eu/")]);

        Assert.Multiple(() =>
        {
            Assert.That(hits.Select(hit => hit.Rule.RuleId), Is.EqualTo(new[] { "guard" }));
            Assert.That(TenantRuleShadow.Message(hits[0], form), Does.Contain(" on tree orders: platform rule guard decides first."));
        });
    }

    [Test]
    public void A_platform_rule_deciding_only_some_operations_names_them()
    {
        var form = Form(TenantRuleFormat.TreeScope, operations: LatticeOperation.Read | LatticeOperation.Write);

        var hits = TenantRuleShadow.Find(form, [Platform("guard", TenantRuleScopeKind.Tree, operations: LatticeOperation.Write | LatticeOperation.Delete)]);

        Assert.Multiple(() =>
        {
            Assert.That(hits[0].Partial, Is.True);
            Assert.That(hits[0].Operations, Is.EqualTo(LatticeOperation.Write));
            Assert.That(TenantRuleShadow.Message(hits[0], form), Does.Contain("tenant-group:eng (Write): platform rule guard"));
        });
    }

    [Test]
    public void Another_subject_tree_or_operation_or_a_tenant_or_withheld_rule_shadows_nothing()
    {
        var form = Form(TenantRuleFormat.TreeScope);
        TenantRuleView[] rules =
        [
            Platform("other-subject", TenantRuleScopeKind.Tree) with { SubjectId = "ops" },
            Platform("other-kind", TenantRuleScopeKind.Tree) with { SubjectKind = TenantSubjectKind.ClusterGroup },
            Platform("other-tree", TenantRuleScopeKind.Tree) with { TreeName = "billing" },
            Platform("other-operation", TenantRuleScopeKind.Tree, operations: LatticeOperation.Delete),
            Platform("tenant", TenantRuleScopeKind.Tree) with { Layer = TenantRuleLayer.Tenant, Origin = TenantRuleOrigin.Tenant },
            Platform("withheld", TenantRuleScopeKind.Tree) with { Origin = TenantRuleOrigin.PlatformWide },
        ];

        Assert.That(TenantRuleShadow.Find(form, rules), Is.Empty);
    }

    [Test]
    public void A_form_without_a_subject_or_operations_is_never_shadowed()
    {
        var noSubject = Form(TenantRuleFormat.TreeScope);
        noSubject.SubjectId = " ";
        var noOperations = Form(TenantRuleFormat.TreeScope, operations: LatticeOperation.None);
        TenantRuleView[] rules = [Platform("guard", TenantRuleScopeKind.Tree)];

        Assert.Multiple(() =>
        {
            Assert.That(TenantRuleShadow.Find(noSubject, rules), Is.Empty);
            Assert.That(TenantRuleShadow.Find(noOperations, rules), Is.Empty);
            Assert.That(() => TenantRuleShadow.Find(null!, rules), Throws.ArgumentNullException);
            Assert.That(() => TenantRuleShadow.Find(noSubject, null!), Throws.ArgumentNullException);
            Assert.That(() => TenantRuleShadow.Message(null!, noSubject), Throws.ArgumentNullException);
        });
    }

    private static TenantRuleForm Form(string scope, string keyOrPrefix = "", LatticeOperation operations = LatticeOperation.Read)
    {
        var form = TenantRuleForm.New();
        form.RuleId = "eng-orders";
        form.SubjectId = "eng";
        form.ScopeValue = scope;
        form.TreeName = "orders";
        form.KeyOrPrefix = keyOrPrefix;
        form.Operations = operations;
        return form;
    }

    private static TenantRuleView Platform(
        string id,
        TenantRuleScopeKind scope,
        string? keyOrPrefix = null,
        LatticeOperation operations = LatticeOperation.Read) => new()
    {
        RuleId = id,
        Layer = TenantRuleLayer.Platform,
        Origin = TenantRuleOrigin.PlatformTree,
        SubjectId = "eng",
        SubjectKind = TenantSubjectKind.TenantGroup,
        ScopeKind = scope,
        TreeName = "orders",
        KeyOrPrefix = keyOrPrefix,
        Operations = operations,
        Effect = LatticeEffect.Deny,
    };
}
