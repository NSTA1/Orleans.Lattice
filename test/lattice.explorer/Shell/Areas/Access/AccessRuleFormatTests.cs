using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Areas.Access;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// How the area names operations, scopes, subjects and effects, which
/// operations are scopeless, and the presentation-only precedence order.
/// </summary>
[TestFixture]
public sealed class AccessRuleFormatTests
{
    [Test]
    public void The_catalogue_covers_every_operation_once_and_marks_only_app_install_and_telemetry_scopeless()
    {
        var flags = AccessRuleFormat.Operations.Select(option => option.Flag).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(flags, Is.EquivalentTo(Enum.GetValues<LatticeOperation>().Where(flag => flag != LatticeOperation.None)));
            Assert.That(AccessRuleFormat.Operations.Where(option => option.IsScopeless).Select(option => option.Flag),
                Is.EquivalentTo(new[] { LatticeOperation.AppInstall, LatticeOperation.Telemetry }));
            Assert.That(AccessRuleFormat.Operations.Where(option => option.Group == AccessOperationGroup.ClusterWide).All(option => option.IsScopeless), Is.True);
            Assert.That(AccessRuleFormat.Operations.Select(option => option.Value), Is.Unique);
            Assert.That(AccessRuleFormat.Operations.Single(option => option.Flag == LatticeOperation.AppInstall).Value, Is.EqualTo("appinstall"));
        });
    }

    [Test]
    public void Operations_are_labelled_in_catalogue_order()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessRuleFormat.OperationsLabel(LatticeOperation.None), Is.EqualTo("none"));
            Assert.That(AccessRuleFormat.OperationsLabel(LatticeOperation.Write | LatticeOperation.Read | LatticeOperation.AppInstall), Is.EqualTo("Read, Write, App install"));
            Assert.That(AccessRuleFormat.OperationsLabel((LatticeOperation)(1 << 20)), Is.EqualTo("1048576"));
            Assert.That(AccessRuleFormat.OperationLabel(LatticeOperation.CrdtApply), Is.EqualTo("CRDT apply"));
            Assert.That(AccessRuleFormat.OperationLabel(LatticeOperation.None), Is.EqualTo("None"));
        });
    }

    [Test]
    public void Scopes_subjects_and_effects_have_readable_labels()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.ClusterWide()), Is.EqualTo("all trees (cluster-wide)"));
            Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Tree(LatticeAuthReservedTrees.PolicyTreeId)), Is.EqualTo("access administration"));
            Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Tree("orders")), Is.EqualTo("orders"));
            Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Prefix("orders", "d/")), Is.EqualTo("orders prefix d/"));
            Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Key("orders", "k")), Is.EqualTo("orders key k"));
            Assert.That(AccessRuleFormat.SubjectLabel(LatticeSubjectSelector.User("alice")), Is.EqualTo("user:alice"));
            Assert.That(AccessRuleFormat.SubjectLabel(LatticeSubjectSelector.Group("ops")), Is.EqualTo("group:ops"));
            Assert.That(AccessRuleFormat.EffectLabel(LatticeEffect.Deny), Is.EqualTo("Deny"));
            Assert.That(AccessRuleFormat.EffectLabel(LatticeEffect.Allow), Is.EqualTo("Allow"));
            Assert.That(AccessRuleFormat.IsClusterWide(LatticeScope.Prefix("*", "x")), Is.False);
        });
    }

    [Test]
    public void Precedence_puts_an_all_trees_deny_first_narrower_scopes_next_deny_before_allow_and_an_all_trees_allow_last()
    {
        var rules = new[]
        {
            Make("wide-allow", LatticeScope.ClusterWide(), LatticeEffect.Allow),
            Make("tree-allow", LatticeScope.Tree("orders"), LatticeEffect.Allow),
            Make("tree-deny", LatticeScope.Tree("orders"), LatticeEffect.Deny),
            Make("short-prefix", LatticeScope.Prefix("orders", "a"), LatticeEffect.Allow),
            Make("long-prefix", LatticeScope.Prefix("orders", "abc"), LatticeEffect.Allow),
            Make("key", LatticeScope.Key("orders", "k"), LatticeEffect.Allow),
            Make("wide-deny", LatticeScope.ClusterWide(), LatticeEffect.Deny),
        };

        var ordered = AccessRuleFormat.InPrecedenceOrder(rules).Select(rule => rule.RuleId);

        Assert.That(ordered, Is.EqualTo(new[] { "wide-deny", "key", "long-prefix", "short-prefix", "tree-deny", "tree-allow", "wide-allow" }));
    }

    private static LatticeAuthorizationRule Make(string id, LatticeScope scope, LatticeEffect effect) =>
        new(id, LatticeSubjectSelector.Group("g"), scope, LatticeOperation.Read, effect);
}
