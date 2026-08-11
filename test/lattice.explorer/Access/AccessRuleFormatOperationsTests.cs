using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Access;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit coverage for <see cref="AccessRuleFormat.Operations"/>, the canonical
/// assignable-operation list that drives every Access-area picker and label, and
/// for <see cref="AccessRuleFormat.OperationsLabel"/>, the compact label built by
/// iterating that list. The consistency guard asserts every non-<see
/// cref="LatticeOperation.None"/> flag is represented, so a future capability can
/// never again silently drop out of the pickers or labels.
/// </summary>
[TestFixture]
public sealed class AccessRuleFormatOperationsTests
{
    [Test]
    public void Operations_contains_replication()
    {
        Assert.That(
            AccessRuleFormat.Operations.Select(o => o.Flag),
            Does.Contain(LatticeOperation.Replication));
    }

    [Test]
    public void Operations_labels_replication_as_replication()
    {
        var option = AccessRuleFormat.Operations.Single(o => o.Flag == LatticeOperation.Replication);

        Assert.That(option.Label, Is.EqualTo("Replication"));
    }

    [Test]
    public void Operations_covers_every_non_none_lattice_operation_flag()
    {
        var represented = AccessRuleFormat.Operations.Select(o => o.Flag).ToHashSet();
        var expected = Enum.GetValues<LatticeOperation>()
            .Where(flag => flag != LatticeOperation.None)
            .ToArray();

        Assert.That(expected, Is.Not.Empty);
        Assert.That(represented, Is.SupersetOf(expected));
    }

    [Test]
    public void OperationsLabel_none_returns_none()
    {
        Assert.That(AccessRuleFormat.OperationsLabel(LatticeOperation.None), Is.EqualTo("none"));
    }

    [Test]
    public void OperationsLabel_replication_alone_renders_friendly_label()
    {
        Assert.That(AccessRuleFormat.OperationsLabel(LatticeOperation.Replication), Is.EqualTo("Replication"));
    }

    [Test]
    public void OperationsLabel_read_and_replication_renders_both_labels()
    {
        var label = AccessRuleFormat.OperationsLabel(LatticeOperation.Read | LatticeOperation.Replication);

        Assert.That(label, Is.EqualTo("Read, Replication"));
    }

    [Test]
    public void ScopeLabel_policy_tree_whole_tree_renders_access_administration()
    {
        var label = AccessRuleFormat.ScopeLabel(LatticeScope.Tree("sys-auth-policy"));

        Assert.That(label, Is.EqualTo("access administration"));
    }

    [Test]
    public void ScopeLabel_policy_tree_id_matches_the_reserved_constant()
    {
        var label = AccessRuleFormat.ScopeLabel(LatticeScope.Tree(LatticeAuthReservedTrees.PolicyTreeId));

        Assert.That(label, Is.EqualTo("access administration"));
    }

    [Test]
    public void ScopeLabel_ordinary_whole_tree_renders_tree()
    {
        Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Tree("orders")), Is.EqualTo("tree"));
    }

    [Test]
    public void ScopeLabel_other_reserved_tree_still_renders_tree()
    {
        // Only the policy tree is special-cased; a different reserved tree must not
        // be mislabelled as access administration.
        Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Tree("sys-auth-audit")), Is.EqualTo("tree"));
    }

    [Test]
    public void ScopeLabel_key_and_prefix_scopes_are_unchanged()
    {
        Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Key("orders", "k1")), Is.EqualTo("key 'k1'"));
        Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Prefix("orders", "p/")), Is.EqualTo("prefix 'p/'"));
    }

    [Test]
    public void ScopeLabel_key_scope_on_policy_tree_is_not_access_administration()
    {
        // The access-administration label is whole-tree only; a key scope on the
        // policy tree renders as an ordinary key scope.
        Assert.That(AccessRuleFormat.ScopeLabel(LatticeScope.Key("sys-auth-policy", "k1")), Is.EqualTo("key 'k1'"));
    }

    [Test]
    public void ScopeLabel_null_scope_throws()
    {
        Assert.That(() => AccessRuleFormat.ScopeLabel(null!), Throws.ArgumentNullException);
    }
}
