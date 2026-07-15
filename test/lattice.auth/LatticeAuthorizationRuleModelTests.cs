using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the authorization rule model: the discriminated
/// <see cref="LatticeSubjectSelector"/> / <see cref="LatticeScope"/> shapes, their
/// factory methods and argument validation, and the
/// <see cref="LatticeAuthorizationRule"/> aggregate.
/// </summary>
[TestFixture]
public class LatticeAuthorizationRuleModelTests
{
    [Test]
    public void SubjectSelector_User_sets_kind_and_id()
    {
        var selector = LatticeSubjectSelector.User("alice");

        Assert.That(selector.Kind, Is.EqualTo(LatticeSubjectSelectorKind.User));
        Assert.That(selector.Id, Is.EqualTo("alice"));
    }

    [Test]
    public void SubjectSelector_Group_sets_kind_and_id()
    {
        var selector = LatticeSubjectSelector.Group("admins");

        Assert.That(selector.Kind, Is.EqualTo(LatticeSubjectSelectorKind.Group));
        Assert.That(selector.Id, Is.EqualTo("admins"));
    }

    [Test]
    public void SubjectSelector_User_with_empty_id_throws()
    {
        Assert.That(() => LatticeSubjectSelector.User(""), Throws.ArgumentException);
    }

    [Test]
    public void SubjectSelector_Group_with_null_id_throws()
    {
        Assert.That(() => LatticeSubjectSelector.Group(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Scope_Tree_sets_kind_and_tree_and_leaves_key_null()
    {
        var scope = LatticeScope.Tree("orders");

        Assert.That(scope.Kind, Is.EqualTo(LatticeScopeKind.Tree));
        Assert.That(scope.TreeId, Is.EqualTo("orders"));
        Assert.That(scope.KeyOrPrefix, Is.Null);
    }

    [Test]
    public void Scope_Key_sets_kind_tree_and_key()
    {
        var scope = LatticeScope.Key("orders", "order-42");

        Assert.That(scope.Kind, Is.EqualTo(LatticeScopeKind.Key));
        Assert.That(scope.TreeId, Is.EqualTo("orders"));
        Assert.That(scope.KeyOrPrefix, Is.EqualTo("order-42"));
    }

    [Test]
    public void Scope_Prefix_sets_kind_tree_and_prefix()
    {
        var scope = LatticeScope.Prefix("orders", "tenant-7/");

        Assert.That(scope.Kind, Is.EqualTo(LatticeScopeKind.Prefix));
        Assert.That(scope.TreeId, Is.EqualTo("orders"));
        Assert.That(scope.KeyOrPrefix, Is.EqualTo("tenant-7/"));
    }

    [Test]
    public void Scope_Tree_with_empty_tree_throws()
    {
        Assert.That(() => LatticeScope.Tree(""), Throws.ArgumentException);
    }

    [Test]
    public void Scope_Key_with_empty_key_throws()
    {
        Assert.That(() => LatticeScope.Key("orders", ""), Throws.ArgumentException);
    }

    [Test]
    public void Scope_Prefix_with_null_prefix_throws()
    {
        Assert.That(() => LatticeScope.Prefix("orders", null!), Throws.ArgumentException);
    }

    [Test]
    public void Scope_ClusterWide_is_a_whole_tree_scope_over_the_sentinel()
    {
        var scope = LatticeScope.ClusterWide();

        Assert.Multiple(() =>
        {
            Assert.That(scope.Kind, Is.EqualTo(LatticeScopeKind.Tree));
            Assert.That(scope.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
            Assert.That(scope.KeyOrPrefix, Is.Null);
        });
    }

    [Test]
    public void Scope_ClusterWide_equals_a_tree_scope_over_the_sentinel_id()
    {
        Assert.That(
            LatticeScope.ClusterWide(),
            Is.EqualTo(LatticeScope.Tree(LatticeScope.ClusterWideTreeId)));
    }

    [Test]
    public void Rule_can_carry_a_cluster_wide_telemetry_grant()
    {
        var rule = new LatticeAuthorizationRule(
            "r-telemetry",
            LatticeSubjectSelector.User("observer"),
            LatticeScope.ClusterWide(),
            LatticeOperation.Telemetry,
            LatticeEffect.Allow);

        Assert.Multiple(() =>
        {
            Assert.That(rule.Scope.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
            Assert.That(rule.Operations, Is.EqualTo(LatticeOperation.Telemetry));
            Assert.That(rule.Operations.HasFlag(LatticeOperation.Telemetry), Is.True);
        });
    }

    [Test]
    public void Scope_constructor_rejects_tree_kind_carrying_a_key()
    {
        Assert.That(
            () => new LatticeScope(LatticeScopeKind.Tree, "orders", "stray-key"),
            Throws.ArgumentException);
    }

    [Test]
    public void Scope_constructor_rejects_key_kind_without_a_key()
    {
        Assert.That(
            () => new LatticeScope(LatticeScopeKind.Key, "orders"),
            Throws.ArgumentException);
    }

    [Test]
    public void Rule_constructor_populates_every_member()
    {
        var rule = new LatticeAuthorizationRule(
            "r1",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Key("orders", "order-42"),
            LatticeOperation.Read | LatticeOperation.Write,
            LatticeEffect.Allow);

        Assert.That(rule.RuleId, Is.EqualTo("r1"));
        Assert.That(rule.Subject, Is.EqualTo(LatticeSubjectSelector.User("alice")));
        Assert.That(rule.Scope, Is.EqualTo(LatticeScope.Key("orders", "order-42")));
        Assert.That(rule.Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
        Assert.That(rule.Effect, Is.EqualTo(LatticeEffect.Allow));
        Assert.That(rule.Condition, Is.Null);
    }

    [Test]
    public void Rule_constructor_rejects_empty_rule_id()
    {
        Assert.That(
            () => new LatticeAuthorizationRule(
                "",
                LatticeSubjectSelector.User("alice"),
                LatticeScope.Tree("orders"),
                LatticeOperation.Read,
                LatticeEffect.Allow),
            Throws.ArgumentException);
    }

    [Test]
    public void Rule_constructor_rejects_null_subject()
    {
        Assert.That(
            () => new LatticeAuthorizationRule(
                "r1",
                null!,
                LatticeScope.Tree("orders"),
                LatticeOperation.Read,
                LatticeEffect.Allow),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Rule_constructor_rejects_null_scope()
    {
        Assert.That(
            () => new LatticeAuthorizationRule(
                "r1",
                LatticeSubjectSelector.User("alice"),
                null!,
                LatticeOperation.Read,
                LatticeEffect.Allow),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Operation_All_covers_every_defined_operation()
    {
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.Read), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.Write), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.Delete), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.RangeRead), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.RangeDelete), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.CrdtApply), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.AtomicWrite), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.BulkLoad), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.Admin), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.Backup), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.Restore), Is.True);
        Assert.That(LatticeAuthOperations.All.HasFlag(LatticeOperation.SchemaAdmin), Is.True);
    }
}
