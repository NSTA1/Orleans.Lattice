using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for the policy-store authoring guard
/// <see cref="AuthConstants.EnsureAuthorableRuleScope"/> and its shape predicate
/// <see cref="AuthConstants.IsAccessAdministrationDelegationShape"/>. The guard is
/// the single seam that decides whether a rule may be persisted: an ordinary tree
/// is always authorable; the reserved <c>sys-auth-*</c> namespace is rejected
/// fail-closed except for the one access-administration delegation shape (a
/// whole-tree <see cref="LatticeOperation.Admin"/> rule on the policy tree), and
/// only when delegation is enabled. Every branch is asserted with the flag both on
/// and off so the enforcement is proven to fire and to be skippable.
/// </summary>
[TestFixture]
public sealed class AuthConstantsAuthorableRuleScopeTests
{
    private const string PolicyTree = "sys-auth-policy";

    private static LatticeAuthorizationRule Rule(
        LatticeScope scope,
        LatticeOperation operations = LatticeOperation.Admin,
        LatticeEffect effect = LatticeEffect.Allow) =>
        new("r", LatticeSubjectSelector.User("alice"), scope, operations, effect);

    // ---- Ordinary (non-reserved) trees: always authorable ----------------

    [Test]
    public void EnsureAuthorableRuleScope_ordinary_tree_is_authorable_with_delegation_off()
    {
        var rule = Rule(LatticeScope.Tree("orders"), LatticeOperation.Read | LatticeOperation.Write);

        Assert.That(() => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: false), Throws.Nothing);
    }

    [Test]
    public void EnsureAuthorableRuleScope_ordinary_tree_key_scope_is_authorable()
    {
        var rule = Rule(LatticeScope.Key("orders", "k1"), LatticeOperation.Read);

        Assert.That(() => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: true), Throws.Nothing);
    }

    // ---- The delegation shape on the policy tree -------------------------

    [Test]
    public void EnsureAuthorableRuleScope_policy_tree_whole_tree_admin_is_accepted_when_delegation_on()
    {
        var rule = Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Admin);

        Assert.That(() => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: true), Throws.Nothing);
    }

    [Test]
    public void EnsureAuthorableRuleScope_policy_tree_whole_tree_admin_deny_is_accepted_when_delegation_on()
    {
        // Effect is unconstrained for the delegation shape; a Deny grant is a valid
        // way to author a negative access-administration rule.
        var rule = Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Admin, LatticeEffect.Deny);

        Assert.That(() => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: true), Throws.Nothing);
    }

    [Test]
    public void EnsureAuthorableRuleScope_policy_tree_whole_tree_admin_is_rejected_when_delegation_off()
    {
        var rule = Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Admin);

        Assert.That(
            () => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: false),
            Throws.ArgumentException,
            "the delegation grant must be rejected fail-closed while the delegation option is off");
    }

    // ---- Wrong shapes on the policy tree: rejected even with delegation on

    [Test]
    public void EnsureAuthorableRuleScope_policy_tree_extra_operation_bits_are_rejected_even_when_delegation_on()
    {
        var rule = Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Admin | LatticeOperation.Read);

        Assert.That(
            () => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: true),
            Throws.ArgumentException,
            "only an exactly-Admin operation set is the authorable delegation shape");
    }

    [Test]
    public void EnsureAuthorableRuleScope_policy_tree_non_admin_operation_is_rejected_even_when_delegation_on()
    {
        var rule = Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Read);

        Assert.That(
            () => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: true),
            Throws.ArgumentException);
    }

    [Test]
    public void EnsureAuthorableRuleScope_policy_tree_key_scope_admin_is_rejected_even_when_delegation_on()
    {
        var rule = Rule(LatticeScope.Key(PolicyTree, "k1"), LatticeOperation.Admin);

        Assert.That(
            () => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: true),
            Throws.ArgumentException,
            "a key scope is never the authorable delegation shape");
    }

    [Test]
    public void EnsureAuthorableRuleScope_policy_tree_prefix_scope_admin_is_rejected_even_when_delegation_on()
    {
        var rule = Rule(LatticeScope.Prefix(PolicyTree, "p/"), LatticeOperation.Admin);

        Assert.That(
            () => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: true),
            Throws.ArgumentException,
            "a prefix scope is never the authorable delegation shape");
    }

    // ---- Any other reserved sys-auth-* tree: always rejected -------------

    [Test]
    public void EnsureAuthorableRuleScope_other_reserved_tree_is_rejected_regardless_of_flag()
    {
        var rule = Rule(LatticeScope.Tree("sys-auth-audit"), LatticeOperation.Admin);

        Assert.That(() => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: false), Throws.ArgumentException);
        Assert.That(() => AuthConstants.EnsureAuthorableRuleScope(rule, delegationEnabled: true), Throws.ArgumentException);
    }

    [Test]
    public void EnsureAuthorableRuleScope_null_rule_throws()
    {
        Assert.That(
            () => AuthConstants.EnsureAuthorableRuleScope(null!, delegationEnabled: true),
            Throws.ArgumentNullException);
    }

    // ---- The shape predicate --------------------------------------------

    [Test]
    public void IsAccessAdministrationDelegationShape_is_true_for_the_exact_shape()
    {
        var rule = Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Admin);

        Assert.That(AuthConstants.IsAccessAdministrationDelegationShape(rule), Is.True);
    }

    [Test]
    public void IsAccessAdministrationDelegationShape_is_false_for_extra_bits_key_scope_or_other_tree()
    {
        Assert.That(
            AuthConstants.IsAccessAdministrationDelegationShape(
                Rule(LatticeScope.Tree(PolicyTree), LatticeOperation.Admin | LatticeOperation.Write)),
            Is.False);
        Assert.That(
            AuthConstants.IsAccessAdministrationDelegationShape(
                Rule(LatticeScope.Key(PolicyTree, "k"), LatticeOperation.Admin)),
            Is.False);
        Assert.That(
            AuthConstants.IsAccessAdministrationDelegationShape(
                Rule(LatticeScope.Tree("orders"), LatticeOperation.Admin)),
            Is.False);
    }

    [Test]
    public void IsAccessAdministrationDelegationShape_null_rule_throws()
    {
        Assert.That(
            () => AuthConstants.IsAccessAdministrationDelegationShape(null!),
            Throws.ArgumentNullException);
    }
}
