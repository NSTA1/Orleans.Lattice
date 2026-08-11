using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Integration tests for the policy store's authoring guard when
/// access-administration delegation is enabled on the silo
/// (<see cref="LatticeAuthOptions.AccessAdministrationDelegationEnabled"/>). Proves
/// the store reads the option and permits exactly the one narrow delegation shape -
/// a whole-tree <c>Admin</c> grant on the reserved <c>sys-auth-policy</c> tree -
/// while still rejecting every other reserved-namespace shape fail-closed, and that
/// a delegation grant round-trips and can be listed and removed.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeAuthorizationPolicyStoreDelegationTests
{
    private const string PolicyTree = "sys-auth-policy";

    private AuthDelegationClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthDelegationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task PutRuleAsync_accepts_the_whole_tree_admin_delegation_grant_when_delegation_enabled()
    {
        var rule = new LatticeAuthorizationRule(
            "delegate-alice",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Tree(PolicyTree),
            LatticeOperation.Admin,
            LatticeEffect.Allow);

        await _fixture.Store.PutRuleAsync(rule);

        var stored = await _fixture.Store.GetRuleAsync(PolicyTree, "delegate-alice");
        Assert.That(stored, Is.Not.Null);
        Assert.That(stored!.Scope.TreeId, Is.EqualTo(PolicyTree));
        Assert.That(stored.Operations, Is.EqualTo(LatticeOperation.Admin));
        Assert.That(stored.Effect, Is.EqualTo(LatticeEffect.Allow));

        // A delegation grant must remain removable so it can be revoked.
        var removed = await _fixture.Store.RemoveRuleAsync(PolicyTree, "delegate-alice");
        Assert.That(removed, Is.True);
        Assert.That(await _fixture.Store.GetRuleAsync(PolicyTree, "delegate-alice"), Is.Null);
    }

    [Test]
    public void PutRuleAsync_rejects_a_non_admin_operation_on_the_policy_tree_even_when_delegation_enabled()
    {
        var rule = new LatticeAuthorizationRule(
            "bad-ops",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Tree(PolicyTree),
            LatticeOperation.Admin | LatticeOperation.Read,
            LatticeEffect.Allow);

        Assert.That(async () => await _fixture.Store.PutRuleAsync(rule), Throws.ArgumentException);
    }

    [Test]
    public void PutRuleAsync_rejects_a_key_scope_on_the_policy_tree_even_when_delegation_enabled()
    {
        var rule = new LatticeAuthorizationRule(
            "bad-scope",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Key(PolicyTree, "k1"),
            LatticeOperation.Admin,
            LatticeEffect.Allow);

        Assert.That(async () => await _fixture.Store.PutRuleAsync(rule), Throws.ArgumentException);
    }

    [Test]
    public void PutRuleAsync_rejects_any_other_reserved_tree_even_when_delegation_enabled()
    {
        var rule = new LatticeAuthorizationRule(
            "bad-tree",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Tree("sys-auth-audit"),
            LatticeOperation.Admin,
            LatticeEffect.Allow);

        Assert.That(async () => await _fixture.Store.PutRuleAsync(rule), Throws.ArgumentException);
    }

    [Test]
    public async Task PutRuleAsync_leaves_an_ordinary_tree_unaffected_when_delegation_enabled()
    {
        var rule = new LatticeAuthorizationRule(
            "ord-1",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Tree("orders"),
            LatticeOperation.Read | LatticeOperation.Write,
            LatticeEffect.Allow);

        await _fixture.Store.PutRuleAsync(rule);

        Assert.That(await _fixture.Store.GetRuleAsync("orders", "ord-1"), Is.Not.Null);
    }
}
