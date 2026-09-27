namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Asserts that app-owned rule protection is a policy-store write-path guard only
/// and leaves data-path enforcement behaviourally unchanged: once a rule with an
/// app-owned id (<see cref="LatticeAppRuleIds.Prefix"/>) is in the compiled policy,
/// <see cref="PolicyEvaluator"/> allows and denies exactly as it does for an
/// equivalent ordinary rule, including deny-over-allow precedence between an app
/// rule and an operator-authored rule.
/// </summary>
[TestFixture]
public sealed class AppOwnedRuleEnforcementUnchangedTests
{
    private const string Tree = "a/billing/orders";
    private const string AppRuleId = LatticeAppRuleIds.Prefix + "billing/reader/orders";
    private const string OrdinaryRuleId = "ops-orders-read";

    private static LatticeAuthorizationRule Rule(string ruleId, LatticeEffect effect, LatticeOperation ops = LatticeOperation.Read) =>
        new(ruleId, LatticeSubjectSelector.Group("billing-readers"), LatticeScope.Tree(Tree), ops, effect);

    private static bool Allowed(IEnumerable<LatticeAuthorizationRule> rules, LatticeSubject subject, LatticeOperation operation) =>
        PolicyEvaluator.Evaluate(CompiledPolicy.Compile(rules), new LatticeAuthOptions(), subject, Tree, operation, "k", null, null).Allowed;

    private static readonly LatticeSubject Member = new("alice", new[] { "billing-readers" });
    private static readonly LatticeSubject Outsider = new("bob", null);

    [TestCase(LatticeEffect.Allow, LatticeOperation.Read)]
    [TestCase(LatticeEffect.Allow, LatticeOperation.Write)]
    [TestCase(LatticeEffect.Deny, LatticeOperation.Read)]
    [TestCase(LatticeEffect.Deny, LatticeOperation.Write)]
    public void Evaluate_an_app_owned_rule_decides_exactly_as_an_equivalent_ordinary_rule(LatticeEffect effect, LatticeOperation operation)
    {
        var app = new[] { Rule(AppRuleId, effect, LatticeOperation.Read) };
        var ordinary = new[] { Rule(OrdinaryRuleId, effect, LatticeOperation.Read) };

        foreach (var subject in new[] { Member, Outsider })
        {
            Assert.That(
                Allowed(app, subject, operation),
                Is.EqualTo(Allowed(ordinary, subject, operation)),
                $"subject {subject.SubjectId}, effect {effect}, operation {operation}");
        }
    }

    [Test]
    public void Evaluate_an_app_owned_allow_grants_its_bound_group_and_nobody_else()
    {
        var rules = new[] { Rule(AppRuleId, LatticeEffect.Allow) };

        Assert.That(Allowed(rules, Member, LatticeOperation.Read), Is.True);
        Assert.That(Allowed(rules, Outsider, LatticeOperation.Read), Is.False);
    }

    [Test]
    public void Evaluate_an_operator_deny_overrides_an_app_owned_allow_at_equal_scope()
    {
        // The documented remedy for narrowing an app's grant is a separate operator
        // rule; deny-over-allow must apply to app rules like any other.
        var rules = new[]
        {
            Rule(AppRuleId, LatticeEffect.Allow),
            Rule(OrdinaryRuleId, LatticeEffect.Deny),
        };

        Assert.That(Allowed(rules, Member, LatticeOperation.Read), Is.False);
    }

    [Test]
    public void Evaluate_an_app_owned_deny_overrides_an_operator_allow_at_equal_scope()
    {
        var rules = new[]
        {
            Rule(OrdinaryRuleId, LatticeEffect.Allow),
            Rule(AppRuleId, LatticeEffect.Deny),
        };

        Assert.That(Allowed(rules, Member, LatticeOperation.Read), Is.False);
    }
}
