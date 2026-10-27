using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// A rule a newer build persisted can carry a scope or subject-selector kind this
/// build does not define, and it reaches the policy tree by replication or restore
/// rather than through this build's store, which refuses it. The facade must treat
/// such a rule as governing nothing rather than cite it as covering the request.
/// A substitute store stands in for the policy tree holding it.
/// </summary>
[TestFixture]
public sealed class LatticeAuthAdminUnrecognisedRuleTests
{
    private const string Subject = "alice";
    private const string Tree = "orders";

    [Test]
    public async Task Explain_never_cites_a_rule_whose_own_scope_kind_is_unrecognised()
    {
        var rule = new LatticeAuthorizationRule(
            "r-unknown-scope",
            LatticeSubjectSelector.User(Subject),
            new LatticeScope((LatticeScopeKind)99, Tree),
            LatticeOperation.Read,
            LatticeEffect.Allow);

        var explanation = await CreateAdmin(rule).ExplainAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.That(explanation.MatchedRules.Select(r => r.RuleId), Does.Not.Contain("r-unknown-scope"));
    }

    [Test]
    public async Task Explain_never_cites_a_rule_whose_subject_selector_kind_is_unrecognised()
    {
        var rule = new LatticeAuthorizationRule(
            "r-unknown-selector",
            new LatticeSubjectSelector((LatticeSubjectSelectorKind)99, Subject),
            LatticeScope.Tree(Tree),
            LatticeOperation.Read,
            LatticeEffect.Allow);

        var explanation = await CreateAdmin(rule).ExplainAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.That(explanation.MatchedRules.Select(r => r.RuleId), Does.Not.Contain("r-unknown-selector"));
    }

    [Test]
    public async Task Explain_cites_a_well_formed_rule_over_the_same_substitute_store()
    {
        // Anti-vacuity: the same setup cites a rule this build understands.
        var rule = new LatticeAuthorizationRule(
            "r-known", LatticeSubjectSelector.User(Subject), LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow);

        var explanation = await CreateAdmin(rule).ExplainAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.That(explanation.MatchedRules.Select(r => r.RuleId), Does.Contain("r-known"));
    }

    private static LatticeAuthAdmin CreateAdmin(LatticeAuthorizationRule treeRule)
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.ListRulesForTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Stream());
        store.ListRulesForTreeAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(_ => Stream(treeRule));
        store.ListRulesAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream(treeRule));

        var directory = Substitute.For<ILatticeMembershipDirectory>();
        directory.GroupsOfAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyCollection<string>>([]));

        var authMonitor = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        authMonitor.CurrentValue.Returns(new LatticeAuthOptions());
        var membershipMonitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        membershipMonitor.CurrentValue.Returns(new LatticeMembershipOptions());
        var identityMonitor = Substitute.For<IOptionsMonitor<LatticeIdentityDirectoryOptions>>();
        identityMonitor.CurrentValue.Returns(new LatticeIdentityDirectoryOptions());

        return new LatticeAuthAdmin(
            store,
            directory,
            new AllowAllAccessGate(),
            new AnonymousMembershipContext(),
            Substitute.For<ILatticeIdentityDirectory>(),
            [new AnonymousCredentialAuthenticator()],
            Options.Create(new LatticeApiAuthOptions()),
            authMonitor,
            membershipMonitor,
            identityMonitor);
    }

    private static async IAsyncEnumerable<LatticeAuthorizationRule> Stream(params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            yield return rule;
            await Task.Yield();
        }
    }

    private sealed class AllowAllAccessGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Allow());
    }

    private sealed class AnonymousMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = LatticeSubject.Anonymous;
            return true;
        }
    }
}
