using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// A subject-relative (templated) rule (issue #6061) governs a different key range
/// for each caller, so the explain facade cites it exactly when a scope it resolves
/// to for the explained subject overlaps the request, and lists it as written. A
/// substitute store stands in for the policy tree.
/// </summary>
[TestFixture]
public sealed class LatticeAuthAdminTemplatedRuleTests
{
    private const string Tree = "images";

    private static readonly LatticeAuthorizationRule OwnerRule = new(
        "owner",
        LatticeSubjectSelector.Group("members"),
        LatticeScope.Prefix(Tree, LatticeScope.SubjectPlaceholder + "/"),
        LatticeOperation.Read,
        LatticeEffect.Allow);

    [TestCase("alice", "alice/cat.jpg", ExpectedResult = true)]
    [TestCase("alice", "bob/cat.jpg", ExpectedResult = false)]
    [TestCase("alice", "alice2/cat.jpg", ExpectedResult = false)]
    public async Task<bool> Explain_cites_a_templated_rule_only_for_the_subjects_own_keys(string subject, string key)
    {
        var explanation = await CreateAdmin().ExplainAsync(subject, LatticeOperation.Read, LatticeScope.Key(Tree, key));

        return explanation.MatchedRules.Any(r => r.RuleId == "owner");
    }

    [Test]
    public async Task Explain_lists_the_templated_rule_as_written()
    {
        var explanation = await CreateAdmin().ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.That(explanation.MatchedRules.Single().Scope, Is.EqualTo(OwnerRule.Scope));
    }

    private static LatticeAuthAdmin CreateAdmin()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.ListRulesForTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Stream());
        store.ListRulesForTreeAsync(Tree, Arg.Any<CancellationToken>()).Returns(_ => Stream(OwnerRule));
        store.ListRulesAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(OwnerRule));

        var directory = Substitute.For<ILatticeMembershipDirectory>();
        directory.GroupsOfAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyCollection<string>>(["members"]));

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
