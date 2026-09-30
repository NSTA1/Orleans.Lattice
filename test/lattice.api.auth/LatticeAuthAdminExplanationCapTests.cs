using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Unit coverage for the two bounding behaviours the policy-introspection reads
/// apply while assembling their citation lists: the
/// <see cref="LatticeApiAuthOptions.MaxExplanationRules"/> cap that stops a scan
/// before it can enumerate an unbounded rule set, and the composite-key dedup
/// that keeps a rule cited once even when the store yields it twice.
/// <para>
/// Both are unreachable from the cluster fixtures: every one of them configures a
/// sane cap that its handful of seeded rules never approaches, and a real store
/// never yields the same rule twice within a scan. Instantiating
/// <see cref="LatticeAuthAdmin"/> directly over a substitute
/// <see cref="ILatticeAuthorizationPolicyStore"/> lets the cap be set to a value
/// the rule set does exceed, and lets the duplicate-yield case be produced
/// deliberately rather than waited for.
/// </para>
/// <para>
/// Every capping assertion is paired with an uncapped counterpart over the same
/// rule set, so a facade that collected nothing - or that always truncated -
/// fails one of the pair rather than passing both.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeAuthAdminExplanationCapTests
{
    private const string Subject = "alice";
    private const string Tree = "orders";

    private static LatticeAuthorizationRule Rule(string ruleId, LatticeScope? scope = null) =>
        new(
            ruleId,
            LatticeSubjectSelector.User(Subject),
            scope ?? LatticeScope.Tree(Tree),
            LatticeOperation.Read,
            LatticeEffect.Allow);

    private static async IAsyncEnumerable<LatticeAuthorizationRule> Stream(
        params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            yield return rule;
            await Task.Yield();
        }
    }

    /// <summary>
    /// Builds a facade over a substitute store. <paramref name="treeRules"/> answers
    /// the target tree's scan and <paramref name="wildcardRules"/> the cluster-wide
    /// bucket's, so a test can place the same rule in both and observe the dedup.
    /// The membership directory is configured to return an empty group closure
    /// explicitly: an unconfigured substitute yields a null collection, which the
    /// subject resolver would carry into <see cref="LatticeSubject"/>.
    /// </summary>
    private static LatticeAuthAdmin CreateAdmin(
        int maxExplanationRules,
        LatticeAuthorizationRule[] treeRules,
        LatticeAuthorizationRule[]? wildcardRules = null)
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.ListRulesForTreeAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(_ => Stream(treeRules));
        store.ListRulesForTreeAsync(LatticeScope.ClusterWideTreeId, Arg.Any<CancellationToken>())
            .Returns(_ => Stream(wildcardRules ?? []));
        store.ListRulesAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Stream([.. treeRules, .. wildcardRules ?? []]));

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
            Options.Create(new LatticeApiAuthOptions { MaxExplanationRules = maxExplanationRules }),
            authMonitor,
            membershipMonitor,
            identityMonitor);
    }

    private static LatticeAuthorizationRule[] FiveMatchingRules() =>
        [Rule("r-1"), Rule("r-2"), Rule("r-3"), Rule("r-4"), Rule("r-5")];

    // ----- EffectivePermissionsAsync: the all-trees scan cap -----

    [Test]
    public async Task EffectivePermissionsAsync_stops_collecting_at_the_explanation_cap()
    {
        var admin = CreateAdmin(maxExplanationRules: 2, treeRules: FiveMatchingRules());

        var result = await admin.EffectivePermissionsAsync(Subject);

        Assert.That(
            result.Rules,
            Has.Count.EqualTo(2),
            "the scan must stop at the cap rather than enumerate every matching rule");
    }

    [Test]
    public async Task EffectivePermissionsAsync_collects_every_matching_rule_below_the_cap()
    {
        // Anti-vacuity for the test above: the same five rules, a cap they do not
        // reach. A facade that collected nothing would pass the capped assertion
        // only if the cap happened to be zero, and fails here outright.
        var admin = CreateAdmin(maxExplanationRules: 100, treeRules: FiveMatchingRules());

        var result = await admin.EffectivePermissionsAsync(Subject);

        Assert.That(result.Rules, Has.Count.EqualTo(5));
    }

    [Test]
    public async Task EffectivePermissionsAsync_returns_no_rules_when_the_cap_is_zero()
    {
        // The degenerate operator value: a zero cap trips the guard on the very
        // first rule, before any selector match is attempted.
        var admin = CreateAdmin(maxExplanationRules: 0, treeRules: FiveMatchingRules());

        var result = await admin.EffectivePermissionsAsync(Subject);

        Assert.That(result.Rules, Is.Empty);
    }

    // ----- ExplainAsync: the per-tree collection cap -----

    [Test]
    public async Task ExplainAsync_stops_citing_at_the_explanation_cap()
    {
        var admin = CreateAdmin(maxExplanationRules: 2, treeRules: FiveMatchingRules());

        var explanation = await admin.ExplainAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.That(
            explanation.MatchedRules,
            Has.Count.EqualTo(2),
            "the per-tree collection must stop at the cap");
    }

    [Test]
    public async Task ExplainAsync_cites_every_matching_rule_below_the_cap()
    {
        var admin = CreateAdmin(maxExplanationRules: 100, treeRules: FiveMatchingRules());

        var explanation = await admin.ExplainAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.That(explanation.MatchedRules, Has.Count.EqualTo(5));
    }

    [Test]
    public async Task ExplainAsync_applies_the_cap_across_both_the_tree_and_wildcard_scans()
    {
        // The cap is shared state across the two collection passes, so a wildcard
        // bucket cannot top the list back up once the target tree has filled it.
        var admin = CreateAdmin(
            maxExplanationRules: 3,
            treeRules: FiveMatchingRules(),
            wildcardRules: [Rule("w-1", LatticeScope.ClusterWide())]);

        var explanation = await admin.ExplainAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(explanation.MatchedRules, Has.Count.EqualTo(3));
            Assert.That(
                explanation.MatchedRules.Select(r => r.RuleId),
                Does.Not.Contain("w-1"),
                "the wildcard pass must observe the already-full citation list");
        });
    }

    // ----- ExplainAsync: composite-key deduplication -----

    [Test]
    public async Task ExplainAsync_cites_a_rule_once_when_the_store_yields_it_twice()
    {
        // A store that yields the same rule twice within one scan must not produce
        // two citations of it.
        var duplicated = Rule("r-dup");
        var admin = CreateAdmin(maxExplanationRules: 100, treeRules: [duplicated, duplicated]);

        var explanation = await admin.ExplainAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.That(
            explanation.MatchedRules.Select(r => r.RuleId),
            Is.EqualTo(new[] { "r-dup" }),
            "the composite-key dedup must collapse a duplicated yield to one citation");
    }

    [Test]
    public async Task ExplainAsync_cites_a_shared_rule_id_from_two_trees_separately()
    {
        // Anti-vacuity for the dedup above, and the reason it keys on the composite
        // (tree id plus rule id): a rule id is only unique within its own tree, so a
        // target-tree rule and a cluster-wide rule that share one are two distinct
        // citations and must both survive.
        var admin = CreateAdmin(
            maxExplanationRules: 100,
            treeRules: [Rule("shared")],
            wildcardRules: [Rule("shared", LatticeScope.ClusterWide())]);

        var explanation = await admin.ExplainAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(explanation.MatchedRules, Has.Count.EqualTo(2));
            Assert.That(
                explanation.MatchedRules.Select(r => r.Scope.TreeId),
                Is.EquivalentTo(new[] { Tree, LatticeScope.ClusterWideTreeId }));
        });
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
