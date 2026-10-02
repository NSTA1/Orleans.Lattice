using static Orleans.Lattice.Auth.Tests.TenantLayerTestRules;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Property test for the two-layer range / scan filter (epic #4154, D8). Over many
/// generated rule sets spanning both layers - key, prefix, tree, all-trees and
/// tenant-wide scopes, user and group subjects, both effects, mixed operations,
/// both default effects and both tie-break settings - it proves, for every key of
/// a closed key universe, that:
/// <list type="number">
/// <item><description>the collection decision admits the key exactly when the point decision for it is allowed; and</description></item>
/// <item><description>
/// both equal an independent, naive oracle of the composition rule: the
/// operator-allowed keys, plus the tenant-allowed (or default-allowed) keys no
/// operator rule covers, minus the operator-denied keys.
/// </description></item>
/// </list>
/// The generator is seeded, so every run explores the same cases and a failure
/// reproduces exactly. A second property covers bounded ranges and the
/// uniform-range plain-allow shortcut (#4278) on both the operator-only and the
/// layered path.
/// </summary>
[TestFixture]
public sealed class TenantLayerRangeFilterPropertyTests
{
    private const int Cases = 600;
    private const string GroupA = "t/contoso/a";
    private const string ClusterGroup = "ops";

    private static readonly string[] KeyUniverse = BuildKeyUniverse();
    private static readonly string[] Prefixes = ["a", "b", "aa", "ab", "ba", "aab"];
    private static readonly LatticeOperation[] Operations = [LatticeOperation.RangeRead, LatticeOperation.Read, LatticeOperation.RangeRead | LatticeOperation.Read];

    [Test]
    public void Range_filter_equals_per_key_evaluation_and_the_composition_oracle_for_every_key()
    {
        var random = new Random(4160);
        var subject = Subject("alice", GroupA, ClusterGroup);

        for (var c = 0; c < Cases; c++)
        {
            var rules = GenerateRules(random);
            var options = new LatticeAuthOptions
            {
                DefaultEffect = random.Next(2) == 0 ? LatticeEffect.Deny : LatticeEffect.Allow,
                UserRuleBeatsGroupRuleAtEqualScope = random.Next(2) == 0,
                AllTreesGrantsEnabled = random.Next(3) != 0,
            };
            var policy = CompiledPolicy.Compile(rules, includeTenantLayer: true);

            var collection = PolicyEvaluator.Evaluate(
                policy, options, subject, TenantTree, LatticeOperation.RangeRead, null, null, null, tenantLayerActive: true, out _);

            foreach (var key in KeyUniverse)
            {
                var admitted = collection.KeyFilter is { } filter ? collection.Allowed && filter(key) : collection.Allowed;
                var point = PolicyEvaluator.Evaluate(
                    policy, options, subject, TenantTree, LatticeOperation.RangeRead, key, null, null, tenantLayerActive: true, out _);
                var oracle = Oracle(rules, options, subject, key);

                if (admitted != point.Allowed || point.Allowed != oracle)
                {
                    Assert.Fail(
                        $"Case {c}, key '{key}': filter={admitted}, point={point.Allowed}, oracle={oracle}; "
                        + $"default={options.DefaultEffect}, userBeatsGroup={options.UserRuleBeatsGroupRuleAtEqualScope}, "
                        + $"allTrees={options.AllTreesGrantsEnabled}; rules:{Environment.NewLine}"
                        + string.Join(Environment.NewLine, rules.Select(Describe)));
                }
            }
        }
    }

    /// <summary>
    /// The bounded-range form of the property, covering the uniform-range shortcut
    /// (#4278): a collection request whose range every operator rule governs
    /// uniformly is answered as a plain allow when that shared operator verdict
    /// allows. Every case is evaluated on both paths - with the tenant layer off
    /// (the operator-only shortcut, where an unmatched range falls to the default
    /// effect) and on (the layered shortcut, where only a matched operator allow is
    /// final) - against the same compiled snapshot. For every key inside the range
    /// it asserts filter == point decision == oracle, and that a plain (unfiltered)
    /// allow is returned only when the oracle allows every key in the range. Half
    /// the cases are seeded with an operator prefix grant exactly covering a prefix
    /// range, and with tenant denies inside it, so the shortcut demonstrably fires
    /// on both paths; the firing counts are asserted so the coverage cannot go
    /// silently vacuous.
    /// </summary>
    [Test]
    public void Bounded_range_filter_equals_point_and_oracle_on_both_paths_with_the_uniform_range_shortcut()
    {
        var random = new Random(4278);
        var subject = Subject("alice", GroupA, ClusterGroup);
        var operatorOnlyShortcuts = 0;
        var layeredShortcuts = 0;

        for (var c = 0; c < Cases; c++)
        {
            var (rangeStart, rangeEnd, rangePrefix) = GenerateRange(random);
            var rules = GenerateRules(random);
            if (rangePrefix is not null && random.Next(2) == 0)
            {
                SeedUniformOperatorGrant(rules, rangePrefix, random);
            }

            var options = new LatticeAuthOptions
            {
                DefaultEffect = random.Next(2) == 0 ? LatticeEffect.Deny : LatticeEffect.Allow,
                UserRuleBeatsGroupRuleAtEqualScope = random.Next(2) == 0,
                AllTreesGrantsEnabled = random.Next(3) != 0,
            };
            var policy = CompiledPolicy.Compile(rules, includeTenantLayer: true);
            var operatorHasPerKeyRules = policy.TryGetTree(TenantTree, out var operatorTree) && operatorTree!.HasPerKeyRules;
            var tenantBucket = policy.Tenant?.TryGetBuckets(TenantTree, out _, out _) == true;
            var operatorRules = rules.Where(r => !LatticeTenantRuleIds.IsTenantOwned(r.RuleId)).ToList();
            var inRange = KeyUniverse.Where(k => InRange(k, rangeStart, rangeEnd)).ToArray();

            foreach (var active in new[] { false, true })
            {
                var oracleRules = active ? rules : operatorRules;
                var collection = PolicyEvaluator.Evaluate(
                    policy, options, subject, TenantTree, LatticeOperation.RangeRead, null, rangeStart, rangeEnd, active, out _);
                var plainAllow = collection.Allowed && collection.KeyFilter is null;

                foreach (var key in inRange)
                {
                    var admitted = collection.KeyFilter is { } filter ? collection.Allowed && filter(key) : collection.Allowed;
                    var point = PolicyEvaluator.Evaluate(
                        policy, options, subject, TenantTree, LatticeOperation.RangeRead, key, null, null, active, out _);
                    var oracle = Oracle(oracleRules, options, subject, key);

                    if (admitted != point.Allowed || point.Allowed != oracle || (plainAllow && !oracle))
                    {
                        Assert.Fail(
                            $"Case {c} (layer {(active ? "active" : "inactive")}), range [{rangeStart ?? "(start)"}, {rangeEnd ?? "(end)"}), "
                            + $"key '{key}': filter={admitted}, point={point.Allowed}, oracle={oracle}, plainAllow={plainAllow}; "
                            + $"default={options.DefaultEffect}, userBeatsGroup={options.UserRuleBeatsGroupRuleAtEqualScope}, "
                            + $"allTrees={options.AllTreesGrantsEnabled}; rules:{Environment.NewLine}"
                            + string.Join(Environment.NewLine, rules.Select(Describe)));
                    }
                }

                // With per-key operator rules the only plain-allow exit is the
                // uniform-range shortcut, so these count its firings per path.
                if (plainAllow && operatorHasPerKeyRules)
                {
                    if (active && tenantBucket)
                    {
                        layeredShortcuts++;
                    }
                    else if (!active)
                    {
                        operatorOnlyShortcuts++;
                    }
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(operatorOnlyShortcuts, Is.GreaterThan(25), "the operator-only uniform-range shortcut must be exercised");
            Assert.That(layeredShortcuts, Is.GreaterThan(25), "the layered uniform-range shortcut must be exercised");
        });
    }

    /// <summary>
    /// Generates a collection range: the whole keyspace, a prefix range
    /// <c>[p, PrefixUpperBound(p))</c> (returned with its prefix so a case can seed
    /// a grant that covers it exactly), or an arbitrary - possibly open-ended or
    /// empty - range over the key universe.
    /// </summary>
    private static (string? Start, string? End, string? Prefix) GenerateRange(Random random)
    {
        switch (random.Next(6))
        {
            case 0:
                return (null, null, null);
            case 1:
            case 2:
            case 3:
                var prefix = random.Next(2) == 0
                    ? Prefixes[random.Next(Prefixes.Length)]
                    : KeyUniverse[random.Next(KeyUniverse.Length)];
                return (prefix, LatticeKeyRange.PrefixUpperBound(prefix), prefix);
            default:
                var start = random.Next(5) == 0 ? null : KeyUniverse[random.Next(KeyUniverse.Length)];
                var end = random.Next(5) == 0 ? null : KeyUniverse[random.Next(KeyUniverse.Length)];
                return (start, end, null);
        }
    }

    /// <summary>
    /// Adds an operator prefix allow exactly covering the prefix range, plus tenant
    /// denies inside it (a key deny, a prefix deny and a tenant-wide deny) that a
    /// matched operator allow must override, so the layered shortcut has a tenant
    /// bucket to ignore.
    /// </summary>
    private static void SeedUniformOperatorGrant(List<LatticeAuthorizationRule> rules, string prefix, Random random)
    {
        var subject = random.Next(2) == 0 ? LatticeSubjectSelector.User("alice") : LatticeSubjectSelector.Group(ClusterGroup);
        rules.Add(Operator("seed-grant", subject, LatticeScope.Prefix(TenantTree, prefix), LatticeOperation.RangeRead, LatticeEffect.Allow));

        var alice = LatticeSubjectSelector.User("alice");
        rules.Add(Tenant(Contoso, "seed-key-deny", alice, LatticeScope.Key(TenantTree, prefix), LatticeOperation.RangeRead, LatticeEffect.Deny));
        rules.Add(Tenant(Contoso, "seed-prefix-deny", alice, LatticeScope.Prefix(TenantTree, prefix), LatticeOperation.RangeRead, LatticeEffect.Deny));
        if (random.Next(2) == 0)
        {
            rules.Add(Tenant(Contoso, "seed-wide-deny", alice, LatticeScope.TenantWide(Contoso), LatticeOperation.RangeRead, LatticeEffect.Deny));
        }
    }

    private static bool InRange(string key, string? start, string? end) =>
        (start is null || string.CompareOrdinal(key, start) >= 0)
        && (end is null || string.CompareOrdinal(key, end) < 0);

    private static List<LatticeAuthorizationRule> GenerateRules(Random random)
    {
        var rules = new List<LatticeAuthorizationRule>();
        var count = random.Next(0, 9);
        for (var i = 0; i < count; i++)
        {
            var tenant = random.Next(2) == 0;
            var subject = random.Next(4) switch
            {
                0 => LatticeSubjectSelector.User("alice"),
                1 => LatticeSubjectSelector.User("bob"),
                2 => LatticeSubjectSelector.Group(ClusterGroup),
                _ => LatticeSubjectSelector.Group(tenant ? GroupA : ClusterGroup),
            };
            var ops = Operations[random.Next(Operations.Length)];
            var effect = random.Next(2) == 0 ? LatticeEffect.Allow : LatticeEffect.Deny;
            var scope = random.Next(5) switch
            {
                0 => LatticeScope.Key(TenantTree, KeyUniverse[random.Next(KeyUniverse.Length)]),
                1 or 2 => LatticeScope.Prefix(TenantTree, Prefixes[random.Next(Prefixes.Length)]),
                3 => LatticeScope.Tree(TenantTree),
                _ => tenant ? LatticeScope.TenantWide(Contoso) : LatticeScope.ClusterWide(),
            };

            rules.Add(tenant
                ? Tenant(Contoso, "r" + i, subject, scope, ops, effect)
                : Operator("r" + i, subject, scope, ops, effect));
        }

        return rules;
    }

    // ---- Independent oracle ---------------------------------------------------

    private static bool Oracle(List<LatticeAuthorizationRule> rules, LatticeAuthOptions options, LatticeSubject subject, string key)
    {
        var tenantRules = rules.Where(r => LatticeTenantRuleIds.IsTenantOwned(r.RuleId)).ToList();
        var operatorRules = rules.Except(tenantRules).ToList();
        var userBeatsGroup = options.UserRuleBeatsGroupRuleAtEqualScope;

        // Operator layer: all-trees deny, then the tree's most specific verdict, then all-trees allow.
        var allTrees = options.AllTreesGrantsEnabled
            ? Best(operatorRules.Where(r => r.Scope.TreeId == LatticeScope.ClusterWideTreeId), subject, userBeatsGroup)
            : null;
        var operatorTree = MostSpecific(operatorRules.Where(r => r.Scope.TreeId == TenantTree), subject, key, userBeatsGroup);
        var operatorVerdict = allTrees == LatticeEffect.Deny ? LatticeEffect.Deny : operatorTree ?? allTrees;
        if (operatorVerdict is { } covered)
        {
            return covered == LatticeEffect.Allow;
        }

        // Tenant layer: tenant-wide deny, then the tenant tree's verdict, then tenant-wide allow.
        var wide = Best(tenantRules.Where(r => r.Scope.IsTenantWide()), subject, userBeatsGroup);
        var tenantTree = MostSpecific(tenantRules.Where(r => r.Scope.TreeId == TenantTree), subject, key, userBeatsGroup);
        var tenantVerdict = wide == LatticeEffect.Deny ? LatticeEffect.Deny : tenantTree ?? wide;
        return (tenantVerdict ?? options.DefaultEffect) == LatticeEffect.Allow;
    }

    private static LatticeEffect? MostSpecific(
        IEnumerable<LatticeAuthorizationRule> rules, LatticeSubject subject, string key, bool userBeatsGroup)
    {
        var list = rules.ToList();
        var exact = Best(list.Where(r => r.Scope.Kind == LatticeScopeKind.Key && r.Scope.KeyOrPrefix == key), subject, userBeatsGroup);
        if (exact is not null)
        {
            return exact;
        }

        foreach (var prefix in list
            .Where(r => r.Scope.Kind == LatticeScopeKind.Prefix && key.StartsWith(r.Scope.KeyOrPrefix!, StringComparison.Ordinal))
            .Select(r => r.Scope.KeyOrPrefix!)
            .Distinct()
            .OrderByDescending(p => p.Length))
        {
            var verdict = Best(list.Where(r => r.Scope.Kind == LatticeScopeKind.Prefix && r.Scope.KeyOrPrefix == prefix), subject, userBeatsGroup);
            if (verdict is not null)
            {
                return verdict;
            }
        }

        return Best(list.Where(r => r.Scope.Kind == LatticeScopeKind.Tree), subject, userBeatsGroup);
    }

    private static LatticeEffect? Best(IEnumerable<LatticeAuthorizationRule> rules, LatticeSubject subject, bool userBeatsGroup)
    {
        var applicable = rules
            .Where(r => (r.Operations & LatticeOperation.RangeRead) == LatticeOperation.RangeRead)
            .Where(r => r.Subject.Kind == LatticeSubjectSelectorKind.User
                ? r.Subject.Id == subject.SubjectId
                : subject.GroupIds.Contains(r.Subject.Id))
            .ToList();
        if (applicable.Count == 0)
        {
            return null;
        }

        if (userBeatsGroup && applicable.Any(r => r.Subject.Kind == LatticeSubjectSelectorKind.User))
        {
            applicable = applicable.Where(r => r.Subject.Kind == LatticeSubjectSelectorKind.User).ToList();
        }

        return applicable.Any(r => r.Effect == LatticeEffect.Deny) ? LatticeEffect.Deny : LatticeEffect.Allow;
    }

    private static string[] BuildKeyUniverse()
    {
        var keys = new List<string> { "a", "b", "c" };
        foreach (var first in "abc")
        {
            foreach (var second in "abc")
            {
                keys.Add($"{first}{second}");
                foreach (var third in "ab")
                {
                    keys.Add($"{first}{second}{third}");
                }
            }
        }

        return keys.ToArray();
    }

    private static string Describe(LatticeAuthorizationRule r) =>
        $"  {r.RuleId} {r.Subject.Kind}:{r.Subject.Id} {r.Scope.Kind}:{r.Scope.TreeId}/{r.Scope.KeyOrPrefix} {r.Operations} {r.Effect}";
}
