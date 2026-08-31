using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Coverage for the two order-sensitive helpers behind the policy-introspection
/// half of the control facade: the advisory scope-overlap test that decides which
/// stored rules are worth citing in an explanation, and the two-way merge that
/// folds the cluster-wide wildcard bucket into a per-tree rule listing.
/// </summary>
/// <remarks>
/// Scope overlap is advisory - the gate's verdict remains authoritative - but a
/// gap in it silently drops the rule that actually decided a request from the
/// operator-facing explanation, which is the failure mode this fixture pins. The
/// matrix is exercised for every combination of rule extent (whole-tree, exact
/// key, key prefix) against every target extent, including the fail-closed
/// treatment of a scope kind this build does not recognise.
/// </remarks>
public sealed partial class AuthAdminPolicyTests
{
    /// <summary>A scope carrying a kind no shipped build defines, as a forward-version payload would.</summary>
    private static LatticeScope UnknownKindScope(string treeId) => new((LatticeScopeKind)99, treeId);

    private static LatticeAuthorizationRule AllowUserPrefix(
        string ruleId, string subject, string treeId, string prefix, LatticeOperation ops) =>
        new(ruleId, LatticeSubjectSelector.User(subject), LatticeScope.Prefix(treeId, prefix), ops, LatticeEffect.Allow);

    private async Task<IReadOnlyList<string>> ExplainedRuleIdsAsync(
        string subjectId,
        LatticeOperation operation,
        LatticeScope scope,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User)
    {
        var explanation = await _fixture.Admin.ExplainAsync(subjectId, operation, scope, subjectKind);
        return explanation.MatchedRules.Select(static r => r.RuleId).ToArray();
    }

    // ----- Rule scope: exact key -----

    [Test]
    public async Task explain_of_a_whole_tree_target_cites_a_key_scoped_rule_inside_it()
    {
        const string tree = "overlap-key-vs-tree";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(AllowUserKey("r-key-tree", Subject, tree, "k1", LatticeOperation.Read));

            var cited = await ExplainedRuleIdsAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(tree));

            Assert.That(cited, Does.Contain("r-key-tree"));
        }
    }

    [Test]
    public async Task explain_of_a_prefix_target_cites_a_key_scoped_rule_under_that_prefix()
    {
        const string tree = "overlap-key-vs-prefix";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(AllowUserKey("r-key-in", Subject, tree, "orders/1", LatticeOperation.Read));
            await _fixture.Admin.PutRuleAsync(AllowUserKey("r-key-out", Subject, tree, "invoices/1", LatticeOperation.Read));

            var cited = await ExplainedRuleIdsAsync(
                Subject, LatticeOperation.Read, LatticeScope.Prefix(tree, "orders/"));

            Assert.Multiple(() =>
            {
                Assert.That(cited, Does.Contain("r-key-in"));
                Assert.That(cited, Does.Not.Contain("r-key-out"), "a key outside the prefix cannot govern it");
            });
        }
    }

    [Test]
    public async Task explain_of_an_unrecognised_target_scope_kind_cites_no_key_scoped_rule()
    {
        // Fail closed on a scope extent this build does not understand: a key rule
        // must not be presented as governing a target whose extent is unknown.
        const string tree = "overlap-key-vs-unknown";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(AllowUserKey("r-key-unknown", Subject, tree, "k1", LatticeOperation.Read));

            var cited = await ExplainedRuleIdsAsync(Subject, LatticeOperation.Read, UnknownKindScope(tree));

            Assert.That(cited, Does.Not.Contain("r-key-unknown"));
        }
    }

    // ----- Rule scope: key prefix -----

    [Test]
    public async Task explain_of_a_whole_tree_target_cites_a_prefix_scoped_rule_inside_it()
    {
        const string tree = "overlap-prefix-vs-tree";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(
                AllowUserPrefix("r-prefix-tree", Subject, tree, "orders/", LatticeOperation.Read));

            var cited = await ExplainedRuleIdsAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(tree));

            Assert.That(cited, Does.Contain("r-prefix-tree"));
        }
    }

    [Test]
    public async Task explain_of_a_key_target_cites_a_prefix_scoped_rule_covering_that_key()
    {
        const string tree = "overlap-prefix-vs-key";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(
                AllowUserPrefix("r-prefix-covers", Subject, tree, "orders/", LatticeOperation.Read));
            await _fixture.Admin.PutRuleAsync(
                AllowUserPrefix("r-prefix-misses", Subject, tree, "invoices/", LatticeOperation.Read));

            var cited = await ExplainedRuleIdsAsync(
                Subject, LatticeOperation.Read, LatticeScope.Key(tree, "orders/1"));

            Assert.Multiple(() =>
            {
                Assert.That(cited, Does.Contain("r-prefix-covers"));
                Assert.That(cited, Does.Not.Contain("r-prefix-misses"));
            });
        }
    }

    [Test]
    public async Task explain_of_a_prefix_target_cites_prefix_rules_that_overlap_in_either_direction()
    {
        // Containment is symmetric for the advisory test: a broader rule prefix
        // and a narrower one both share keys with the target range, so both are
        // worth citing. A disjoint prefix shares none and must not be.
        const string tree = "overlap-prefix-vs-prefix";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(
                AllowUserPrefix("r-prefix-broader", Subject, tree, "orders/", LatticeOperation.Read));
            await _fixture.Admin.PutRuleAsync(
                AllowUserPrefix("r-prefix-narrower", Subject, tree, "orders/2026/q1/", LatticeOperation.Read));
            await _fixture.Admin.PutRuleAsync(
                AllowUserPrefix("r-prefix-disjoint", Subject, tree, "invoices/", LatticeOperation.Read));

            var cited = await ExplainedRuleIdsAsync(
                Subject, LatticeOperation.Read, LatticeScope.Prefix(tree, "orders/2026/"));

            Assert.Multiple(() =>
            {
                Assert.That(cited, Does.Contain("r-prefix-broader"));
                Assert.That(cited, Does.Contain("r-prefix-narrower"));
                Assert.That(cited, Does.Not.Contain("r-prefix-disjoint"));
            });
        }
    }

    [Test]
    public async Task explain_of_an_unrecognised_target_scope_kind_cites_no_prefix_scoped_rule()
    {
        const string tree = "overlap-prefix-vs-unknown";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(
                AllowUserPrefix("r-prefix-unknown", Subject, tree, "orders/", LatticeOperation.Read));

            var cited = await ExplainedRuleIdsAsync(Subject, LatticeOperation.Read, UnknownKindScope(tree));

            Assert.That(cited, Does.Not.Contain("r-prefix-unknown"));
        }
    }

    // ----- Rule scope: unrecognised kind -----

    [Test]
    public async Task explain_never_cites_a_rule_whose_own_scope_kind_is_unrecognised()
    {
        // The mirror case: a rule persisted by a newer build carries an extent
        // this one cannot interpret, so it must be treated as governing nothing
        // rather than assumed to cover the request.
        const string tree = "overlap-unknown-rule";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                "r-unknown-scope",
                LatticeSubjectSelector.User(Subject),
                UnknownKindScope(tree),
                LatticeOperation.Read,
                LatticeEffect.Allow));

            var cited = await ExplainedRuleIdsAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(tree));

            Assert.That(cited, Does.Not.Contain("r-unknown-scope"));
        }
    }

    // ----- Subject selector matching -----

    [Test]
    public async Task explain_never_cites_a_rule_whose_subject_selector_kind_is_unrecognised()
    {
        // Same fail-closed posture as the scope matrix: a selector kind persisted
        // by a newer build matches nobody here, rather than being presented as
        // governing the caller.
        const string tree = "overlap-unknown-selector";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                "r-unknown-selector",
                new LatticeSubjectSelector((LatticeSubjectSelectorKind)99, Subject),
                LatticeScope.Tree(tree),
                LatticeOperation.Read,
                LatticeEffect.Allow));

            var cited = await ExplainedRuleIdsAsync(Subject, LatticeOperation.Read, LatticeScope.Tree(tree));

            Assert.That(cited, Does.Not.Contain("r-unknown-selector"));
        }
    }

    [Test]
    public async Task explain_cites_a_group_rule_for_a_user_who_is_a_member_of_that_group()
    {
        const string tree = "overlap-group-selector";
        const string group = "overlap-readers";
        const string member = "overlap-member";
        using (AsAdmin())
        {
            await _fixture.Admin.UpsertGroupAsync(new AuthGroup { GroupId = group });
            await _fixture.Admin.AddMemberAsync(group, member);
            await _fixture.Admin.PutRuleAsync(AllowGroupTree("r-group-sel", group, tree, LatticeOperation.Read));

            var citedForMember = await ExplainedRuleIdsAsync(member, LatticeOperation.Read, LatticeScope.Tree(tree));
            var citedForOutsider = await ExplainedRuleIdsAsync("overlap-outsider", LatticeOperation.Read, LatticeScope.Tree(tree));

            Assert.Multiple(() =>
            {
                Assert.That(citedForMember, Does.Contain("r-group-sel"));
                Assert.That(citedForOutsider, Does.Not.Contain("r-group-sel"), "a non-member is not governed by the group rule");
            });
        }
    }

    // ----- Wildcard-bucket merge ordering -----

    [Test]
    public async Task list_rules_for_tree_merges_a_tree_bucket_that_sorts_before_the_wildcard_bucket()
    {
        // ListRulesForTreeAsync merges the reserved "*" bucket with the tree's own
        // bucket into one catalog-key-ordered stream. Tree ids normally sort after
        // "*", so the wildcard side drains first and the merge never has to take
        // the other branch. A tree id that sorts BELOW "*" flips it, proving this
        // is a genuine two-way merge and not "wildcards first, then tree rules" -
        // which would emit a non-monotonic stream and corrupt paging.
        const string tree = "!overlap-merge-order";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(AllowUserKey("m-tree-1", Subject, tree, "k1", LatticeOperation.Read));
            await _fixture.Admin.PutRuleAsync(AllowUserKey("m-tree-2", Subject, tree, "k2", LatticeOperation.Read));
            await _fixture.Admin.PutRuleAsync(AllowUserClusterWide("m-wild-1", Subject, LatticeOperation.Read));
            await _fixture.Admin.PutRuleAsync(AllowUserClusterWide("m-wild-2", Subject, LatticeOperation.Read));

            var merged = new List<LatticeAuthorizationRule>();
            string? token = null;
            do
            {
                var page = await _fixture.Admin.ListRulesForTreeAsync(
                    tree, new AuthPageRequest { PageSize = 100, PageToken = token });
                merged.AddRange(page.Entries);
                token = page.NextPageToken;
            }
            while (token is not null);

            var ids = merged.Select(static r => r.RuleId).ToArray();
            var keys = merged.Select(static r => r.Scope.TreeId + "\u001f" + r.RuleId).ToArray();

            Assert.Multiple(() =>
            {
                Assert.That(ids, Does.Contain("m-tree-1"));
                Assert.That(ids, Does.Contain("m-tree-2"));
                Assert.That(ids, Does.Contain("m-wild-1"));
                Assert.That(ids, Does.Contain("m-wild-2"));
                Assert.That(
                    keys,
                    Is.Ordered.Using<string>(StringComparer.Ordinal),
                    "the merged stream must stay catalog-key ordered so paging advances monotonically");
            });
        }
    }
}
