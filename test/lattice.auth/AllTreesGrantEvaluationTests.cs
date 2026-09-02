using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the all-trees (<c>Tree:*</c>) authorization tier promoted by
/// <see cref="LatticeAuthOptions.AllTreesGrantsEnabled"/>. Exercises the four-tier
/// precedence (all-trees deny wins outright, then the specific tree, then an
/// all-trees allow, then the default effect) for both point and range requests,
/// with the flag on and off, plus the fail-closed system-tree / sentinel
/// exclusion, operation-bit separation, existence-hiding, and the all-trees match
/// / reason labelling. Evaluated directly against
/// <see cref="CompiledPolicy"/> + <see cref="PolicyEvaluator"/> without a
/// maintainer, snapshot swap, or cluster.
/// </summary>
[TestFixture]
public sealed class AllTreesGrantEvaluationTests
{
    private const string Tree = "app-tree";
    private const string ReservedTree = "sys-auth-policy";
    private const string Sentinel = "*"; // LatticeScope.ClusterWideTreeId

    private static LatticeAuthorizationRule User(string id, string user, LatticeScope scope, LatticeOperation ops, LatticeEffect effect) =>
        new(id, LatticeSubjectSelector.User(user), scope, ops, effect);

    private static LatticeAuthorizationRule Group(string id, string group, LatticeScope scope, LatticeOperation ops, LatticeEffect effect) =>
        new(id, LatticeSubjectSelector.Group(group), scope, ops, effect);

    private static LatticeSubject Subject(string id, params string[] groups) =>
        new(id, groups.Length == 0 ? null : groups);

    private static LatticeAuthOptions Enabled() => new() { AllTreesGrantsEnabled = true };

    private static LatticeAccessDecision Eval(
        IEnumerable<LatticeAuthorizationRule> rules,
        LatticeSubject subject,
        LatticeOperation operation,
        LatticeAuthOptions options,
        string treeId = Tree,
        string? key = "k",
        string? rangeStart = null,
        string? rangeEnd = null)
    {
        var policy = CompiledPolicy.Compile(rules);
        return PolicyEvaluator.Evaluate(policy, options, subject, treeId, operation, key, rangeStart, rangeEnd);
    }

    private static PolicyMatch EvalMatch(
        IEnumerable<LatticeAuthorizationRule> rules,
        LatticeSubject subject,
        LatticeOperation operation,
        LatticeAuthOptions options,
        string treeId = Tree,
        string? key = "k")
    {
        var policy = CompiledPolicy.Compile(rules);
        PolicyEvaluator.Evaluate(policy, options, subject, treeId, operation, key, null, null, out var match);
        return match;
    }

    // ---- Flag default / inert-when-off ----------------------------------

    [Test]
    public void AllTreesGrantsEnabled_defaults_to_false()
    {
        Assert.That(new LatticeAuthOptions().AllTreesGrantsEnabled, Is.False);
    }

    [Test]
    public void Evaluate_all_trees_allow_is_inert_when_flag_is_off()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow) };

        // Default-deny + inert wildcard -> denied, exactly as before the feature.
        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, new LatticeAuthOptions()).Allowed, Is.False);
    }

    [Test]
    public void Evaluate_all_trees_deny_is_inert_when_flag_is_off()
    {
        var rules = new[]
        {
            User("all-deny", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Deny),
            User("tree-allow", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow),
        };

        // With the flag off the wildcard deny does not fire; the specific allow stands.
        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, new LatticeAuthOptions()).Allowed, Is.True);
    }

    // ---- The four intent rows (point) -----------------------------------

    [Test]
    public void Evaluate_global_allow_no_specific_allows()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow) };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, Enabled()).Allowed, Is.True);
    }

    [Test]
    public void Evaluate_global_deny_plus_specific_allow_denies()
    {
        var rules = new[]
        {
            User("all-deny", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Deny),
            User("tree-allow", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, Enabled()).Allowed, Is.False,
            "an all-trees deny is never overridden by a specific-tree allow");
    }

    [Test]
    public void Evaluate_global_allow_plus_specific_deny_denies()
    {
        var rules = new[]
        {
            User("all-allow", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow),
            User("tree-deny", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, Enabled()).Allowed, Is.False,
            "a specific-tree deny overrides a global allow");
    }

    [Test]
    public void Evaluate_specific_allow_plus_global_allow_allows()
    {
        var rules = new[]
        {
            User("all-allow", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow),
            User("tree-allow", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, Enabled()).Allowed, Is.True);
    }

    [Test]
    public void Evaluate_global_allow_plus_specific_key_deny_denies_that_key_only()
    {
        var rules = new[]
        {
            User("all-allow", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow),
            User("key-deny", "alice", LatticeScope.Key(Tree, "secret"), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, Enabled(), key: "secret").Allowed, Is.False);
        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, Enabled(), key: "open").Allowed, Is.True);
    }

    [Test]
    public void Evaluate_all_trees_grant_matches_a_group_member()
    {
        var rules = new[] { Group("all", "admins", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow) };

        Assert.That(Eval(rules, Subject("alice", "admins"), LatticeOperation.Read, Enabled()).Allowed, Is.True);
        Assert.That(Eval(rules, Subject("bob"), LatticeOperation.Read, Enabled()).Allowed, Is.False);
    }

    // ---- Range / per-key -------------------------------------------------

    [Test]
    public void Evaluate_all_trees_allow_is_uniform_across_a_range()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.RangeRead, LatticeEffect.Allow) };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.RangeRead, Enabled(), key: null, rangeStart: "a", rangeEnd: "z");

        Assert.That(decision.Allowed, Is.True);
        Assert.That(decision.KeyFilter, Is.Null, "a uniform all-trees allow needs no per-key filter");
    }

    [Test]
    public void Evaluate_all_trees_deny_prunes_every_key_of_a_range_with_per_key_rules()
    {
        var rules = new[]
        {
            // Per-key rule on the tree forces the Filtered path; the all-trees deny
            // must then prune every candidate key regardless of the per-key allow.
            User("open", "alice", LatticeScope.Key(Tree, "open"), LatticeOperation.RangeRead, LatticeEffect.Allow),
            User("all-deny", "alice", LatticeScope.ClusterWide(), LatticeOperation.RangeRead, LatticeEffect.Deny),
        };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.RangeRead, Enabled(), key: null, rangeStart: "a", rangeEnd: "z");

        Assert.That(decision.KeyFilter, Is.Not.Null);
        Assert.That(decision.KeyFilter!("open"), Is.False, "the all-trees deny overrides the per-key allow");
        Assert.That(decision.KeyFilter!("other"), Is.False);
    }

    [Test]
    public void Evaluate_range_per_key_specific_deny_overrides_all_trees_allow()
    {
        var rules = new[]
        {
            User("all-allow", "alice", LatticeScope.ClusterWide(), LatticeOperation.RangeRead, LatticeEffect.Allow),
            User("secret", "alice", LatticeScope.Key(Tree, "secret"), LatticeOperation.RangeRead, LatticeEffect.Deny),
        };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.RangeRead, Enabled(), key: null, rangeStart: "a", rangeEnd: "z");

        Assert.That(decision.KeyFilter, Is.Not.Null);
        Assert.That(decision.KeyFilter!("open"), Is.True, "the all-trees allow admits a key with no specific rule");
        Assert.That(decision.KeyFilter!("secret"), Is.False, "the specific key deny overrides the all-trees allow");
    }

    [Test]
    public void Evaluate_range_filter_agrees_with_point_decision_under_all_trees_tier()
    {
        var rules = new[]
        {
            User("all-allow", "alice", LatticeScope.ClusterWide(), LatticeOperation.RangeRead, LatticeEffect.Allow),
            User("blocked", "alice", LatticeScope.Prefix(Tree, "no/"), LatticeOperation.RangeRead, LatticeEffect.Deny),
        };
        var policy = CompiledPolicy.Compile(rules);
        var options = Enabled();
        var subject = Subject("alice");

        var range = PolicyEvaluator.Evaluate(policy, options, subject, Tree, LatticeOperation.RangeRead, null, null, null);

        Assert.That(range.KeyFilter, Is.Not.Null);
        foreach (var key in new[] { "yes", "no/x", "no/", "other" })
        {
            var point = PolicyEvaluator.Evaluate(policy, options, subject, Tree, LatticeOperation.RangeRead, key, null, null);
            Assert.That(range.KeyFilter!(key), Is.EqualTo(point.Allowed), $"filter and point must agree for '{key}'");
        }
    }

    // ---- Fail-closed system / sentinel exclusion ------------------------

    [Test]
    public void Evaluate_all_trees_tier_never_applies_to_a_reserved_tree()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Admin, LatticeEffect.Allow) };

        // A wildcard Admin allow must NOT authorize the reserved policy tree, or a
        // wildcard data grant would reach the control plane.
        var decision = Eval(rules, Subject("alice"), LatticeOperation.Admin, Enabled(), treeId: ReservedTree, key: null);

        Assert.That(decision.Allowed, Is.False, "the reserved namespace is excluded from the all-trees tier");
    }

    [Test]
    public void Evaluate_all_trees_tier_never_applies_to_the_tenant_admin_capability_namespace()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Admin, LatticeEffect.Allow) };

        // The delegated per-tenant administration capability is modelled as Admin on a
        // reserved id in its own namespace, which the gate routes to the fail-closed
        // control plane. That namespace starts with neither the reserved policy-tree id
        // nor the tenant-registry prefix, so it has to be excluded from the all-trees
        // tier explicitly - otherwise a cluster-wide data grant would silently confer
        // delegated administration over every tenant.
        var scope = LatticeTenantAdminScope.ForTenant(TenantId.Parse("acme")).TreeScope;

        var decision = Eval(rules, Subject("alice"), LatticeOperation.Admin, Enabled(), treeId: scope, key: null);

        Assert.That(decision.Allowed, Is.False, "the tenant-admin capability namespace is excluded from the all-trees tier");
    }

    [Test]
    public void Evaluate_all_trees_tier_never_applies_to_any_tenant_admin_capability_id()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Admin, LatticeEffect.Allow) };

        // The exclusion is by namespace prefix, not by a per-tenant enumeration, so it
        // holds for a tenant the policy has never heard of.
        foreach (var tenant in new[] { "acme", "beta", "tenant-the-policy-never-saw" })
        {
            var scope = LatticeTenantAdminScope.ForTenant(TenantId.Parse(tenant)).TreeScope;

            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Admin, Enabled(), treeId: scope, key: null).Allowed,
                Is.False, $"'{tenant}' administration is not conferred by a wildcard grant");
        }
    }

    [Test]
    public void Tenant_admin_capability_prefix_is_not_covered_by_the_reserved_tree_exclusion()
    {
        // Drift guard for the reason the dedicated exclusion is needed at all: a
        // tenant-admin capability id is not itself a reserved sys-auth- id, so the
        // reserved-tree test alone never covered it. If this ever starts failing the
        // dedicated exclusion has become redundant; while it passes, removing that
        // exclusion silently reopens the gap.
        var scope = LatticeTenantAdminScope.ForTenant(TenantId.Parse("acme")).TreeScope;

        Assert.That(LatticeAuthReservedTrees.IsReserved(scope), Is.False);
    }

    [Test]
    public void Evaluate_request_on_the_sentinel_resolves_its_own_bucket_without_double_fold()
    {
        // A rule scoped Tree:* granting Telemetry, and a request literally targeting
        // "*" for Telemetry, must resolve exactly as today - the sentinel's own
        // bucket - with no second all-trees fold layered on.
        var rules = new[] { User("tel", "alice", LatticeScope.ClusterWide(), LatticeOperation.Telemetry, LatticeEffect.Allow) };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.Telemetry, Enabled(), treeId: Sentinel, key: null);

        Assert.That(decision.Allowed, Is.True, "a literal telemetry request on the sentinel resolves against its own bucket");
    }

    // ---- Operation-bit separation ---------------------------------------

    [Test]
    public void Evaluate_data_plane_all_trees_grant_never_confers_telemetry()
    {
        var rules = new[] { User("all-read", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow) };

        // A telemetry request literally targets the sentinel; the data-plane Read
        // grant on the sentinel must not confer Telemetry.
        var decision = Eval(rules, Subject("alice"), LatticeOperation.Telemetry, Enabled(), treeId: Sentinel, key: null);

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public void Evaluate_telemetry_all_trees_grant_never_confers_a_data_plane_op()
    {
        var rules = new[] { User("all-tel", "alice", LatticeScope.ClusterWide(), LatticeOperation.Telemetry, LatticeEffect.Allow) };

        // A data-plane Read on an ordinary tree must not be granted by a telemetry
        // wildcard grant, even with the tier enabled.
        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, Enabled()).Allowed, Is.False);
    }

    [Test]
    public void Evaluate_all_trees_admin_grant_never_confers_tree_lifecycle()
    {
        var rules = new[] { User("all-admin", "alice", LatticeScope.ClusterWide(), LatticeOperation.Admin, LatticeEffect.Allow) };

        // A cluster-wide Admin allow must NOT authorize a destructive / structural
        // TreeLifecycle operation on an application tree: routine administration
        // never silently confers the authority to drop / reshard / resize / move.
        var decision = Eval(rules, Subject("alice"), LatticeOperation.TreeLifecycle, Enabled(), key: null);

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public void Evaluate_all_trees_tree_lifecycle_grant_authorizes_tree_lifecycle()
    {
        var rules = new[] { User("all-life", "alice", LatticeScope.ClusterWide(), LatticeOperation.TreeLifecycle, LatticeEffect.Allow) };

        // A deliberate cluster-wide TreeLifecycle allow authorizes the structural
        // whole-tree operation - the capability stays expressible, just never as a
        // silent rider on Admin.
        var decision = Eval(rules, Subject("alice"), LatticeOperation.TreeLifecycle, Enabled(), key: null);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public void Evaluate_all_trees_tree_lifecycle_grant_never_confers_admin()
    {
        var rules = new[] { User("all-life", "alice", LatticeScope.ClusterWide(), LatticeOperation.TreeLifecycle, LatticeEffect.Allow) };

        // Conversely the structural grant confers nothing else: a routine Admin
        // request is not satisfied by a TreeLifecycle wildcard grant.
        var decision = Eval(rules, Subject("alice"), LatticeOperation.Admin, Enabled(), key: null);

        Assert.That(decision.Allowed, Is.False);
    }

    // ---- Match / reason labelling ---------------------------------------

    [Test]
    public void Evaluate_all_trees_deny_labels_the_match_and_reason_as_all_trees()
    {
        var rules = new[] { User("all-deny", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Deny) };

        var match = EvalMatch(rules, Subject("alice"), LatticeOperation.Read, Enabled());
        var decision = Eval(rules, Subject("alice"), LatticeOperation.Read, Enabled());

        Assert.That(match.Matched, Is.True);
        Assert.That(match.AllTrees, Is.True, "the winning match is flagged as originating from the all-trees tier");
        Assert.That(match.RuleId, Is.EqualTo("all-deny"));
        Assert.That(decision.Reason, Does.Contain("all trees"));
        Assert.That(decision.Reason, Does.Not.Contain("(tree scope)"));
    }

    [Test]
    public void Evaluate_all_trees_allow_flags_the_match()
    {
        var rules = new[] { User("all-allow", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow) };

        var match = EvalMatch(rules, Subject("alice"), LatticeOperation.Read, Enabled());

        Assert.That(match.Matched, Is.True);
        Assert.That(match.AllTrees, Is.True);
        Assert.That(match.Effect, Is.EqualTo(LatticeEffect.Allow));
    }

    [Test]
    public void Evaluate_specific_rule_winning_over_all_trees_is_not_flagged_all_trees()
    {
        var rules = new[]
        {
            User("all-allow", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow),
            User("tree-deny", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Deny),
        };

        var match = EvalMatch(rules, Subject("alice"), LatticeOperation.Read, Enabled());

        Assert.That(match.Matched, Is.True);
        Assert.That(match.AllTrees, Is.False, "the specific-tree rule won, so the match is not an all-trees match");
        Assert.That(match.RuleId, Is.EqualTo("tree-deny"));
    }

    // ---- HasAnyGrant / existence-hiding ---------------------------------

    [Test]
    public void HasAnyGrant_counts_an_all_trees_allow_when_enabled()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow) };
        var policy = CompiledPolicy.Compile(rules);

        Assert.That(PolicyEvaluator.HasAnyGrant(policy, Enabled(), Subject("alice"), Tree, LatticeOperation.Read), Is.True);
    }

    [Test]
    public void HasAnyGrant_ignores_an_all_trees_allow_when_flag_is_off()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow) };
        var policy = CompiledPolicy.Compile(rules);

        Assert.That(PolicyEvaluator.HasAnyGrant(policy, new LatticeAuthOptions(), Subject("alice"), Tree, LatticeOperation.Read), Is.False);
    }

    [Test]
    public void HasAnyGrant_ignores_an_all_trees_allow_for_a_reserved_tree()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Admin, LatticeEffect.Allow) };
        var policy = CompiledPolicy.Compile(rules);

        Assert.That(PolicyEvaluator.HasAnyGrant(policy, Enabled(), Subject("alice"), ReservedTree, LatticeOperation.Admin), Is.False);
    }

    [Test]
    public void HasAnyGrant_ignores_an_all_trees_allow_for_the_tenant_admin_capability_namespace()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Admin, LatticeEffect.Allow) };
        var policy = CompiledPolicy.Compile(rules);
        var scope = LatticeTenantAdminScope.ForTenant(TenantId.Parse("acme")).TreeScope;

        // The existence-hiding probe must agree with the enforcement decision, or a
        // wildcard grant leaks the shape of the control plane it cannot act on.
        Assert.That(PolicyEvaluator.HasAnyGrant(policy, Enabled(), Subject("alice"), scope, LatticeOperation.Admin), Is.False);
    }

    [Test]
    public void HasAnyGrant_all_trees_deny_does_not_hide_a_tree_with_a_specific_allow()
    {
        var rules = new[]
        {
            User("all-deny", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Deny),
            User("tree-allow", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow),
        };
        var policy = CompiledPolicy.Compile(rules);

        // HasAnyGrant is a pure "any resolved allow" signal: the specific allow keeps
        // the tree visible even though a real read would be denied by the wildcard.
        Assert.That(PolicyEvaluator.HasAnyGrant(policy, Enabled(), Subject("alice"), Tree, LatticeOperation.Read), Is.True);
    }

    // ---- Compiled snapshot -----------------------------------------------

    [Test]
    public void Compile_exposes_the_all_trees_bucket()
    {
        var rules = new[] { User("all", "alice", LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow) };

        Assert.That(CompiledPolicy.Compile(rules).AllTrees, Is.Not.Null);
    }

    [Test]
    public void Compile_has_no_all_trees_bucket_when_no_wildcard_rule_exists()
    {
        var rules = new[] { User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow) };

        Assert.That(CompiledPolicy.Compile(rules).AllTrees, Is.Null);
    }

    [Test]
    public void Empty_snapshot_has_no_all_trees_bucket()
    {
        Assert.That(CompiledPolicy.Empty.AllTrees, Is.Null);
    }
}
