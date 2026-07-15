using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the compiled-policy decision algorithm
/// (<see cref="CompiledPolicy"/> + <see cref="PolicyEvaluator"/>), exercised
/// directly without a maintainer, snapshot swap, or cluster. Covers precedence
/// (deny-over-allow at equal specificity), scope specificity
/// (exact key &gt; longer prefix &gt; shorter prefix &gt; tree), longest-prefix
/// match, user-over-group at equal scope (and the toggle off), operation-bitset
/// matching, group-membership matching against the subject's flat group closure,
/// range-read key-filter correctness, and the default effect.
/// </summary>
[TestFixture]
public sealed class CompiledPolicyEvaluationTests
{
    private const string Tree = "t";

    private static LatticeAuthorizationRule User(string id, string user, LatticeScope scope, LatticeOperation ops, LatticeEffect effect) =>
        new(id, LatticeSubjectSelector.User(user), scope, ops, effect);

    private static LatticeAuthorizationRule Group(string id, string group, LatticeScope scope, LatticeOperation ops, LatticeEffect effect) =>
        new(id, LatticeSubjectSelector.Group(group), scope, ops, effect);

    private static LatticeAccessDecision Eval(
        IEnumerable<LatticeAuthorizationRule> rules,
        LatticeSubject subject,
        LatticeOperation operation,
        string? key = null,
        string? rangeStart = null,
        string? rangeEnd = null,
        LatticeAuthOptions? options = null)
    {
        var policy = CompiledPolicy.Compile(rules);
        return PolicyEvaluator.Evaluate(policy, options ?? new LatticeAuthOptions(), subject, Tree, operation, key, rangeStart, rangeEnd);
    }

    private static LatticeSubject Subject(string id, params string[] groups) =>
        new(id, groups.Length == 0 ? null : groups);

    // ---- Default effect --------------------------------------------------

    [Test]
    public void Evaluate_no_rules_point_read_denies_by_default()
    {
        var decision = Eval(Array.Empty<LatticeAuthorizationRule>(), Subject("alice"), LatticeOperation.Read, key: "k");

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Evaluate_no_rules_point_read_allows_when_default_effect_is_allow()
    {
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow };

        var decision = Eval(Array.Empty<LatticeAuthorizationRule>(), Subject("alice"), LatticeOperation.Read, key: "k", options: options);

        Assert.That(decision.Allowed, Is.True);
        Assert.That(decision.Reason, Is.Null, "a plain allow uses the allocation-free cached decision with no reason");
    }

    // ---- Tree scope + subject matching ----------------------------------

    [Test]
    public void Evaluate_tree_allow_grants_the_named_user_only()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow) };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "k").Allowed, Is.True);
        Assert.That(Eval(rules, Subject("bob"), LatticeOperation.Read, key: "k").Allowed, Is.False);
    }

    [Test]
    public void Evaluate_group_rule_matches_a_member_of_the_group_closure()
    {
        var rules = new[] { Group("r", "admins", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow) };

        Assert.That(Eval(rules, Subject("alice", "admins"), LatticeOperation.Read, key: "k").Allowed, Is.True);
        Assert.That(Eval(rules, Subject("alice", "readers"), LatticeOperation.Read, key: "k").Allowed, Is.False);
        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "k").Allowed, Is.False);
    }

    [Test]
    public void Evaluate_group_membership_matches_when_closure_is_a_set()
    {
        var rules = new[] { Group("r", "admins", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow) };
        var subject = new LatticeSubject("alice", new HashSet<string>(StringComparer.Ordinal) { "admins", "x" });

        Assert.That(Eval(rules, subject, LatticeOperation.Read, key: "k").Allowed, Is.True);
    }

    // ---- Precedence: deny over allow at equal specificity ---------------

    [Test]
    public void Evaluate_deny_beats_allow_at_equal_specificity()
    {
        var rules = new[]
        {
            User("allow", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow),
            User("deny", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Deny),
        };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.Read, key: "k");

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("deny"));
    }

    // ---- Scope specificity ----------------------------------------------

    [Test]
    public void Evaluate_exact_key_beats_tree()
    {
        var rules = new[]
        {
            User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Deny),
            User("key", "alice", LatticeScope.Key(Tree, "abc"), LatticeOperation.Read, LatticeEffect.Allow),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "abc").Allowed, Is.True);
        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "other").Allowed, Is.False);
    }

    [Test]
    public void Evaluate_exact_key_beats_prefix()
    {
        var rules = new[]
        {
            User("prefix", "alice", LatticeScope.Prefix(Tree, "ab"), LatticeOperation.Read, LatticeEffect.Deny),
            User("key", "alice", LatticeScope.Key(Tree, "abc"), LatticeOperation.Read, LatticeEffect.Allow),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "abc").Allowed, Is.True);
    }

    [Test]
    public void Evaluate_longer_prefix_beats_shorter_prefix()
    {
        var rules = new[]
        {
            User("short", "alice", LatticeScope.Prefix(Tree, "a"), LatticeOperation.Read, LatticeEffect.Deny),
            User("long", "alice", LatticeScope.Prefix(Tree, "ab"), LatticeOperation.Read, LatticeEffect.Allow),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "abc").Allowed, Is.True, "longest matching prefix wins");
        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "az").Allowed, Is.False, "only the shorter prefix matches 'az'");
    }

    [Test]
    public void Evaluate_prefix_beats_tree()
    {
        var rules = new[]
        {
            User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Deny),
            User("prefix", "alice", LatticeScope.Prefix(Tree, "ab"), LatticeOperation.Read, LatticeEffect.Allow),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "abc").Allowed, Is.True);
        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "xyz").Allowed, Is.False);
    }

    // ---- User over group at equal scope ---------------------------------

    [Test]
    public void Evaluate_user_rule_beats_group_rule_at_equal_scope_by_default()
    {
        var rules = new[]
        {
            Group("group", "admins", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Deny),
            User("user", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var decision = Eval(rules, Subject("alice", "admins"), LatticeOperation.Read, key: "k");

        Assert.That(decision.Allowed, Is.True, "a user-specific allow overrides a group-level deny at equal scope by default");
    }

    [Test]
    public void Evaluate_user_and_group_are_equal_when_option_disabled_so_deny_wins()
    {
        var options = new LatticeAuthOptions { UserRuleBeatsGroupRuleAtEqualScope = false };
        var rules = new[]
        {
            Group("group", "admins", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Deny),
            User("user", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var decision = Eval(rules, Subject("alice", "admins"), LatticeOperation.Read, key: "k", options: options);

        Assert.That(decision.Allowed, Is.False, "with the toggle off, user and group are equally specific so deny overrides");
    }

    // ---- Operation bitset ------------------------------------------------

    [TestCase(LatticeOperation.Read, ExpectedResult = true)]
    [TestCase(LatticeOperation.Write, ExpectedResult = true)]
    [TestCase(LatticeOperation.Delete, ExpectedResult = false)]
    public bool Evaluate_matches_only_operations_in_the_rule_bitset(LatticeOperation requested)
    {
        var rules = new[]
        {
            User("r", "alice", LatticeScope.Tree(Tree), LatticeOperation.Read | LatticeOperation.Write, LatticeEffect.Allow),
        };

        return Eval(rules, Subject("alice"), requested, key: "k").Allowed;
    }

    [Test]
    public void Evaluate_none_operation_matches_no_rule()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), LatticeAuthOperations.All, LatticeEffect.Allow) };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.None, key: "k").Allowed, Is.False);
    }

    // ---- Range read key filter ------------------------------------------

    [Test]
    public void Evaluate_range_read_with_per_key_carveout_returns_a_key_filter()
    {
        var rules = new[]
        {
            User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.RangeRead, LatticeEffect.Allow),
            User("secret", "alice", LatticeScope.Key(Tree, "secret"), LatticeOperation.RangeRead, LatticeEffect.Deny),
        };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.RangeRead, key: null, rangeStart: "a", rangeEnd: "z");

        Assert.That(decision.Allowed, Is.True);
        Assert.That(decision.KeyFilter, Is.Not.Null);
        Assert.That(decision.KeyFilter!("open"), Is.True, "keys with no deny are admitted");
        Assert.That(decision.KeyFilter!("secret"), Is.False, "the per-key deny prunes the key");
        Assert.That(decision.Reason, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Evaluate_range_read_uniform_allow_has_no_filter()
    {
        var rules = new[] { User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.RangeRead, LatticeEffect.Allow) };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.RangeRead, key: null);

        Assert.That(decision.Allowed, Is.True);
        Assert.That(decision.KeyFilter, Is.Null, "a uniform allow needs no per-key filtering");
    }

    [Test]
    public void Evaluate_range_read_with_no_rules_denies_uniformly()
    {
        var decision = Eval(Array.Empty<LatticeAuthorizationRule>(), Subject("alice"), LatticeOperation.RangeRead, key: null);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.KeyFilter, Is.Null);
    }

    [Test]
    public void Evaluate_range_read_key_filter_admits_iff_point_decision_allows()
    {
        var options = new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny };
        var rules = new[]
        {
            User("open-prefix", "alice", LatticeScope.Prefix(Tree, "pub/"), LatticeOperation.RangeRead, LatticeEffect.Allow),
            User("closed-key", "alice", LatticeScope.Key(Tree, "pub/blocked"), LatticeOperation.RangeRead, LatticeEffect.Deny),
        };
        var subject = Subject("alice");
        var policy = CompiledPolicy.Compile(rules);

        var range = PolicyEvaluator.Evaluate(policy, options, subject, Tree, LatticeOperation.RangeRead, null, null, null);

        Assert.That(range.KeyFilter, Is.Not.Null);
        foreach (var key in new[] { "pub/a", "pub/blocked", "private/x" })
        {
            var point = PolicyEvaluator.Evaluate(policy, options, subject, Tree, LatticeOperation.RangeRead, key, null, null);
            Assert.That(range.KeyFilter!(key), Is.EqualTo(point.Allowed), $"filter and point decision must agree for '{key}'");
        }
    }

    // ---- Whole-tree / null-key shape ------------------------------------

    [Test]
    public void Evaluate_null_key_with_only_tree_rules_returns_uniform_decision()
    {
        var rules = new[] { User("admin", "alice", LatticeScope.Tree(Tree), LatticeOperation.Admin, LatticeEffect.Allow) };

        var decision = Eval(rules, Subject("alice"), LatticeOperation.Admin, key: null);

        Assert.That(decision.Allowed, Is.True);
        Assert.That(decision.KeyFilter, Is.Null);
    }

    [Test]
    public void Compile_null_rules_throws()
    {
        Assert.That(() => CompiledPolicy.Compile(null!), Throws.ArgumentNullException);
    }

    // ---- Backup / restore capabilities ----------------------------------

    [Test]
    public void Evaluate_backup_tree_allow_grants_backup_and_nothing_else()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), LatticeOperation.Backup, LatticeEffect.Allow) };

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Backup, key: null).Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "k").Allowed, Is.False,
                "a backup grant is distinct from read");
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Restore, key: null).Allowed, Is.False,
                "a backup grant does not confer restore");
        });
    }

    [Test]
    public void Evaluate_backup_denies_by_default_absent_a_grant()
    {
        var decision = Eval(Array.Empty<LatticeAuthorizationRule>(), Subject("alice"), LatticeOperation.Backup, key: null);

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public void Evaluate_restore_tree_allow_grants_restore_without_a_separate_write_grant()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), LatticeOperation.Restore, LatticeEffect.Allow) };

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Restore, key: null).Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Write, key: "k").Allowed, Is.False,
                "restore is modelled as its own capability at the rule level; the subsumption of write is enforced at the backup seam");
        });
    }

    [Test]
    public void Evaluate_backup_prefix_allow_is_scoped_to_the_prefix()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Prefix(Tree, "tenant-a/"), LatticeOperation.Backup, LatticeEffect.Allow) };

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Backup, key: "tenant-a/").Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Backup, key: "tenant-b/").Allowed, Is.False);
        });
    }

    [Test]
    public void Evaluate_backup_deny_overrides_a_broader_allow_at_equal_scope()
    {
        var rules = new[]
        {
            User("allow", "alice", LatticeScope.Key(Tree, "k"), LatticeOperation.Backup, LatticeEffect.Allow),
            User("deny", "alice", LatticeScope.Key(Tree, "k"), LatticeOperation.Backup, LatticeEffect.Deny),
        };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Backup, key: "k").Allowed, Is.False,
            "deny wins over allow at equal specificity");
    }

    [Test]
    public void Evaluate_restore_more_specific_deny_beats_less_specific_allow()
    {
        var rules = new[]
        {
            User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.Restore, LatticeEffect.Allow),
            User("key", "alice", LatticeScope.Key(Tree, "locked"), LatticeOperation.Restore, LatticeEffect.Deny),
        };

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Restore, key: "open").Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Restore, key: "locked").Allowed, Is.False,
                "the more specific key-scope deny beats the tree-scope allow");
        });
    }

    // ---- SchemaAdmin capability -----------------------------------------

    [Test]
    public void Evaluate_schema_admin_grant_authorizes_schema_admin_but_not_admin()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), LatticeOperation.SchemaAdmin, LatticeEffect.Allow) };

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.SchemaAdmin, key: null).Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Admin, key: null).Allowed, Is.False,
                "a schema-admin grant is distinct from data-plane admin");
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "k").Allowed, Is.False,
                "a schema-admin grant does not confer read");
        });
    }

    [Test]
    public void Evaluate_admin_grant_does_not_authorize_schema_admin()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), LatticeOperation.Admin, LatticeEffect.Allow) };

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Admin, key: null).Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.SchemaAdmin, key: null).Allowed, Is.False,
                "holding data-plane admin does not confer schema-admin");
        });
    }

    [Test]
    public void Evaluate_holding_both_admin_and_schema_admin_authorizes_each_independently()
    {
        var rules = new[]
        {
            User("admin", "alice", LatticeScope.Tree(Tree), LatticeOperation.Admin, LatticeEffect.Allow),
            User("schema", "alice", LatticeScope.Tree(Tree), LatticeOperation.SchemaAdmin, LatticeEffect.Allow),
        };

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Admin, key: null).Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.SchemaAdmin, key: null).Allowed, Is.True);
        });
    }

    [Test]
    public void Evaluate_schema_admin_denies_by_default_absent_a_grant()
    {
        var decision = Eval(Array.Empty<LatticeAuthorizationRule>(), Subject("alice"), LatticeOperation.SchemaAdmin, key: null);

        Assert.That(decision.Allowed, Is.False);
    }

    [Test]
    public void Evaluate_schema_admin_wildcard_grant_via_all_aggregate_authorizes_it()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), LatticeAuthOperations.All, LatticeEffect.Allow) };

        Assert.That(Eval(rules, Subject("alice"), LatticeOperation.SchemaAdmin, key: null).Allowed, Is.True,
            "a wildcard grant (LatticeAuthOperations.All) still covers schema-admin");
    }

    [Test]
    public void Evaluate_schema_admin_grant_narrows_to_a_single_tree()
    {
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), LatticeOperation.SchemaAdmin, LatticeEffect.Allow) };
        var policy = CompiledPolicy.Compile(rules);
        var options = new LatticeAuthOptions();

        Assert.Multiple(() =>
        {
            Assert.That(
                PolicyEvaluator.Evaluate(policy, options, Subject("alice"), Tree, LatticeOperation.SchemaAdmin, null, null, null).Allowed,
                Is.True,
                "the grant applies on its own tree");
            Assert.That(
                PolicyEvaluator.Evaluate(policy, options, Subject("alice"), "other-tree", LatticeOperation.SchemaAdmin, null, null, null).Allowed,
                Is.False,
                "the grant does not leak to another tree");
        });
    }

    [Test]
    public void Evaluate_schema_admin_prefix_deny_beats_tree_allow()
    {
        var rules = new[]
        {
            User("tree", "alice", LatticeScope.Tree(Tree), LatticeOperation.SchemaAdmin, LatticeEffect.Allow),
            User("prefix", "alice", LatticeScope.Prefix(Tree, "locked/"), LatticeOperation.SchemaAdmin, LatticeEffect.Deny),
        };

        Assert.Multiple(() =>
        {
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.SchemaAdmin, key: "open").Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.SchemaAdmin, key: "locked/x").Allowed, Is.False,
                "the more specific prefix-scope deny beats the tree-scope allow");
        });
    }

    // ---- Cluster-wide, scopeless Telemetry grant -------------------------

    private static LatticeAccessDecision EvalClusterWide(
        IEnumerable<LatticeAuthorizationRule> rules,
        LatticeSubject subject,
        LatticeOperation operation,
        LatticeAuthOptions? options = null)
    {
        var policy = CompiledPolicy.Compile(rules);
        return PolicyEvaluator.Evaluate(
            policy,
            options ?? new LatticeAuthOptions(),
            subject,
            LatticeScope.ClusterWideTreeId,
            operation,
            key: null,
            rangeStart: null,
            rangeEnd: null);
    }

    [Test]
    public void Evaluate_telemetry_authorizes_when_the_cluster_wide_grant_is_present()
    {
        var rules = new[]
        {
            User("r", "observer", LatticeScope.ClusterWide(), LatticeOperation.Telemetry, LatticeEffect.Allow),
        };

        Assert.That(
            EvalClusterWide(rules, Subject("observer"), LatticeOperation.Telemetry).Allowed,
            Is.True,
            "a cluster-wide telemetry grant authorizes a telemetry request");
    }

    [Test]
    public void Evaluate_telemetry_denies_by_default_when_the_grant_is_absent()
    {
        Assert.That(
            EvalClusterWide(Array.Empty<LatticeAuthorizationRule>(), Subject("observer"), LatticeOperation.Telemetry).Allowed,
            Is.False,
            "absent a grant, telemetry is denied by the deny-by-default effect");
    }

    [Test]
    public void Evaluate_telemetry_denies_when_only_another_subject_is_granted()
    {
        var rules = new[]
        {
            User("r", "observer", LatticeScope.ClusterWide(), LatticeOperation.Telemetry, LatticeEffect.Allow),
        };

        Assert.That(
            EvalClusterWide(rules, Subject("intruder"), LatticeOperation.Telemetry).Allowed,
            Is.False,
            "the grant is scoped to its subject only");
    }

    [Test]
    public void Evaluate_telemetry_is_not_conferred_by_a_full_data_plane_grant()
    {
        // A whole-data-plane grant (LatticeAuthOperations.All) over the sentinel
        // tree must not confer the scopeless Telemetry capability.
        var rules = new[]
        {
            User("r", "admin", LatticeScope.ClusterWide(), LatticeAuthOperations.All, LatticeEffect.Allow),
        };

        Assert.That(
            EvalClusterWide(rules, Subject("admin"), LatticeOperation.Telemetry).Allowed,
            Is.False,
            "Telemetry is excluded from LatticeAuthOperations.All and is not conferred by it");
    }

    [Test]
    public void Evaluate_telemetry_grant_does_not_confer_data_plane_read()
    {
        // Holding Telemetry must grant nothing else: a Telemetry-only grant does
        // not authorize a data-plane Read on an ordinary tree.
        var rules = new[]
        {
            User("r", "observer", LatticeScope.ClusterWide(), LatticeOperation.Telemetry, LatticeEffect.Allow),
        };
        var policy = CompiledPolicy.Compile(rules);

        Assert.That(
            PolicyEvaluator.Evaluate(policy, new LatticeAuthOptions(), Subject("observer"), Tree, LatticeOperation.Read, "k", null, null).Allowed,
            Is.False,
            "a telemetry grant confers no data-plane capability on a data tree");
    }

    [Test]
    public void Evaluate_existing_mask_is_unaffected_by_the_new_telemetry_bit()
    {
        // Backward-compatibility regression: a rule authored before Telemetry
        // existed carries the same integer mask and resolves exactly as before -
        // the new bit neither appears in nor perturbs an existing grant.
        var legacyMask = LatticeOperation.Read | LatticeOperation.Write;
        var rules = new[] { User("r", "alice", LatticeScope.Tree(Tree), legacyMask, LatticeEffect.Allow) };

        Assert.Multiple(() =>
        {
            Assert.That((int)legacyMask, Is.EqualTo(3), "the legacy mask value is byte-stable");
            Assert.That(legacyMask.HasFlag(LatticeOperation.Telemetry), Is.False);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Read, key: "k").Allowed, Is.True);
            Assert.That(Eval(rules, Subject("alice"), LatticeOperation.Write, key: "k").Allowed, Is.True);
            Assert.That(
                PolicyEvaluator.Evaluate(
                    CompiledPolicy.Compile(rules), new LatticeAuthOptions(), Subject("alice"),
                    LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry, null, null, null).Allowed,
                Is.False,
                "the legacy grant does not accidentally confer the new telemetry bit");
        });
    }
}
