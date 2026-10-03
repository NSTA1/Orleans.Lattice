using static Orleans.Lattice.Auth.Tests.TenantLayerTestRules;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Point-decision tests for the two-layer evaluation (epic #4154, D8): operator
/// rules are final when they match, and the tenant layer - tenant-wide deny, then
/// the tree's most-specific tenant verdict, then tenant-wide allow, then the
/// default effect - runs only when no operator rule matches, only while the layer
/// is active, and only for a tenant-layer tree.
/// </summary>
[TestFixture]
public sealed class TenantLayerEvaluationTests
{
    private static readonly LatticeSubjectSelector Alice = LatticeSubjectSelector.User("alice");
    private static readonly LatticeSubjectSelector Readers = LatticeSubjectSelector.Group(ContosoReaders);

    // ---- Definition-of-done tests ------------------------------------------

    [Test]
    public void Tenant_key_allow_under_an_operator_tree_deny_is_denied()
    {
        var rules = new[]
        {
            Operator("ops-deny", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
            Tenant("key-allow", Alice, LatticeScope.Key(TenantTree, "k"), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k", out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Operator));
            Assert.That(match.RuleId, Is.EqualTo("ops-deny"));
        });
    }

    [Test]
    public void Tenant_tree_deny_under_an_operator_all_trees_allow_is_allowed()
    {
        var options = new LatticeAuthOptions { AllTreesGrantsEnabled = true };
        var rules = new[]
        {
            Operator("ops-all", Alice, LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("tree-deny", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k", out var match, options);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Operator));
            Assert.That(match.AllTrees, Is.True);
        });
    }

    [Test]
    public void Tenant_wide_deny_beats_a_tenant_tree_allow()
    {
        var rules = new[]
        {
            Tenant("wide-deny", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Deny),
            Tenant("key-allow", Alice, LatticeScope.Key(TenantTree, "k"), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k", out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Tenant));
            Assert.That(match.TenantWide, Is.True);
            Assert.That(match.RuleId, Is.EqualTo(LatticeTenantRuleIds.For(Contoso, "wide-deny")));
            Assert.That(decision.Reason, Does.Contain("tenant rule").And.Contain("tenant-wide"));
        });
    }

    [Test]
    public void Tenant_wide_allow_grants_where_nothing_else_matches()
    {
        var rules = new[]
        {
            Tenant("wide-allow", Readers, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("bob", ContosoReaders), TenantTree, LatticeOperation.Read, "k", out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Tenant));
            Assert.That(match.TenantWide, Is.True);
        });
    }

    [TestCase("t/contoso/a/billing/ledger", TestName = "Tenant_layer_never_applies_to_an_app_owned_tree")]
    [TestCase("t/contoso/sys-auth-policy", TestName = "Tenant_layer_never_applies_to_a_system_local_name")]
    [TestCase("t/contoso/_lattice_trees", TestName = "Tenant_layer_never_applies_to_an_internal_local_name")]
    [TestCase("sys-auth-policy", TestName = "Tenant_layer_never_applies_to_a_system_tree")]
    [TestCase("orders", TestName = "Tenant_layer_never_applies_to_a_default_tenant_tree")]
    [TestCase("t/default/orders", TestName = "Tenant_layer_never_applies_to_a_composed_default_tenant_tree")]
    [TestCase("t/fabrikam/orders", TestName = "Tenant_layer_never_applies_to_another_tenants_tree")]
    public void Tenant_layer_never_applies_outside_the_tenants_own_data_trees(string treeId)
    {
        // A tenant-wide allow is the broadest tenant grant; even it must not reach these trees.
        var rules = new[]
        {
            Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), treeId, LatticeOperation.Read, "k", out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(match.Matched, Is.False);
        });
    }

    [Test]
    public void Inactive_layer_decides_byte_identically_with_a_pre_existing_tenant_rule()
    {
        var operatorRules = new[]
        {
            Operator("ops-key", Alice, LatticeScope.Key(TenantTree, "a"), LatticeOperation.Read, LatticeEffect.Allow),
            Operator("ops-deny", Alice, LatticeScope.Prefix(TenantTree, "x/"), LatticeOperation.Read, LatticeEffect.Deny),
        };
        var withTenant = operatorRules.Concat(new[]
        {
            Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("key-allow", Alice, LatticeScope.Key(TenantTree, "x/1"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("tree-deny", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
        }).ToArray();

        foreach (var options in new[] { new LatticeAuthOptions(), new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow } })
        {
            foreach (var key in new[] { "a", "b", "x/1", "x/2" })
            {
                var baseline = PolicyEvaluator.Evaluate(
                    CompiledPolicy.Compile(operatorRules), options, Subject("alice"), TenantTree, LatticeOperation.Read, key, null, null, out var baselineMatch);
                var inactive = Evaluate(withTenant, Subject("alice"), TenantTree, LatticeOperation.Read, key, out var inactiveMatch, options, active: false);

                Assert.Multiple(() =>
                {
                    Assert.That(inactive.Allowed, Is.EqualTo(baseline.Allowed), key);
                    Assert.That(inactive.Reason, Is.EqualTo(baseline.Reason), key);
                    Assert.That(inactiveMatch, Is.EqualTo(baselineMatch), key);
                });
            }
        }
    }

    [Test]
    public void Inactive_layer_over_a_snapshot_with_a_tenant_partition_does_not_enter_the_tenant_layer()
    {
        // The flag is switched off after a snapshot was compiled with the partition.
        var policy = CompiledPolicy.Compile(
            new[] { Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow) },
            includeTenantLayer: true);

        var decision = PolicyEvaluator.Evaluate(
            policy, new LatticeAuthOptions(), Subject("alice"), TenantTree, LatticeOperation.Read, "k", null, null, tenantLayerActive: false, out _);

        Assert.That(decision.Allowed, Is.False);
    }

    // ---- Layer precedence --------------------------------------------------

    [Test]
    public void Operator_allow_is_final_over_a_tenant_wide_deny()
    {
        var rules = new[]
        {
            Operator("ops-allow", Alice, LatticeScope.Prefix(TenantTree, "k"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("wide-deny", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k").Allowed, Is.True);
    }

    [Test]
    public void Operator_app_rule_is_final_over_a_tenant_deny()
    {
        var rules = new[]
        {
            Operator(LatticeAppRuleIds.Prefix + "x/role", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("tree-deny", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k").Allowed, Is.True);
    }

    [Test]
    public void Tenant_layer_runs_when_operator_rules_exist_but_none_matches_the_operation()
    {
        var rules = new[]
        {
            Operator("ops-write", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Write, LatticeEffect.Deny),
            Tenant("tree-allow", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k", out var match);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Tenant));
            Assert.That(match.TenantWide, Is.False);
        });
    }

    [Test]
    public void Tenant_tree_verdict_beats_a_tenant_wide_allow()
    {
        var rules = new[]
        {
            Tenant("wide-allow", Alice, LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("prefix-deny", Alice, LatticeScope.Prefix(TenantTree, "secret/"), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.Multiple(() =>
        {
            Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "secret/1").Allowed, Is.False);
            Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "public/1").Allowed, Is.True);
        });
    }

    [Test]
    public void Tenant_most_specific_scope_wins_inside_the_tenant_tree()
    {
        var rules = new[]
        {
            Tenant("tree-deny", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
            Tenant("prefix-allow", Alice, LatticeScope.Prefix(TenantTree, "p/"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("key-deny", Alice, LatticeScope.Key(TenantTree, "p/blocked"), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.Multiple(() =>
        {
            Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "p/ok").Allowed, Is.True);
            Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "p/blocked").Allowed, Is.False);
            Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "q").Allowed, Is.False);
        });
    }

    [Test]
    public void Tenant_deny_overrides_allow_at_an_equal_tier()
    {
        var rules = new[]
        {
            Tenant("allow", Readers, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("deny", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
        };
        var options = new LatticeAuthOptions { UserRuleBeatsGroupRuleAtEqualScope = false };

        Assert.That(Evaluate(rules, Subject("alice", ContosoReaders), TenantTree, LatticeOperation.Read, "k", options).Allowed, Is.False);
    }

    [Test]
    public void Tenant_user_rule_beats_group_rule_at_equal_scope_when_configured()
    {
        var rules = new[]
        {
            Tenant("group-deny", Readers, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
            Tenant("user-allow", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow),
        };
        var subject = Subject("alice", ContosoReaders);

        Assert.Multiple(() =>
        {
            Assert.That(
                Evaluate(rules, subject, TenantTree, LatticeOperation.Read, "k", new LatticeAuthOptions { UserRuleBeatsGroupRuleAtEqualScope = true }).Allowed,
                Is.True);
            Assert.That(
                Evaluate(rules, subject, TenantTree, LatticeOperation.Read, "k", new LatticeAuthOptions { UserRuleBeatsGroupRuleAtEqualScope = false }).Allowed,
                Is.False);
        });
    }

    [Test]
    public void Unmatched_tenant_layer_falls_to_the_default_effect()
    {
        var rules = new[]
        {
            Tenant("other-user", LatticeSubjectSelector.User("bob"), LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Deny),
        };

        Assert.Multiple(() =>
        {
            Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k").Allowed, Is.False);
            Assert.That(
                Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k", new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }).Allowed,
                Is.True);
        });
    }

    [Test]
    public void Tenant_wide_rule_of_another_tenant_does_not_reach_this_tenants_tree()
    {
        var rules = new[]
        {
            Tenant(Fabrikam, "wide-allow", Alice, LatticeScope.TenantWide(Fabrikam), LatticeOperation.Read, LatticeEffect.Allow),
        };

        Assert.That(Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k").Allowed, Is.False);
    }

    // ---- Explain trace ------------------------------------------------------

    [Test]
    public void Tenant_tree_match_carries_the_tenant_layer_and_rule_id_on_the_trace()
    {
        var rules = new[]
        {
            Tenant("key-deny", Alice, LatticeScope.Key(TenantTree, "k"), LatticeOperation.Read, LatticeEffect.Deny),
        };

        var decision = Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k", out var match);

        Assert.Multiple(() =>
        {
            Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.Tenant));
            Assert.That(match.RuleId, Is.EqualTo(LatticeTenantRuleIds.For(Contoso, "key-deny")));
            Assert.That(match.ScopeKind, Is.EqualTo(LatticeScopeKind.Key));
            Assert.That(match.ScopeValue, Is.EqualTo("k"));
            Assert.That(decision.Reason, Does.StartWith("Denied by tenant rule").And.Contain("key 'k'"));
        });
    }

    [Test]
    public void Default_effect_carries_no_layer_on_the_trace()
    {
        var rules = new[] { Tenant("bob", LatticeSubjectSelector.User("bob"), LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow) };

        Evaluate(rules, Subject("alice"), TenantTree, LatticeOperation.Read, "k", out var match);

        Assert.That(match.Layer, Is.EqualTo(PolicyDecisionLayer.None));
    }

    [Test]
    public void LayerOf_labels_rules_by_their_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantRuleConfinement.LayerOf(LatticeTenantRuleIds.For(Contoso, "r")), Is.EqualTo(PolicyDecisionLayer.Tenant));
            Assert.That(TenantRuleConfinement.LayerOf("ops"), Is.EqualTo(PolicyDecisionLayer.Operator));
            Assert.That(TenantRuleConfinement.LayerOf(LatticeAppRuleIds.Prefix + "x"), Is.EqualTo(PolicyDecisionLayer.Operator));
        });
    }

    [Test]
    public void AsTenantLayer_leaves_an_unmatched_value_unmatched()
    {
        Assert.That(default(PolicyMatch).AsTenantLayer(tenantWide: true), Is.EqualTo(default(PolicyMatch)));
    }
}
