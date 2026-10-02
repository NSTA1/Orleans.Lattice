using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAccessToolMappings"/>: every projection carries
/// the facade's fields across with enums rendered by name, absent reads project as
/// not found, and a rule whose subject is withheld is reduced to its id, layer,
/// origin and effect even when the facade populated more.
/// </summary>
[TestFixture]
public sealed class TenantAccessToolMappingsTests
{
    private static TenantRuleView WithheldRule(TenantRuleOrigin origin) => new()
    {
        RuleId = "Tree:*/r9",
        Layer = TenantRuleLayer.Platform,
        Origin = origin,
        Editable = true,
        SubjectId = "leaky-subject",
        SubjectKind = TenantSubjectKind.ClusterGroup,
        ScopeKind = TenantRuleScopeKind.Key,
        TreeName = "leaky-tree",
        KeyOrPrefix = "leaky-key",
        Operations = LatticeOperation.Write,
        Effect = LatticeEffect.Deny,
    };

    [TestCase(TenantRuleOrigin.PlatformWide)]
    [TestCase(TenantRuleOrigin.AppRole)]
    public void A_withheld_rule_discloses_only_its_id_layer_origin_and_effect(TenantRuleOrigin origin)
    {
        var projected = TenantAccessToolMappings.ToMcp(WithheldRule(origin));

        Assert.That(projected, Is.EqualTo(new McpTenantRule
        {
            RuleId = "Tree:*/r9",
            Layer = "Platform",
            Origin = origin.ToString(),
            Editable = false,
            SubjectWithheld = true,
            Effect = "Deny",
        }));
    }

    [Test]
    public void A_withheld_deciding_rule_and_matched_rules_are_masked_in_an_explanation()
    {
        var explanation = new TenantExplanation
        {
            TenantId = "acme",
            SubjectId = "alice",
            TreeName = "orders",
            Operation = LatticeOperation.Write,
            Allowed = false,
            DecidingLayer = TenantRuleLayer.Platform,
            DecidingRule = WithheldRule(TenantRuleOrigin.PlatformWide),
            DefaultEffect = LatticeEffect.Allow,
            MatchedRules = [WithheldRule(TenantRuleOrigin.AppRole)],
        };

        var projected = TenantAccessToolMappings.ToMcp(explanation);

        Assert.Multiple(() =>
        {
            Assert.That(projected.DecidingLayer, Is.EqualTo("Platform"));
            Assert.That(projected.DecidingRuleId, Is.EqualTo("Tree:*/r9"));
            Assert.That(projected.DecidingRule!.SubjectId, Is.Null);
            Assert.That(projected.DecidingRule.TreeName, Is.Null);
            Assert.That(projected.MatchedRules.Single().SubjectId, Is.Null);
            Assert.That(projected.MatchedRules.Single().Operations, Is.Null);
            Assert.That(projected.DefaultEffect, Is.EqualTo("Allow"));
            Assert.That(projected.Operation, Is.EqualTo("Write"));
            Assert.That(projected.Key, Is.Null);
        });
    }

    [Test]
    public void An_explanation_decided_by_the_default_effect_has_no_deciding_rule()
    {
        var projected = TenantAccessToolMappings.ToMcp(new TenantExplanation
        {
            TenantId = "acme",
            SubjectId = "alice",
            TreeName = "orders",
            DefaultEffect = LatticeEffect.Deny,
        });

        Assert.Multiple(() =>
        {
            Assert.That(projected.DecidingLayer, Is.Null);
            Assert.That(projected.DecidingRuleId, Is.Null);
            Assert.That(projected.DecidingRule, Is.Null);
            Assert.That(projected.MatchedRules, Is.Empty);
        });
    }

    [Test]
    public void A_tenant_rule_carries_every_field()
    {
        var projected = TenantAccessToolMappings.ToMcp(new TenantRuleView
        {
            RuleId = "r1",
            Layer = TenantRuleLayer.Tenant,
            Origin = TenantRuleOrigin.Tenant,
            Editable = true,
            SubjectId = "ops",
            SubjectKind = TenantSubjectKind.TenantGroup,
            ScopeKind = TenantRuleScopeKind.TenantWide,
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Allow,
        });

        Assert.That(projected, Is.EqualTo(new McpTenantRule
        {
            RuleId = "r1",
            Layer = "Tenant",
            Origin = "Tenant",
            Editable = true,
            SubjectWithheld = false,
            SubjectId = "ops",
            SubjectKind = "TenantGroup",
            ScopeKind = "TenantWide",
            Operations = "Read",
            Effect = "Allow",
        }));
    }

    [Test]
    public void Absent_reads_project_as_not_found()
    {
        var group = TenantAccessToolMappings.ToMcpGet("acme", "ops", (TenantGroupDescriptor?)null);
        var rule = TenantAccessToolMappings.ToMcpGet("acme", "r1", (TenantRuleView?)null);

        Assert.Multiple(() =>
        {
            Assert.That(group, Is.EqualTo(new McpTenantGroupGetResult { TenantId = "acme", Name = "ops", Found = false }));
            Assert.That(rule, Is.EqualTo(new McpTenantRuleGetResult { TenantId = "acme", RuleId = "r1", Found = false }));
        });
    }

    [Test]
    public void Group_removal_carries_the_whole_cascade()
    {
        var projected = TenantAccessToolMappings.ToMcp(new TenantGroupRemovalResult
        {
            TenantId = "acme",
            GroupName = "ops",
            Removed = true,
            EdgesRemoved = 3,
            RemovedFromMemberSet = true,
            RemovedFromAdminSet = true,
            RemovedRuleIds = ["r1", "r2"],
        });

        Assert.Multiple(() =>
        {
            Assert.That(projected.Removed, Is.True);
            Assert.That(projected.EdgesRemoved, Is.EqualTo(3));
            Assert.That(projected.RemovedFromMemberSet, Is.True);
            Assert.That(projected.RemovedFromAdminSet, Is.True);
            Assert.That(projected.RemovedRuleIds, Is.EqualTo(new[] { "r1", "r2" }));
        });
    }

    [Test]
    public void Pages_carry_their_entries_and_cursor()
    {
        var groups = TenantAccessToolMappings.ToMcp("acme", new TenantGroupPage
        {
            Entries = [new TenantGroupDescriptor { Name = "a", DisplayName = "A" }],
            NextPageToken = "a",
        });
        var members = TenantAccessToolMappings.ToMcp("acme", new TenantMemberPage
        {
            Entries = [new TenantMemberEntry { SubjectId = "ops", Kind = TenantSubjectKind.ClusterGroup }],
            NextPageToken = "ops",
        });
        var rules = TenantAccessToolMappings.ToMcp("acme", new TenantRulePage { NextPageToken = "z" });

        Assert.Multiple(() =>
        {
            Assert.That(groups.Groups, Is.EqualTo(new[] { new McpTenantGroup { Name = "a", DisplayName = "A" } }));
            Assert.That(groups.NextPageToken, Is.EqualTo("a"));
            Assert.That(members.Members, Is.EqualTo(new[] { new McpTenantSubject { SubjectId = "ops", Kind = "ClusterGroup" } }));
            Assert.That(members.NextPageToken, Is.EqualTo("ops"));
            Assert.That(rules.Rules, Is.Empty);
            Assert.That(rules.NextPageToken, Is.EqualTo("z"));
        });
    }

    [Test]
    public void Posture_carries_every_cap_with_its_usage()
    {
        var projected = TenantAccessToolMappings.ToMcp(new TenantAccessPosture
        {
            TenantId = "acme",
            Enabled = true,
            CallerIsTenantAdmin = true,
            Groups = new TenantQuotaDimensionUsage { Usage = 1, Limit = 500 },
            MembershipEdges = new TenantQuotaDimensionUsage { Usage = 2, Limit = 10000 },
            MemberSubjects = new TenantQuotaDimensionUsage { Usage = 3, Limit = 5000 },
            TenantRules = new TenantQuotaDimensionUsage { Usage = 4, Limit = 1000 },
        });

        Assert.Multiple(() =>
        {
            Assert.That(projected.Enabled, Is.True);
            Assert.That(projected.CallerIsTenantAdmin, Is.True);
            Assert.That(projected.CallerIsPlatformOperator, Is.False);
            Assert.That(projected.Groups, Is.EqualTo(new McpTenantCapUsage { Usage = 1, Limit = 500 }));
            Assert.That(projected.MembershipEdges, Is.EqualTo(new McpTenantCapUsage { Usage = 2, Limit = 10000 }));
            Assert.That(projected.MemberSubjects, Is.EqualTo(new McpTenantCapUsage { Usage = 3, Limit = 5000 }));
            Assert.That(projected.TenantRules, Is.EqualTo(new McpTenantCapUsage { Usage = 4, Limit = 1000 }));
        });
    }

    [Test]
    public void Effective_permissions_carry_the_subject_tree_and_rules()
    {
        var projected = TenantAccessToolMappings.ToMcp(new TenantEffectivePermissions
        {
            TenantId = "acme",
            SubjectId = "ops",
            SubjectKind = TenantSubjectKind.TenantGroup,
            Rules = [WithheldRule(TenantRuleOrigin.AppRole)],
        });

        Assert.Multiple(() =>
        {
            Assert.That(projected.SubjectKind, Is.EqualTo("TenantGroup"));
            Assert.That(projected.TreeName, Is.Null);
            Assert.That(projected.Rules.Single().SubjectWithheld, Is.True);
        });
    }

    [Test]
    public void Projections_reject_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantAccessToolMappings.ToMcp((TenantRuleView)null!), Throws.ArgumentNullException);
            Assert.That(() => TenantAccessToolMappings.ToMcp((TenantExplanation)null!), Throws.ArgumentNullException);
            Assert.That(() => TenantAccessToolMappings.ToMcp((TenantAccessPosture)null!), Throws.ArgumentNullException);
            Assert.That(() => TenantAccessToolMappings.ToMcp((TenantMembershipChangeResult)null!), Throws.ArgumentNullException);
            Assert.That(() => TenantAccessToolMappings.ToMcp("acme", (TenantGroupPage)null!), Throws.ArgumentNullException);
        });
    }
}
