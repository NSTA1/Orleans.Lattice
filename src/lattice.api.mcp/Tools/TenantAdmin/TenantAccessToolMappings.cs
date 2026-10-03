using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Pure projections from the delegated tenant access facades' results
/// (<see cref="ILatticeTenantDirectoryAdmin"/> and
/// <see cref="ILatticeTenantPolicyAdmin"/>) onto the compact MCP structured-content
/// records the tenant access tools return. Enums are rendered by name, so every
/// result is plain ASCII.
/// </summary>
/// <remarks>
/// A rule whose subject the facade withholds (a platform-wide or app-role rule) is
/// projected with only its id, layer, origin and effect, whatever else the facade
/// populated, so the MCP surface never discloses more of such a rule than the
/// contract allows.
/// </remarks>
internal static class TenantAccessToolMappings
{
    /// <summary>Projects a page of tenant groups.</summary>
    /// <param name="tenantId">The tenant the page was read for.</param>
    /// <param name="page">The facade page. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP list result.</returns>
    public static McpTenantGroupListResult ToMcp(string tenantId, TenantGroupPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        var entries = page.Entries;
        var groups = new McpTenantGroup[entries.Count];
        for (var i = 0; i < groups.Length; i++)
        {
            var group = entries[i];
            groups[i] = new McpTenantGroup { Name = group.Name, DisplayName = group.DisplayName };
        }

        return new McpTenantGroupListResult { TenantId = tenantId, Groups = groups, NextPageToken = page.NextPageToken };
    }

    /// <summary>Projects a single-group read; a <see langword="null"/> group reads as not found.</summary>
    /// <param name="tenantId">The tenant the read was made for.</param>
    /// <param name="name">The group name that was read.</param>
    /// <param name="group">The group, or <see langword="null"/> when absent.</param>
    /// <returns>The MCP get result.</returns>
    public static McpTenantGroupGetResult ToMcpGet(string tenantId, string name, TenantGroupDescriptor? group)
        => new()
        {
            TenantId = tenantId,
            Name = group?.Name ?? name,
            Found = group is not null,
            DisplayName = group?.DisplayName,
        };

    /// <summary>Projects a written tenant group.</summary>
    /// <param name="tenantId">The tenant that owns the group.</param>
    /// <param name="group">The written group. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP upsert result.</returns>
    public static McpTenantGroupResult ToMcp(string tenantId, TenantGroupDescriptor group)
    {
        ArgumentNullException.ThrowIfNull(group);
        return new McpTenantGroupResult { TenantId = tenantId, Name = group.Name, DisplayName = group.DisplayName };
    }

    /// <summary>Projects a group-removal result.</summary>
    /// <param name="result">The removal result. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP removal result.</returns>
    public static McpTenantGroupRemovalResult ToMcp(TenantGroupRemovalResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpTenantGroupRemovalResult
        {
            TenantId = result.TenantId,
            GroupName = result.GroupName,
            Removed = result.Removed,
            EdgesRemoved = result.EdgesRemoved,
            RemovedFromMemberSet = result.RemovedFromMemberSet,
            RemovedFromAdminSet = result.RemovedFromAdminSet,
            RemovedRuleIds = result.RemovedRuleIds,
        };
    }

    /// <summary>Projects a group's direct members.</summary>
    /// <param name="tenantId">The tenant that owns the group.</param>
    /// <param name="groupName">The group name.</param>
    /// <param name="members">The members. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP group-members result.</returns>
    public static McpTenantGroupMembersResult ToMcp(
        string tenantId, string groupName, IReadOnlyList<TenantGroupMember> members)
    {
        ArgumentNullException.ThrowIfNull(members);
        var subjects = new McpTenantSubject[members.Count];
        for (var i = 0; i < subjects.Length; i++)
        {
            var member = members[i];
            subjects[i] = new McpTenantSubject { SubjectId = member.MemberId, Kind = Name(member.Kind) };
        }

        return new McpTenantGroupMembersResult { TenantId = tenantId, GroupName = groupName, Members = subjects };
    }

    /// <summary>Projects a page of the tenant member set.</summary>
    /// <param name="tenantId">The tenant the page was read for.</param>
    /// <param name="page">The facade page. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP member-list result.</returns>
    public static McpTenantMemberListResult ToMcp(string tenantId, TenantMemberPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        var entries = page.Entries;
        var subjects = new McpTenantSubject[entries.Count];
        for (var i = 0; i < subjects.Length; i++)
        {
            var entry = entries[i];
            subjects[i] = new McpTenantSubject { SubjectId = entry.SubjectId, Kind = Name(entry.Kind) };
        }

        return new McpTenantMemberListResult { TenantId = tenantId, Members = subjects, NextPageToken = page.NextPageToken };
    }

    /// <summary>Projects a membership change.</summary>
    /// <param name="result">The change result. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP membership-change result.</returns>
    public static McpTenantMembershipChangeResult ToMcp(TenantMembershipChangeResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpTenantMembershipChangeResult
        {
            TenantId = result.TenantId,
            GroupName = result.GroupName,
            SubjectId = result.SubjectId,
            SubjectKind = Name(result.SubjectKind),
            Changed = result.Changed,
        };
    }

    /// <summary>Projects a page of tenant-visible rules.</summary>
    /// <param name="tenantId">The tenant the page was read for.</param>
    /// <param name="page">The facade page. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP rule-list result.</returns>
    public static McpTenantRuleListResult ToMcp(string tenantId, TenantRulePage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        return new McpTenantRuleListResult
        {
            TenantId = tenantId,
            Rules = ToMcp(page.Entries),
            NextPageToken = page.NextPageToken,
        };
    }

    /// <summary>Projects a single-rule read; a <see langword="null"/> rule reads as not found.</summary>
    /// <param name="tenantId">The tenant the read was made for.</param>
    /// <param name="ruleId">The rule id that was read.</param>
    /// <param name="rule">The rule, or <see langword="null"/> when absent.</param>
    /// <returns>The MCP rule-get result.</returns>
    public static McpTenantRuleGetResult ToMcpGet(string tenantId, string ruleId, TenantRuleView? rule)
        => new()
        {
            TenantId = tenantId,
            RuleId = rule?.RuleId ?? ruleId,
            Found = rule is not null,
            Rule = rule is null ? null : ToMcp(rule),
        };

    /// <summary>Projects a persisted tenant rule.</summary>
    /// <param name="tenantId">The tenant that owns the rule.</param>
    /// <param name="rule">The persisted rule. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP rule-put result.</returns>
    public static McpTenantRulePutResult ToMcpPut(string tenantId, TenantRuleView rule)
        => new() { TenantId = tenantId, Rule = ToMcp(rule) };

    /// <summary>Projects an explanation.</summary>
    /// <param name="explanation">The explanation. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP explanation result.</returns>
    public static McpTenantExplanationResult ToMcp(TenantExplanation explanation)
    {
        ArgumentNullException.ThrowIfNull(explanation);
        var deciding = explanation.DecidingRule;
        return new McpTenantExplanationResult
        {
            TenantId = explanation.TenantId,
            SubjectId = explanation.SubjectId,
            SubjectKind = Name(explanation.SubjectKind),
            TreeName = explanation.TreeName,
            Key = explanation.Key,
            Operation = explanation.Operation.ToString(),
            Allowed = explanation.Allowed,
            Filtered = explanation.Filtered,
            Reason = explanation.Reason,
            DecidingLayer = explanation.DecidingLayer is { } layer ? Name(layer) : null,
            DecidingRuleId = deciding?.RuleId,
            DecidingRule = deciding is null ? null : ToMcp(deciding),
            DefaultEffect = Name(explanation.DefaultEffect),
            MatchedRules = ToMcp(explanation.MatchedRules),
        };
    }

    /// <summary>Projects an effective-permissions report.</summary>
    /// <param name="permissions">The report. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP effective-permissions result.</returns>
    public static McpTenantEffectivePermissionsResult ToMcp(TenantEffectivePermissions permissions)
    {
        ArgumentNullException.ThrowIfNull(permissions);
        return new McpTenantEffectivePermissionsResult
        {
            TenantId = permissions.TenantId,
            SubjectId = permissions.SubjectId,
            SubjectKind = Name(permissions.SubjectKind),
            TreeName = permissions.TreeName,
            Rules = ToMcp(permissions.Rules),
        };
    }

    /// <summary>Projects the tenant access posture.</summary>
    /// <param name="posture">The posture. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP posture result.</returns>
    public static McpTenantAccessPostureResult ToMcp(TenantAccessPosture posture)
    {
        ArgumentNullException.ThrowIfNull(posture);
        return new McpTenantAccessPostureResult
        {
            TenantId = posture.TenantId,
            Enabled = posture.Enabled,
            CallerIsTenantAdmin = posture.CallerIsTenantAdmin,
            CallerIsPlatformOperator = posture.CallerIsPlatformOperator,
            Groups = ToMcp(posture.Groups),
            MembershipEdges = ToMcp(posture.MembershipEdges),
            MemberSubjects = ToMcp(posture.MemberSubjects),
            TenantRules = ToMcp(posture.TenantRules),
        };
    }

    /// <summary>
    /// Projects one rule. A withheld rule keeps only its id, layer, origin and
    /// effect, whatever else the facade populated.
    /// </summary>
    /// <param name="rule">The rule. Must not be <see langword="null"/>.</param>
    /// <returns>The MCP rule.</returns>
    public static McpTenantRule ToMcp(TenantRuleView rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        if (rule.SubjectWithheld)
        {
            return new McpTenantRule
            {
                RuleId = rule.RuleId,
                Layer = Name(rule.Layer),
                Origin = Name(rule.Origin),
                Editable = false,
                SubjectWithheld = true,
                Effect = Name(rule.Effect),
            };
        }

        return new McpTenantRule
        {
            RuleId = rule.RuleId,
            Layer = Name(rule.Layer),
            Origin = Name(rule.Origin),
            Editable = rule.Editable,
            SubjectWithheld = false,
            SubjectId = rule.SubjectId,
            SubjectKind = Name(rule.SubjectKind),
            ScopeKind = Name(rule.ScopeKind),
            TreeName = rule.TreeName,
            KeyOrPrefix = rule.KeyOrPrefix,
            Operations = rule.Operations.ToString(),
            Effect = Name(rule.Effect),
        };
    }

    private static McpTenantRule[] ToMcp(IReadOnlyList<TenantRuleView> rules)
    {
        var projected = new McpTenantRule[rules.Count];
        for (var i = 0; i < projected.Length; i++)
        {
            projected[i] = ToMcp(rules[i]);
        }

        return projected;
    }

    private static McpTenantCapUsage ToMcp(TenantQuotaDimensionUsage usage)
        => new() { Usage = usage.Usage, Limit = usage.Limit };

    // Switch expressions over the closed enums return interned literals, so a
    // projection never allocates an Enum.ToString string for a known value. The
    // LatticeOperation flags are the one exception: their combinations are open-ended,
    // so they render with ToString on this cold administrative path.
    private static string Name(TenantSubjectKind kind) => kind switch
    {
        TenantSubjectKind.User => nameof(TenantSubjectKind.User),
        TenantSubjectKind.TenantGroup => nameof(TenantSubjectKind.TenantGroup),
        TenantSubjectKind.ClusterGroup => nameof(TenantSubjectKind.ClusterGroup),
        _ => kind.ToString(),
    };

    private static string Name(LatticeEffect effect) => effect switch
    {
        LatticeEffect.Allow => nameof(LatticeEffect.Allow),
        LatticeEffect.Deny => nameof(LatticeEffect.Deny),
        _ => effect.ToString(),
    };

    private static string Name(TenantRuleLayer layer) => layer switch
    {
        TenantRuleLayer.Platform => nameof(TenantRuleLayer.Platform),
        TenantRuleLayer.Tenant => nameof(TenantRuleLayer.Tenant),
        _ => layer.ToString(),
    };

    private static string Name(TenantRuleOrigin origin) => origin switch
    {
        TenantRuleOrigin.PlatformTree => nameof(TenantRuleOrigin.PlatformTree),
        TenantRuleOrigin.PlatformWide => nameof(TenantRuleOrigin.PlatformWide),
        TenantRuleOrigin.AppRole => nameof(TenantRuleOrigin.AppRole),
        TenantRuleOrigin.Tenant => nameof(TenantRuleOrigin.Tenant),
        _ => origin.ToString(),
    };

    private static string Name(TenantRuleScopeKind scope) => scope switch
    {
        TenantRuleScopeKind.Tree => nameof(TenantRuleScopeKind.Tree),
        TenantRuleScopeKind.Key => nameof(TenantRuleScopeKind.Key),
        TenantRuleScopeKind.Prefix => nameof(TenantRuleScopeKind.Prefix),
        TenantRuleScopeKind.TenantWide => nameof(TenantRuleScopeKind.TenantWide),
        _ => scope.ToString(),
    };
}
