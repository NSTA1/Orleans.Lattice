using System.ComponentModel;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>The tenant policy half of the tenant access tool adapters.</summary>
internal static partial class TenantAccessToolHandlers
{
    private const string RuleIdDescription =
        "The tenant-local rule id (not the composed tenant:{tenant}:{id} id).";

    private const string TreeNameDescription =
        "The tenant-local tree name (not the composed t/{tenant}/{tree} id).";

    // ----- Tenant-tier rules -----

    /// <summary>Lists one page of the tenant's editable rules and the read-only platform rules on its trees.</summary>
    public static async Task<McpTenantRuleListResult> ListRulesAsync(
        ILatticeTenantPolicyAdmin policy,
        [Description(TenantIdDescription)] string tenantId,
        [Description(PageSizeDescription)] int pageSize = 0,
        [Description(PageTokenDescription)] string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        TenantRulePage page;
        try
        {
            page = await policy
                .ListRulesAsync(tenantId, Page(pageSize, pageToken), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(tenantId, page);
    }

    /// <summary>Reads one tenant-tier rule.</summary>
    public static async Task<McpTenantRuleGetResult> GetRuleAsync(
        ILatticeTenantPolicyAdmin policy,
        [Description(TenantIdDescription)] string tenantId,
        [Description(RuleIdDescription)] string ruleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        TenantRuleView? rule;
        try
        {
            rule = await policy.GetRuleAsync(tenantId, ruleId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcpGet(tenantId, ruleId, rule);
    }

    /// <summary>Creates or replaces one tenant-tier rule.</summary>
    public static async Task<McpTenantRulePutResult> PutRuleAsync(
        ILatticeTenantPolicyAdmin policy,
        [Description(TenantIdDescription)] string tenantId,
        [Description(RuleIdDescription)] string ruleId,
        [Description("The rule's subject: a user id, a cluster group id, or one of this tenant's group names, as subjectKind says.")] string subjectId,
        [Description("The rule's scope: Tree (a whole tenant tree), Key (one key), Prefix (a key prefix), or TenantWide (every tree the tenant owns; treeName must then be omitted).")] TenantRuleScopeKind scopeKind,
        [Description("The operations the rule covers, for example Read or Read, Write.")] LatticeOperation operations,
        [Description("Allow or Deny. A tenant rule sits beneath every platform rule: it can never override an operator deny or revoke an operator allow.")] LatticeEffect effect,
        [Description(TreeNameDescription + " Required for Tree, Key and Prefix scopes; omitted for TenantWide.")] string? treeName = null,
        [Description("The key (Key scope) or key prefix (Prefix scope); omitted otherwise.")] string? keyOrPrefix = null,
        [Description(SubjectKindDescription)] TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        var draft = new TenantRuleDraft
        {
            RuleId = ruleId,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            ScopeKind = scopeKind,
            TreeName = treeName,
            KeyOrPrefix = keyOrPrefix,
            Operations = operations,
            Effect = effect,
        };

        TenantRuleView persisted;
        try
        {
            persisted = await policy.PutRuleAsync(tenantId, draft, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcpPut(tenantId, persisted);
    }

    /// <summary>Removes one tenant-tier rule.</summary>
    public static async Task<McpTenantRuleRemoveResult> RemoveRuleAsync(
        ILatticeTenantPolicyAdmin policy,
        [Description(TenantIdDescription)] string tenantId,
        [Description(RuleIdDescription)] string ruleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        bool removed;
        try
        {
            removed = await policy.RemoveRuleAsync(tenantId, ruleId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return new McpTenantRuleRemoveResult { TenantId = tenantId, RuleId = ruleId, Removed = removed };
    }

    // ----- Introspection -----

    /// <summary>Explains a subject's verdict on one tenant tree, optional key and operation.</summary>
    public static async Task<McpTenantExplanationResult> ExplainAsync(
        ILatticeTenantPolicyAdmin policy,
        [Description(TenantIdDescription)] string tenantId,
        [Description("The subject to explain, as subjectKind says.")] string subjectId,
        [Description(TreeNameDescription)] string treeName,
        [Description("The operation to explain, for example Read.")] LatticeOperation operation,
        [Description("The key to explain, or omitted for a whole-tree request.")] string? key = null,
        [Description(SubjectKindDescription)] TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        TenantExplanation explanation;
        try
        {
            explanation = await policy
                .ExplainAsync(tenantId, subjectId, treeName, key, operation, subjectKind, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(explanation);
    }

    /// <summary>Reports the rules in effect for a subject within the tenant.</summary>
    public static async Task<McpTenantEffectivePermissionsResult> EffectivePermissionsAsync(
        ILatticeTenantPolicyAdmin policy,
        [Description(TenantIdDescription)] string tenantId,
        [Description("The subject to report on, as subjectKind says.")] string subjectId,
        [Description(TreeNameDescription + " Omit to report across every tree the tenant owns.")] string? treeName = null,
        [Description(SubjectKindDescription)] TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        TenantEffectivePermissions permissions;
        try
        {
            permissions = await policy
                .EffectivePermissionsAsync(tenantId, subjectId, treeName, subjectKind, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(permissions);
    }

    // ----- Posture -----

    /// <summary>Reads the tenant access posture: the feature flag, the caller's standing and the caps.</summary>
    public static async Task<McpTenantAccessPostureResult> GetPostureAsync(
        ILatticeTenantPolicyAdmin policy,
        [Description(TenantIdDescription)] string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        TenantAccessPosture posture;
        try
        {
            posture = await policy.GetPostureAsync(tenantId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(posture);
    }
}
