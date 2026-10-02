namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_explain</c> tool: a
/// layer-aware verdict for one subject, tree, optional key and operation. When the
/// verdict came from a platform-wide rule or an app role, the deciding rule carries
/// only its id and effect.
/// </summary>
internal sealed record McpTenantExplanationResult
{
    /// <summary>The tenant the explanation was made for.</summary>
    public required string TenantId { get; init; }

    /// <summary>The subject explained.</summary>
    public required string SubjectId { get; init; }

    /// <summary>The subject kind: <c>User</c>, <c>TenantGroup</c> or <c>ClusterGroup</c>.</summary>
    public required string SubjectKind { get; init; }

    /// <summary>The tenant-local tree name.</summary>
    public required string TreeName { get; init; }

    /// <summary>The key explained, or <see langword="null"/> for a whole-tree request.</summary>
    public string? Key { get; init; }

    /// <summary>The operation explained.</summary>
    public required string Operation { get; init; }

    /// <summary>Whether the operation is allowed.</summary>
    public required bool Allowed { get; init; }

    /// <summary>Whether the verdict is filtered (applies per key rather than uniformly).</summary>
    public required bool Filtered { get; init; }

    /// <summary>The engine's reason text, or <see langword="null"/>.</summary>
    public string? Reason { get; init; }

    /// <summary>The layer that decided (<c>Platform</c> or <c>Tenant</c>), or <see langword="null"/> when the default effect decided.</summary>
    public string? DecidingLayer { get; init; }

    /// <summary>The deciding rule's id, or <see langword="null"/> when the default effect decided.</summary>
    public string? DecidingRuleId { get; init; }

    /// <summary>The deciding rule, or <see langword="null"/> when the default effect decided.</summary>
    public McpTenantRule? DecidingRule { get; init; }

    /// <summary>The effect that applies when no rule matches.</summary>
    public required string DefaultEffect { get; init; }

    /// <summary>Every rule that matched the request.</summary>
    public required IReadOnlyList<McpTenantRule> MatchedRules { get; init; }
}
