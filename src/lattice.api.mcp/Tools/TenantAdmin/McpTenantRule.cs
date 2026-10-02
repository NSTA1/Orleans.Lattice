namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// One authorization rule as a tenant admin may see it: an editable tenant-tier
/// rule, or a read-only platform rule. A rule whose subject is withheld (a
/// platform-wide or app-role rule) carries only its id, layer, origin and effect;
/// every other field is <see langword="null"/>.
/// </summary>
internal sealed record McpTenantRule
{
    /// <summary>The rule id: tenant-local for a tenant rule, the platform id otherwise.</summary>
    public required string RuleId { get; init; }

    /// <summary>The policy layer: <c>Platform</c> or <c>Tenant</c>.</summary>
    public required string Layer { get; init; }

    /// <summary>Where the rule comes from: <c>PlatformTree</c>, <c>PlatformWide</c>, <c>AppRole</c> or <c>Tenant</c>.</summary>
    public required string Origin { get; init; }

    /// <summary>Whether a tenant admin may edit the rule (tenant-tier rules only).</summary>
    public required bool Editable { get; init; }

    /// <summary>Whether the rule's subject and scope are withheld from a tenant admin.</summary>
    public required bool SubjectWithheld { get; init; }

    /// <summary>The rule's subject, or <see langword="null"/> when withheld.</summary>
    public string? SubjectId { get; init; }

    /// <summary>The subject kind, or <see langword="null"/> when withheld.</summary>
    public string? SubjectKind { get; init; }

    /// <summary>The scope kind (<c>Tree</c>, <c>Key</c>, <c>Prefix</c> or <c>TenantWide</c>), or <see langword="null"/> when withheld.</summary>
    public string? ScopeKind { get; init; }

    /// <summary>The tenant-local tree name, or <see langword="null"/> for a tenant-wide rule or when withheld.</summary>
    public string? TreeName { get; init; }

    /// <summary>The key or prefix of a key- or prefix-scoped rule; otherwise <see langword="null"/>.</summary>
    public string? KeyOrPrefix { get; init; }

    /// <summary>The operations the rule covers, or <see langword="null"/> when withheld.</summary>
    public string? Operations { get; init; }

    /// <summary>The rule's effect: <c>Allow</c> or <c>Deny</c>.</summary>
    public required string Effect { get; init; }
}
