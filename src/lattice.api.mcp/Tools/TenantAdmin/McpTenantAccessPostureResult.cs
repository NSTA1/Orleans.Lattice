namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_access_posture</c>
/// tool: whether delegated tenant access administration is enabled, the caller's
/// standing on the tenant, and the four delegated-access caps with their usage.
/// It is the one tenant access read that answers while the feature is off.
/// </summary>
internal sealed record McpTenantAccessPostureResult
{
    /// <summary>The tenant the posture was read for.</summary>
    public required string TenantId { get; init; }

    /// <summary>Whether delegated tenant access administration is enabled on the cluster.</summary>
    public required bool Enabled { get; init; }

    /// <summary>Whether the caller is an admin of the tenant.</summary>
    public required bool CallerIsTenantAdmin { get; init; }

    /// <summary>Whether the caller is a platform operator.</summary>
    public required bool CallerIsPlatformOperator { get; init; }

    /// <summary>The tenant-groups cap and usage.</summary>
    public required McpTenantCapUsage Groups { get; init; }

    /// <summary>The membership-edges cap and usage.</summary>
    public required McpTenantCapUsage MembershipEdges { get; init; }

    /// <summary>The member-set cap and usage.</summary>
    public required McpTenantCapUsage MemberSubjects { get; init; }

    /// <summary>The tenant-rules cap and usage.</summary>
    public required McpTenantCapUsage TenantRules { get; init; }
}
