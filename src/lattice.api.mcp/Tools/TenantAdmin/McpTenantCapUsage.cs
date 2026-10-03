namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// One delegated-access cap and its usage, as the
/// <c>lattice_tenant_access_posture</c> tool reports it.
/// </summary>
internal sealed record McpTenantCapUsage
{
    /// <summary>The current count, or <see langword="null"/> when not measured.</summary>
    public long? Usage { get; init; }

    /// <summary>The effective cap, or <see langword="null"/> when not reported.</summary>
    public long? Limit { get; init; }
}
