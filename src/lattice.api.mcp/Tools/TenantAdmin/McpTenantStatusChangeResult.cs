namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_suspend</c> and
/// <c>lattice_tenant_resume</c> tools: the tenant's lifecycle status before and
/// after the transition and whether the call actually moved it
/// (<see cref="Changed"/> is <see langword="false"/> when the tenant was already
/// in the requested status, so the call was an idempotent no-op).
/// </summary>
internal sealed record McpTenantStatusChangeResult
{
    /// <summary>The tenant id whose status was transitioned.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant's lifecycle status name before the transition.</summary>
    public required string PreviousStatus { get; init; }

    /// <summary>The tenant's lifecycle status name after the transition.</summary>
    public required string NewStatus { get; init; }

    /// <summary>
    /// Whether the call moved the tenant to a different status;
    /// <see langword="false"/> when the tenant was already in the requested
    /// status and the call was an idempotent no-op.
    /// </summary>
    public required bool Changed { get; init; }
}
