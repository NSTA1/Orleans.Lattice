namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_set_quotas</c> tool:
/// the tenant id whose quotas were authored and the quotas now in effect for it,
/// so an operator can confirm the applied allocation without a follow-up read.
/// Each resource ceiling is <see langword="null"/> when that dimension is
/// unbounded.
/// </summary>
internal sealed record McpTenantSetQuotasResult
{
    /// <summary>The tenant id whose quotas were authored.</summary>
    public required string TenantId { get; init; }

    /// <summary>The maximum total stored value bytes, or <see langword="null"/> for unbounded.</summary>
    public long? MaxBytes { get; init; }

    /// <summary>The maximum total live key count, or <see langword="null"/> for unbounded.</summary>
    public long? MaxKeys { get; init; }

    /// <summary>The maximum resident memory in bytes, or <see langword="null"/> for unbounded.</summary>
    public long? MaxMemoryBytes { get; init; }

    /// <summary>The maximum number of trees the tenant may own, or <see langword="null"/> for unbounded.</summary>
    public long? MaxTreeCount { get; init; }

    /// <summary>The maximum sustained operations per second, or <see langword="null"/> for unbounded.</summary>
    public long? MaxOpsPerSecond { get; init; }

    /// <summary>The transient burst headroom above the bounded ceilings, as a percentage (<c>0</c> for none).</summary>
    public required int BurstPercent { get; init; }

    /// <summary><see langword="true"/> when every resource dimension is unbounded.</summary>
    public required bool IsUnbounded { get; init; }
}
