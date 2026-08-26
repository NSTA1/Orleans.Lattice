namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content view of a tenant's resource quotas and burst
/// allowance, embedded in a tenant status report. Each resource ceiling is
/// <see langword="null"/> when that dimension is unbounded; the reserved default
/// tenant, and any tenant whose quotas have never been authored, reports every
/// dimension unbounded.
/// </summary>
internal sealed record McpTenantQuotasView
{
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
