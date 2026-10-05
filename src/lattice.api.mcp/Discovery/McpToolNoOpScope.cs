namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The shared no-op scope the per-invocation MCP tool scopes
/// (<see cref="McpToolCredentialScope"/> and
/// <see cref="McpToolActiveTenantScope"/>) return on their cold path, when
/// there is no ambient state to stamp, so the tool's <c>using</c> stays
/// unconditional without allocating a scope per call.
/// </summary>
internal sealed class McpToolNoOpScope : IDisposable
{
    /// <summary>The single shared instance.</summary>
    public static readonly McpToolNoOpScope Instance = new();

    private McpToolNoOpScope()
    {
    }

    /// <inheritdoc />
    public void Dispose()
    {
    }
}
