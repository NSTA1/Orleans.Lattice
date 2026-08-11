namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The read-only adapter behind the repository-context tool module. At this
/// foundation stage it exposes a single <c>repocontext_health</c> probe that
/// proves the module is registered and that the caller cleared the fail-closed
/// authorization gate; the capture, maintenance, and retrieval handlers land in
/// later work.
/// </summary>
/// <remarks>
/// The health result is invariant, so it is built once and reused on every call:
/// the probe adds no per-invocation allocation to the hot path.
/// </remarks>
internal static class RepoContextToolHandlers
{
    /// <summary>
    /// The single, shared health result. It carries no caller- or
    /// request-specific state, so one immutable instance serves every session and
    /// no allocation occurs per <c>tools/call</c>.
    /// </summary>
    private static readonly RepoContextHealthResult Healthy = new()
    {
        Available = true,
        Group = LatticeApiMcpGroupCapabilityMap.DisplayName(LatticeApiMcpGroup.RepoContext),
        Status = "The Orleans.Lattice repository-context MCP surface is registered and reachable.",
    };

    /// <summary>
    /// Reports that the repository-context surface is available to the caller.
    /// Reaching this handler means the caller was advertised the tool and cleared
    /// the authorization gate, so it always returns the ready result.
    /// </summary>
    /// <returns>The shared, immutable health result.</returns>
    public static RepoContextHealthResult Health() => Healthy;
}
