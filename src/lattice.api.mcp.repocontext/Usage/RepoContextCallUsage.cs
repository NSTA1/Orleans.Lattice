namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The usage figures recorded for a single successfully answered repocontext call:
/// the exact response token cost the caller was charged, and a conservative estimate of
/// the whole-file read tokens the answer made unnecessary. Purely a measurement record -
/// it never influences the answer a tool returns.
/// </summary>
/// <param name="Command">The MCP tool name the figures are attributed to (a low-cardinality label).</param>
/// <param name="ResponseTokens">The exact BPE token cost of the delivered response.</param>
/// <param name="ReplacedReadTokens">
/// The conservatively estimated whole-file read tokens the delivered content replaced.
/// Credited only for whole-file-equivalent delivery; zero for discovery, partial detail, and reused content.
/// </param>
internal readonly record struct RepoContextCallUsage(string Command, int ResponseTokens, int ReplacedReadTokens)
{
    /// <summary>
    /// The net tokens saved by this call: the read tokens replaced minus the response tokens spent.
    /// May be negative when a delivery cost more than the whole-file read it replaced.
    /// </summary>
    public int NetSavedTokens => ReplacedReadTokens - ResponseTokens;
}
