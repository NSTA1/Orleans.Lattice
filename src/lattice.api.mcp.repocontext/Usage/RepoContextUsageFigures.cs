namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Computes the usage figures for a successfully answered repocontext call. Read-replacement
/// credit is deliberately conservative: it is awarded only for delivered whole-file-equivalent
/// content (the <c>repocontext_context</c> tool at <c>slices</c> detail, where each delivered
/// entry replaced a whole-file read). Discovery calls, the partial <c>paths</c>/<c>outline</c>
/// detail levels, and any content suppressed or reused via the reuse ledger earn zero credit,
/// because none of them delivered a whole file the caller would otherwise have read.
/// </summary>
internal static class RepoContextUsageFigures
{
    /// <summary>The MCP tool name usage is attributed to for the context-bundle surface.</summary>
    internal const string ContextCommand = "repocontext_context";

    /// <summary>The <see cref="RepoContextContextResult.Detail"/> label that delivers whole-file-equivalent content.</summary>
    private const string SlicesDetail = "slices";

    /// <summary>
    /// Computes the recorded figures for a completed context bundle. The response cost is the
    /// bundle's exact precomputed BPE total; read-replacement credit is the sum of each delivered
    /// entry's whole-file read cost, but only when the bundle was packed at <c>slices</c> detail -
    /// the only level at which each delivered entry is a whole-body replacement. At every other
    /// detail level the delivery is partial, so no read-replacement credit is given. Suppressed and
    /// reused content never appears in <see cref="RepoContextContextResult.Entries"/>, so it is
    /// structurally excluded from the credit and never counted twice.
    /// </summary>
    /// <param name="result">The completed context bundle to derive figures from.</param>
    /// <returns>The usage figures for the call.</returns>
    internal static RepoContextCallUsage ForContextBundle(RepoContextContextResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        var replaced = 0;
        if (string.Equals(result.Detail, SlicesDetail, StringComparison.Ordinal))
        {
            var entries = result.Entries;
            for (var i = 0; i < entries.Count; i++)
            {
                var full = entries[i].FullReadTokenCount;
                if (full is int value and > 0)
                {
                    replaced += value;
                }
            }
        }

        return new RepoContextCallUsage(ContextCommand, result.TotalTokens, replaced);
    }
}
