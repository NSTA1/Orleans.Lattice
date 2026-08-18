namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One file packed into a <see cref="RepoContextContextResult"/> bundle: its
/// repository-relative path, the match score and machine-readable reasons carried
/// over from the underlying search hit, the exact BPE token cost of the packed
/// <see cref="Content"/>, the token cost of reading the whole file, and the packed
/// content itself at the bundle's detail level.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextContextEntry
{
    /// <summary>The repository-relative path of the bundled file.</summary>
    public required string Path { get; init; }

    /// <summary>
    /// The match score of the search hit this entry was drawn from, higher meaning a
    /// closer match. Carried through unchanged so the caller can see the ranking that
    /// drove packing order.
    /// </summary>
    public required double Score { get; init; }

    /// <summary>
    /// The machine-readable reasons the underlying record ranked, carried over from
    /// the search hit (see <see cref="RepoContextSearchHit.Reasons"/>). Deterministic,
    /// ordinal-ordered, bounded, and never <see langword="null"/> (empty at worst).
    /// </summary>
    public IReadOnlyList<string> Reasons { get; init; } = Array.Empty<string>();

    /// <summary>
    /// The exact BPE token count of <see cref="Content"/> under the shared tokenizer
    /// profile - the cost this entry contributed to the bundle's
    /// <see cref="RepoContextContextResult.TotalTokens"/>.
    /// </summary>
    public required int TokenCount { get; init; }

    /// <summary>
    /// The token cost of reading the <b>whole</b> file under the configured tokenizer
    /// profile (the indexed per-file count, or a bounded count of the stored content
    /// projection), or <see langword="null"/> when the file was never
    /// content-processed. Lets a caller see how much of the file the packed
    /// <see cref="Content"/> represents, and what a full read would cost.
    /// </summary>
    public required int? FullReadTokenCount { get; init; }

    /// <summary>
    /// The packed content at the bundle's detail level: the file path for
    /// <see cref="RepoContextContextDetail.Paths"/>, the structural skeleton for
    /// <see cref="RepoContextContextDetail.Outline"/>, or the bounded body text for
    /// <see cref="RepoContextContextDetail.Slices"/>. Never <see langword="null"/>.
    /// </summary>
    public required string Content { get; init; }
}
