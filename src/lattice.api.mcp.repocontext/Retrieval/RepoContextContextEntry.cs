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
    /// When reuse suppressed some of the file's units, this is the newline-joined
    /// content of only the <see cref="Units"/> that survived - the caller already holds
    /// the rest.
    /// </summary>
    public required string Content { get; init; }

    /// <summary>
    /// The stable content hash of the packed file version, or <see langword="null"/>
    /// when the file body was not available to hash (so no reuse tracking applies to
    /// this entry). A caller can pair this with the path as a <c>known</c> possession
    /// claim on a later call - but only a version this tool delivered as a whole body
    /// (a slices-detail span) will ever be honoured as whole-file possession.
    /// </summary>
    public string? ContentHash { get; init; }

    /// <summary>
    /// The individually reusable units that make up this entry's delivered
    /// <see cref="Content"/>, each with its own opaque receipt for later suppression.
    /// A paths or slices entry carries a single unit; an outline entry carries one unit
    /// per delivered symbol. Empty when reuse tracking did not apply to this call. Never
    /// <see langword="null"/>.
    /// <para>
    /// A unit is a <b>descriptor, not a copy of the text</b>: the units correspond
    /// one-to-one, in order, to the newline-separated segments of <see cref="Content"/>,
    /// so a caller that needs a unit's text takes the segment at the same index rather
    /// than reading it from the unit. Carrying the text on both would double every byte
    /// of source in the payload (issue #1811).
    /// </para>
    /// </summary>
    public IReadOnlyList<RepoContextContextUnit> Units { get; init; } = Array.Empty<RepoContextContextUnit>();
}
