namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One independently reusable unit of a <see cref="RepoContextContextEntry"/>: the
/// smallest slice of a file the <c>repocontext_context</c> tool delivers and charges
/// for on its own. A file carries a single pointer unit at
/// <see cref="RepoContextContextDetail.Paths"/>, a single body span at
/// <see cref="RepoContextContextDetail.Slices"/>, or one unit per declared symbol at
/// <see cref="RepoContextContextDetail.Outline"/> (which is why an outline entry can
/// deliver some symbols while suppressing others the caller already holds).
/// <para>
/// Each unit carries a stable, opaque <see cref="Receipt"/> a caller can hand back on
/// a later call (via the tool's <c>seen</c> parameter, or implicitly through a named
/// <c>session</c>) to suppress exactly this unit without re-paying for it. The receipt
/// is version-bound: it changes when the file's content changes, so a stale receipt
/// never suppresses a unit whose content has moved on.
/// </para>
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans grain
/// message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextContextUnit
{
    /// <summary>The stable, opaque receipt naming this delivered unit and its file version.</summary>
    public required string Receipt { get; init; }

    /// <summary>
    /// The unit kind: <c>"pointer"</c> (a path at paths detail), <c>"span"</c> (a whole
    /// body at slices detail), or <c>"outline"</c> (one declared symbol at outline
    /// detail).
    /// </summary>
    public required string Kind { get; init; }

    /// <summary>
    /// The fully-qualified name of the declared symbol this unit renders, for an
    /// <c>"outline"</c> unit; <see langword="null"/> for a pointer or span unit.
    /// </summary>
    public string? Symbol { get; init; }

    /// <summary>The exact BPE token count of this unit's <see cref="Content"/> under the shared tokenizer profile.</summary>
    public required int TokenCount { get; init; }

    /// <summary>The rendered content of this unit at the entry's detail level. Never <see langword="null"/>.</summary>
    public required string Content { get; init; }
}
