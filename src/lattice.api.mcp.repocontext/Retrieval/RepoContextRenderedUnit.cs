namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The internal, rendering-time counterpart of <see cref="RepoContextContextUnit"/>:
/// one reusable unit together with the text it renders. The rendered text is needed
/// to assemble an entry's delivered
/// <see cref="RepoContextContextEntry.Content"/> and to measure its token cost, but
/// it is deliberately <b>not</b> carried on the public wire DTO - an entry's
/// <c>Content</c> is exactly the join of its units' text, so publishing both would
/// send every byte of source twice inside a single payload (issue #1811).
/// </summary>
/// <param name="Receipt">The stable, opaque receipt naming this delivered unit and its file version.</param>
/// <param name="Kind">The unit kind: <c>"pointer"</c>, <c>"span"</c>, or <c>"outline"</c>.</param>
/// <param name="Symbol">The declared symbol's fully-qualified name for an outline unit; otherwise <see langword="null"/>.</param>
/// <param name="TokenCount">The exact BPE token count of <paramref name="Content"/>.</param>
/// <param name="Content">The rendered text of this unit at the entry's detail level.</param>
internal readonly record struct RepoContextRenderedUnit(
    string Receipt,
    string Kind,
    string? Symbol,
    int TokenCount,
    string Content)
{
    /// <summary>
    /// Projects this rendered unit to the public wire DTO, dropping the rendered text
    /// (which the owning entry's <see cref="RepoContextContextEntry.Content"/> already
    /// carries) and keeping only what reuse economics needs.
    /// </summary>
    /// <returns>The public unit descriptor.</returns>
    internal RepoContextContextUnit ToWire() => new()
    {
        Receipt = Receipt,
        Kind = Kind,
        Symbol = Symbol,
        TokenCount = TokenCount,
    };
}
