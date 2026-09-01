namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// One entry in the Explorer's glossary: a term, the one line that explains it,
/// and where to read more.
/// </summary>
/// <remarks>
/// An entry is immutable and every entry is constructed once, when
/// <see cref="ExplorerGlossary"/> is initialised, so looking one up allocates
/// nothing.
/// </remarks>
public sealed record ExplorerTerm
{
    /// <summary>
    /// The stable identifier, from <see cref="ExplorerTermIds"/>. Also suitable
    /// as an element-id prefix for the help disclosure that renders the term.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// The short human label, in sentence case - what a heading, a badge
    /// expansion or a help trigger calls the term.
    /// </summary>
    public required string Label { get; init; }

    /// <summary>
    /// One line explaining what the term means, written for someone meeting it
    /// for the first time. Complete sentences, no jargon that is not itself in
    /// the glossary.
    /// </summary>
    public required string Explanation { get; init; }

    /// <summary>
    /// The repository-relative documentation path that covers the term in full,
    /// from <see cref="ExplorerDocsLinks"/>, or <see langword="null"/> when no
    /// document covers it.
    /// </summary>
    public string? DocsLink { get; init; }

    /// <summary>Whether this term has somewhere further to read.</summary>
    public bool HasDocsLink => !string.IsNullOrEmpty(DocsLink);
}
