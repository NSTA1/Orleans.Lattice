namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// What a surface is listing, in the words the copy uses to talk about it.
/// </summary>
/// <remarks>
/// A subject is what makes an empty state specific: "No trees yet" rather than
/// "No items found." <see cref="ExplorerSubjects"/> declares one for every
/// surface the Explorer ships; a consumer with a subject of its own can declare
/// another, and <see cref="ExplorerStateCopy"/> will compose for it.
/// </remarks>
public readonly record struct ExplorerSubject
{
    /// <summary>
    /// A stable id, used to key the pre-built copy. Lower-case and hyphenated.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>The singular noun, lower case: <c>tree</c>.</summary>
    public required string Singular { get; init; }

    /// <summary>The plural noun, lower case: <c>trees</c>.</summary>
    public required string Plural { get; init; }

    /// <summary>
    /// What the surface listing them is called, in the case a heading uses:
    /// <c>Trees</c>.
    /// </summary>
    public required string CollectionLabel { get; init; }

    /// <summary>
    /// The glossary term explaining the subject, from
    /// <see cref="ExplorerTermIds"/>, or <see langword="null"/> when the
    /// glossary does not define one.
    /// </summary>
    public string? TermId { get; init; }

    /// <summary>
    /// Where to read more, from <see cref="ExplorerDocsLinks"/>, or
    /// <see langword="null"/>.
    /// </summary>
    public string? DocsLink { get; init; }

    /// <summary>Whether this is the uninitialised default rather than a declared subject.</summary>
    public bool IsEmpty => Id is null;
}
