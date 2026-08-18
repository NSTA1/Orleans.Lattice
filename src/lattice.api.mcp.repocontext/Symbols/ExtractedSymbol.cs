namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A single symbol a language extractor found in one source file: its
/// fully-qualified name, structural <see cref="SymbolKind"/>, the 1-based line span
/// it occupies, a concise declaration signature, and a content digest of its body.
/// This is an in-memory extraction artefact only - it never crosses an Orleans wire
/// (the persisted shape is <see cref="SymbolRecord"/>), so it carries no
/// serialization attributes.
/// </summary>
/// <param name="FullyQualifiedName">The dotted, fully-qualified symbol name,
/// disambiguated by parameter types for overloadable members so it is unique within
/// a repository (it is the symbol record's key component). Must not be empty.</param>
/// <param name="Kind">The structural classification captured at extraction.</param>
/// <param name="StartLine">The 1-based line the declaration starts on.</param>
/// <param name="EndLine">The 1-based line the declaration ends on.</param>
/// <param name="Signature">A concise, single-line declaration signature (modifiers,
/// keyword or return type, name, and parameter list) for display.</param>
/// <param name="BodyDigest">A stable content digest of the symbol's declaration text
/// (see <see cref="FileDigest"/>), so an unchanged symbol re-extracts to the same
/// value.</param>
internal readonly record struct ExtractedSymbol(
    string FullyQualifiedName,
    SymbolKind Kind,
    int StartLine,
    int EndLine,
    string Signature,
    string BodyDigest)
{
    /// <summary>
    /// The simple (unqualified) type-names this symbol references syntactically -
    /// its base types, and the types named in its members' signatures and bodies
    /// (parameter, return, field, and property types; generic arguments; object
    /// creations; <c>typeof</c>/cast targets). Populated only for type-level
    /// symbols; every other kind carries the empty list. Resolution is purely
    /// syntactic (no semantic model), so a name is the identifier as written, not a
    /// fully-qualified symbol - two distinct types sharing a simple name are
    /// therefore indistinguishable here. The reconciler folds these into the
    /// symbol record's add-wins reference set and the reverse cross-reference index.
    /// Never <see langword="null"/> for an extractor-produced value; a
    /// <c>default</c> struct carries <see langword="null"/>, so readers treat null as
    /// empty.
    /// </summary>
    public IReadOnlyList<string> ReferencedNames { get; init; } = [];
}
