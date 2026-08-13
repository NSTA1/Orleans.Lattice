namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The language-agnostic front door to symbol extraction: it routes a file to the
/// <see cref="ILanguageSymbolExtractor"/> registered for the file's language and
/// returns an empty result when no extractor is registered for that language. This
/// is the single dependency the reconcile pipeline takes, so adding a language is a
/// registration change only.
/// </summary>
internal interface ISymbolExtractor
{
    /// <summary>
    /// Extracts the declared symbols from one source file, dispatching by
    /// <paramref name="language"/>.
    /// </summary>
    /// <param name="relativePath">The repository-relative file path. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="language">The language identifier
    /// (<see cref="LanguageClassifier.Classify(string)"/>); an unrecognised or
    /// unsupported language yields an empty result. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="content">The file's decoded source text. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>The symbols declared in the file, or an empty list when the language
    /// has no registered extractor or none are found.</returns>
    IReadOnlyList<ExtractedSymbol> Extract(string relativePath, string language, string content);

    /// <summary>
    /// Reports whether a symbol extractor is registered for
    /// <paramref name="language"/>, so a caller can cheaply skip reading a file that
    /// no extractor would parse.
    /// </summary>
    /// <param name="language">The language identifier to probe.</param>
    bool Supports(string language);
}
