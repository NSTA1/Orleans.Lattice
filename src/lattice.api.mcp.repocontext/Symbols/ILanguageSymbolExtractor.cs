namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The per-language seam for symbol extraction. One implementation handles exactly
/// one source language (keyed by the identifier <see cref="LanguageClassifier"/>
/// produces), so support for a new language is added by registering another
/// implementation without touching the reconcile pipeline. The shipped binding is
/// <see cref="CSharpSymbolExtractor"/>; other languages fall through to no output
/// until their extractor is added.
/// </summary>
internal interface ILanguageSymbolExtractor
{
    /// <summary>
    /// The language identifier this extractor handles, matching the value
    /// <see cref="LanguageClassifier.Classify(string)"/> assigns (for example
    /// <c>"csharp"</c>).
    /// </summary>
    string Language { get; }

    /// <summary>
    /// Extracts the declared symbols from one source file's text.
    /// </summary>
    /// <param name="relativePath">The repository-relative file path, used only for
    /// diagnostics and stable ordering. Must not be <see langword="null"/>.</param>
    /// <param name="content">The file's decoded source text. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>The symbols declared in the file, or an empty list when none are
    /// found or the text cannot be parsed.</returns>
    IReadOnlyList<ExtractedSymbol> Extract(string relativePath, string content);
}
