namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Routes a file to the <see cref="ILanguageSymbolExtractor"/> registered for its
/// language. Composed from every registered per-language extractor at construction;
/// a language with no extractor - the default for everything but C# today - yields
/// an empty result rather than an error, so the reconcile pipeline treats
/// unsupported files as declaring no symbols.
/// </summary>
internal sealed class SymbolExtractorDispatcher : ISymbolExtractor
{
    private readonly IReadOnlyDictionary<string, ILanguageSymbolExtractor> _byLanguage;

    /// <summary>
    /// Builds the dispatcher from the registered per-language extractors.
    /// </summary>
    /// <param name="extractors">The per-language extractors to route to. Must not be
    /// <see langword="null"/>. When two extractors declare the same language the last
    /// one registered wins.</param>
    /// <exception cref="ArgumentNullException"><paramref name="extractors"/> is null.</exception>
    public SymbolExtractorDispatcher(IEnumerable<ILanguageSymbolExtractor> extractors)
    {
        ArgumentNullException.ThrowIfNull(extractors);
        var map = new Dictionary<string, ILanguageSymbolExtractor>(StringComparer.Ordinal);
        foreach (var extractor in extractors)
        {
            map[extractor.Language] = extractor;
        }

        _byLanguage = map;
    }

    /// <inheritdoc />
    public bool Supports(string language)
    {
        ArgumentNullException.ThrowIfNull(language);
        return _byLanguage.ContainsKey(language);
    }

    /// <inheritdoc />
    public IReadOnlyList<ExtractedSymbol> Extract(string relativePath, string language, string content)
    {
        ArgumentNullException.ThrowIfNull(relativePath);
        ArgumentNullException.ThrowIfNull(language);
        ArgumentNullException.ThrowIfNull(content);
        return _byLanguage.TryGetValue(language, out var extractor)
            ? extractor.Extract(relativePath, content)
            : [];
    }
}
