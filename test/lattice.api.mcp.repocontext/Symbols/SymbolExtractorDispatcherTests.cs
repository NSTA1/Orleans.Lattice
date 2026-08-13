namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Symbols;

/// <summary>
/// Unit tests for <see cref="SymbolExtractorDispatcher"/>, the language-routing
/// front door. It composes the registered per-language extractors and returns an
/// empty result for a language none of them handle, so the reconcile pipeline treats
/// an unsupported file as declaring no symbols rather than failing.
/// </summary>
[TestFixture]
public sealed class SymbolExtractorDispatcherTests
{
    private sealed class StubExtractor(string language) : ILanguageSymbolExtractor
    {
        public string Language => language;

        public IReadOnlyList<ExtractedSymbol> Extract(string relativePath, string content) =>
            [new ExtractedSymbol($"{language}:{relativePath}", SymbolKind.Type, 1, 1, content, "d")];
    }

    [Test]
    public void Supports_reports_only_registered_languages()
    {
        var dispatcher = new SymbolExtractorDispatcher([new StubExtractor("csharp")]);

        Assert.Multiple(() =>
        {
            Assert.That(dispatcher.Supports("csharp"), Is.True);
            Assert.That(dispatcher.Supports("python"), Is.False);
        });
    }

    [Test]
    public void Routes_to_the_extractor_for_the_language()
    {
        var dispatcher = new SymbolExtractorDispatcher(
            [new StubExtractor("csharp"), new StubExtractor("go")]);

        var csharp = dispatcher.Extract("A.cs", "csharp", "body");
        var go = dispatcher.Extract("A.go", "go", "body");

        Assert.Multiple(() =>
        {
            Assert.That(csharp.Single().FullyQualifiedName, Is.EqualTo("csharp:A.cs"));
            Assert.That(go.Single().FullyQualifiedName, Is.EqualTo("go:A.go"));
        });
    }

    [Test]
    public void Unsupported_language_yields_empty_rather_than_throwing()
    {
        var dispatcher = new SymbolExtractorDispatcher([new StubExtractor("csharp")]);

        Assert.That(dispatcher.Extract("A.py", "python", "body"), Is.Empty);
    }

    [Test]
    public void Last_registration_wins_for_a_duplicate_language()
    {
        var first = new StubExtractor("csharp");
        var second = new StubExtractor("csharp");

        // Both declare "csharp"; the second registered must win.
        var dispatcher = new SymbolExtractorDispatcher([first, second]);
        var result = dispatcher.Extract("A.cs", "csharp", "body");

        Assert.That(result.Single().FullyQualifiedName, Is.EqualTo("csharp:A.cs"));
        Assert.That(dispatcher.Supports("csharp"), Is.True);
    }

    [Test]
    public void Null_arguments_throw()
    {
        var dispatcher = new SymbolExtractorDispatcher([new StubExtractor("csharp")]);

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new SymbolExtractorDispatcher(null!));
            Assert.Throws<ArgumentNullException>(() => dispatcher.Supports(null!));
            Assert.Throws<ArgumentNullException>(() => dispatcher.Extract(null!, "csharp", "x"));
            Assert.Throws<ArgumentNullException>(() => dispatcher.Extract("A.cs", null!, "x"));
            Assert.Throws<ArgumentNullException>(() => dispatcher.Extract("A.cs", "csharp", null!));
        });
    }
}
