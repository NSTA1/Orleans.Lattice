using Orleans.Lattice.Testing.Docs;

namespace Orleans.Lattice.Api.Mcp.Tests.Docs;

/// <summary>
/// Compiles this package's own <c>docs/lattice.api.mcp</c> <c>`csharp verify</c> snippets
/// against its product surface. The compilation logic lives in the shared
/// <see cref="DocsSnippetCompilationTestsBase"/>; this fixture only binds the
/// package's docs scope so the snippets are verified whenever this package's
/// test project runs in CI.
/// </summary>
[TestFixture]
[Category("Docs")]
public sealed class DocsSnippetCompilationTests : DocsSnippetCompilationTestsBase
{
    /// <inheritdoc />
    protected override DocsSnippetScope Scope { get; } =
        DocsSnippetScope.ForPackage("docs/lattice.api.mcp");
}
