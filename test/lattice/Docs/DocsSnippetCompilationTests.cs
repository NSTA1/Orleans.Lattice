using Orleans.Lattice.Testing.Docs;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Compiles the core library's own <c>docs/lattice</c> snippets, the repo-root
/// <c>README.md</c>, and every <c>docs/&lt;package&gt;</c> subtree not claimed by
/// a package project's fixture (see <see cref="CoreDocsSnippetScope"/>). The
/// compilation logic lives in the shared <see cref="DocsSnippetCompilationTestsBase"/>;
/// this fixture only binds the core scope.
/// </summary>
[TestFixture]
[Category("Docs")]
public sealed class DocsSnippetCompilationTests : DocsSnippetCompilationTestsBase
{
    /// <inheritdoc />
    protected override DocsSnippetScope Scope { get; } =
        DocsSnippetScope.ForCore(CoreDocsSnippetScope.ClaimedPackageDocsRoots);
}
