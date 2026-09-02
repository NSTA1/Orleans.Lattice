using Orleans.Lattice.Testing.Docs;

namespace Orleans.Lattice.Vector.Tests.Docs;

/// <summary>
/// Compiles this package's own <c>docs/lattice.vector</c> <c>`csharp verify</c>
/// snippets against its product surface. The compilation logic lives in the
/// shared <see cref="DocsSnippetCompilationTestsBase"/>; this fixture only binds
/// the package's docs scope, so the snippets are verified whenever this
/// package's test project runs in CI.
/// </summary>
/// <remarks>
/// Without this fixture the snippets fall to the core scope, which does not
/// reference this assembly, so a snippet naming a type from
/// <c>Orleans.Lattice.Vector</c> fails to compile there. The docs root is
/// therefore also registered in <c>CoreDocsSnippetScope.ClaimedPackageDocsRoots</c>
/// so each snippet is compiled exactly once, by its owning project.
/// </remarks>
[TestFixture]
[Category("Docs")]
public sealed class DocsSnippetCompilationTests : DocsSnippetCompilationTestsBase
{
    /// <inheritdoc />
    protected override DocsSnippetScope Scope { get; } =
        DocsSnippetScope.ForPackage("docs/lattice.vector");
}
