namespace Orleans.Lattice.Testing.Docs;

/// <summary>
/// Describes which slice of the <c>docs/</c> tree a docs-snippet compilation
/// fixture is responsible for verifying. Each package's test project supplies a
/// scope covering only its own <c>docs/&lt;package&gt;</c> directory, so a single
/// test-project run compiles only that package's snippets - and, crucially, is
/// pulled into CI whenever that package changes. Exactly one project (the core
/// <c>Orleans.Lattice.Tests</c>) additionally owns the repo-root
/// <c>README.md</c> plus every <c>docs/&lt;package&gt;</c> subtree that no
/// package project has claimed, so the union of every project's scope still
/// compiles every <c>```csharp verify</c> snippet in the repository exactly once.
/// </summary>
/// <param name="PackageDocsRoots">
/// Repo-root-relative docs directories this fixture owns (e.g.
/// <c>docs/lattice.membership</c>). Empty for the core scope.
/// </param>
/// <param name="IsCore">
/// When true, the fixture owns the repo-root <c>README.md</c> and every
/// <c>docs/</c> subtree not listed in <see cref="ClaimedPackageDocsRoots"/>.
/// </param>
/// <param name="ClaimedPackageDocsRoots">
/// The registry of every <c>docs/&lt;package&gt;</c> directory owned by some
/// package project's scope. The core fixture skips these (they are compiled by
/// their owning project) and picks up everything else. Empty for non-core scopes.
/// </param>
public sealed record DocsSnippetScope(
    IReadOnlyList<string> PackageDocsRoots,
    bool IsCore,
    IReadOnlyList<string> ClaimedPackageDocsRoots)
{
    /// <summary>
    /// Creates a scope for a package project that owns only its own
    /// <c>docs/&lt;package&gt;</c> directory (or directories).
    /// </summary>
    public static DocsSnippetScope ForPackage(params string[] packageDocsRoots) =>
        new(packageDocsRoots, IsCore: false, Array.Empty<string>());

    /// <summary>
    /// Creates the core scope that owns the repo-root <c>README.md</c> plus every
    /// <c>docs/</c> subtree not claimed by a package project.
    /// <paramref name="claimedPackageDocsRoots"/> is the registry of every
    /// package project's owned docs directories, which the core scan skips so
    /// each snippet is compiled exactly once by its owning project.
    /// </summary>
    public static DocsSnippetScope ForCore(IReadOnlyList<string> claimedPackageDocsRoots) =>
        new(Array.Empty<string>(), IsCore: true, claimedPackageDocsRoots);
}
