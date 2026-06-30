namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Describes which slice of the repository a hygiene fixture is responsible
/// for scanning. Each test project supplies a scope covering only its own
/// <c>src/&lt;package&gt;</c> and <c>test/&lt;package&gt;</c> directories, so a
/// single test-project run never scans the whole solution. Exactly one
/// project (the core <c>Orleans.Lattice.Tests</c>) additionally owns the
/// repo-level files that no package owns - <c>docs/</c>, <c>.github/</c>,
/// <c>benchmark/</c>, <c>samples/</c>, <c>tools/</c>, shared test
/// infrastructure, and root files - so the union of every project's scope
/// still covers the entire repository.
/// </summary>
/// <param name="SliceRelativeRoots">
/// Repo-root-relative directories this fixture owns (e.g.
/// <c>src/lattice.replication</c>, <c>test/lattice.replication</c>).
/// </param>
/// <param name="OwnsRepoLevelFiles">
/// When true, the fixture also scans repo-level content that lives outside
/// any package slice listed in <see cref="OtherSliceRoots"/>.
/// </param>
/// <param name="OtherSliceRoots">
/// The full registry of package slice directories owned by some project's
/// slice. The core fixture's repo-level scan skips these (they are covered by
/// their owning project) and picks up everything else - including orphan
/// directories under <c>test/</c> (such as shared test infrastructure) that
/// belong to no package. Empty for non-core scopes.
/// </param>
public sealed record HygieneScanScope(
    IReadOnlyList<string> SliceRelativeRoots,
    bool OwnsRepoLevelFiles,
    IReadOnlyList<string> OtherSliceRoots)
{
    /// <summary>
    /// Creates a scope for a non-core package that owns only its own slice.
    /// </summary>
    public static HygieneScanScope ForSlice(params string[] sliceRelativeRoots) =>
        new(sliceRelativeRoots, OwnsRepoLevelFiles: false, Array.Empty<string>());

    /// <summary>
    /// Creates the core scope that owns its own slice plus all repo-level
    /// files no package owns. <paramref name="allPackageSliceRoots"/> is the
    /// registry of every project's slice directories, which the repo-level
    /// scan skips so each package's slice is scanned exactly once by its
    /// owning project.
    /// </summary>
    public static HygieneScanScope ForCore(
        IReadOnlyList<string> sliceRelativeRoots,
        IReadOnlyList<string> allPackageSliceRoots) =>
        new(sliceRelativeRoots, OwnsRepoLevelFiles: true, allPackageSliceRoots);
}
