using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// The repository ownership registry for the content-hygiene gates. The core
/// test project owns its own slice (<c>src/lattice</c> + <c>test/lattice</c>)
/// plus every repo-level file that no package owns. <see cref="AllPackageSliceRoots"/>
/// lists every project's slice directories so the core repo-level scan skips
/// them - each package's slice is scanned exactly once, by its owning project,
/// while orphan directories under <c>test/</c> (such as <c>test/shared</c> and
/// sample test silos) remain covered by the core repo-level scan.
/// </summary>
internal static class CoreHygieneScope
{
    /// <summary>Slice directories owned by the core test project.</summary>
    internal static readonly string[] CoreSliceRoots = { "src/lattice", "test/lattice" };

    /// <summary>
    /// Every package slice directory owned by some project. Kept in sync with
    /// the per-project <c>Hygiene/</c> subclasses; adding a new family test
    /// project means adding its slice roots here so the core repo-level scan
    /// does not double-scan them.
    /// </summary>
    internal static readonly string[] AllPackageSliceRoots =
    {
        "src/lattice", "test/lattice",
        "src/lattice.api.state", "test/lattice.api.state",
        "src/lattice.api.state.grpc", "test/lattice.api.state.grpc",
        "src/lattice.dashboards", "test/lattice.dashboards",
        "src/lattice.explorer", "test/lattice.explorer",
        "src/lattice.explorer.entra.web", "test/lattice.explorer.entra.web",
        "src/lattice.replication", "test/lattice.replication",
        "src/lattice.replication.grpc", "test/lattice.replication.grpc",
        "src/lattice.storage.azuretable", "test/lattice.storage.azuretable",
        "src/lattice.storage.file", "test/lattice.storage.file",
        "test/lattice.integration",
        "src/lattice.backup.azureblob", "test/lattice.backup.azureblob",
        "src/lattice.caching.azureblob", "test/lattice.caching.azureblob",
        "src/lattice.scaling", "test/lattice.scaling",
        "test/microbench",
    };

    /// <summary>The shared core scope passed to every core content-hygiene fixture.</summary>
    internal static readonly HygieneScanScope Value =
        HygieneScanScope.ForCore(CoreSliceRoots, AllPackageSliceRoots);
}
