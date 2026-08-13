using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Repository-scoped em-dash hygiene gate for this test project. The scan
/// logic lives in the shared base; this fixture only binds the project's
/// scan scope.
/// </summary>
[TestFixture]
public sealed class EmDashHygieneTests : EmDashHygieneTestsBase
{
    /// <inheritdoc />
    protected override HygieneScanScope Scope { get; } =
        HygieneScanScope.ForSlice("src/lattice.storage.file", "test/lattice.storage.file");
}
