using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Repository-scoped tracker-identifier hygiene gate for this test project.
/// The scan logic lives in the shared base; this fixture only binds the
/// project's scan scope.
/// </summary>
[TestFixture]
public sealed class RoadmapIdentifierHygieneTests : RoadmapIdentifierHygieneTestsBase
{
    /// <inheritdoc />
    protected override HygieneScanScope Scope { get; } =
        HygieneScanScope.ForSlice("src/lattice.api.state", "test/lattice.api.state");
}
