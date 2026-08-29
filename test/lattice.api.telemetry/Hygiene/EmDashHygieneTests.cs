using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Repository-scoped emdash hygiene gate for this test project. The scan
/// logic lives in the shared base; this fixture only binds the project's
/// scan scope.
/// </summary>
[TestFixture]
public sealed class EmDashHygieneTests : EmDashHygieneTestsBase
{
    /// <inheritdoc />
    protected override HygieneScanScope Scope { get; } =
        HygieneScanScope.ForSlice("src/lattice.api.telemetry", "test/lattice.api.telemetry");
}
