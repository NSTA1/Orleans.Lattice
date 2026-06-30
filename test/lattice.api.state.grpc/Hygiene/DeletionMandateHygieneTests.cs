using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Repository-scoped deletion-mandate hygiene gate for this test project.
/// The scan logic lives in the shared base; this fixture only binds the
/// project's scan scope.
/// </summary>
[TestFixture]
public sealed class DeletionMandateHygieneTests : DeletionMandateHygieneTestsBase
{
    /// <inheritdoc />
    protected override HygieneScanScope Scope { get; } =
        HygieneScanScope.ForSlice("src/lattice.api.state.grpc", "test/lattice.api.state.grpc");
}
