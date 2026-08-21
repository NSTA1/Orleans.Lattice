using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Integration.Tests.Hygiene;

/// <summary>
/// Scans <c>test/lattice.integration</c> for retired apply-mode / staging-
/// buffer identifiers. See <see cref="DeletionMandateHygieneTestsBase"/> for
/// the scan rule.
/// </summary>
[TestFixture]
public sealed class DeletionMandateHygieneTests : DeletionMandateHygieneTestsBase
{
    /// <inheritdoc />
    protected override HygieneScanScope Scope { get; } =
        HygieneScanScope.ForSlice("test/lattice.integration");
}
