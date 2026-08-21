using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Integration.Tests.Hygiene;

/// <summary>
/// Scans <c>test/lattice.integration</c> for em-dash characters (U+2014).
/// See <see cref="EmDashHygieneTestsBase"/> for the scan rule.
/// </summary>
[TestFixture]
public sealed class EmDashHygieneTests : EmDashHygieneTestsBase
{
    /// <inheritdoc />
    protected override HygieneScanScope Scope { get; } =
        HygieneScanScope.ForSlice("test/lattice.integration");
}
