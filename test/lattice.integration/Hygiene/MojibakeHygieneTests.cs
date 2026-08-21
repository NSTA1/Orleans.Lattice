using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Integration.Tests.Hygiene;

/// <summary>
/// Scans <c>test/lattice.integration</c> for byte-level mojibake sequences.
/// See <see cref="MojibakeHygieneTestsBase"/> for the scan rule.
/// </summary>
[TestFixture]
public sealed class MojibakeHygieneTests : MojibakeHygieneTestsBase
{
    /// <inheritdoc />
    protected override HygieneScanScope Scope { get; } =
        HygieneScanScope.ForSlice("test/lattice.integration");
}
