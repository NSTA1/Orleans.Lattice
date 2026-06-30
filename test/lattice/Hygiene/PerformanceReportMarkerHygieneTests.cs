using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Performance-report marker hygiene gate. The validated doc
/// (docs/lattice/performance-single-silo.md) is a repo-level file owned by the
/// core test project, so this fixture lives only here. The contract logic
/// lives in the shared base.
/// </summary>
[TestFixture]
public sealed class PerformanceReportMarkerHygieneTests : PerformanceReportMarkerHygieneTestsBase
{
}
