using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Asserts every instrument declared in the auth package source is listed, by its
/// exact dotted name, in the instrument-to-panel reference map, reusing the shared
/// <see cref="MetricsDocCoverageTestsBase"/> drift guard so a new auth instrument
/// cannot ship without a documentation entry.
/// </summary>
/// <remarks>
/// This complements <see cref="AuthMeterDashboardCoverageTests"/> rather than
/// duplicating it. That guard asserts every instrument is referenced by a dashboard
/// panel; this one asserts every instrument is documented in the reference map a
/// human reads. An instrument can satisfy either without the other.
/// </remarks>
[TestFixture]
public sealed class AuthMetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    protected override IEnumerable<string> SourceRoots => new[] { "src/lattice.auth" };

    protected override IEnumerable<string> DocRelativePaths => new[]
    {
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    };

    // The auth meter name shares the instrument prefix but is not itself an instrument.
    protected override IReadOnlySet<string> NonInstrumentLiterals { get; } =
        new HashSet<string>(StringComparer.Ordinal)
        {
            "orleans.lattice.auth",
        };
}
