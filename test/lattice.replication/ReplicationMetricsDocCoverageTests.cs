using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Asserts every instrument declared in the replication package source is listed, by
/// its exact dotted name, in the instrument-to-panel reference map, reusing the
/// shared <see cref="MetricsDocCoverageTestsBase"/> drift guard so a new replication
/// instrument cannot ship without a documentation entry.
/// </summary>
/// <remarks>
/// Replication is the one instrument-publishing package that carried neither a
/// doc-coverage guard nor a <c>MeterDashboardCoverageTestsBase</c> dashboard guard,
/// so before this fixture nothing at all held its 64 instruments to any reference.
/// </remarks>
[TestFixture]
public sealed class ReplicationMetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    protected override IEnumerable<string> SourceRoots => new[] { "src/lattice.replication" };

    protected override IEnumerable<string> DocRelativePaths => new[]
    {
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    };

    // The replication meter name shares the instrument prefix but is not itself an instrument.
    protected override IReadOnlySet<string> NonInstrumentLiterals { get; } =
        new HashSet<string>(StringComparer.Ordinal)
        {
            "orleans.lattice.replication",
        };
}
