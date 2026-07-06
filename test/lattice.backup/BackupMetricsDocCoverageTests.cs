using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Asserts every instrument declared in the backup package source is listed, by its
/// exact dotted name, in the instrument-to-panel reference map, reusing the shared
/// <see cref="MetricsDocCoverageTestsBase"/> drift guard so a new backup instrument
/// cannot ship without a documentation entry.
/// </summary>
[TestFixture]
public sealed class BackupMetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    protected override IEnumerable<string> SourceRoots => new[] { "src/lattice.backup" };

    protected override IEnumerable<string> DocRelativePaths => new[]
    {
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    };

    // The backup meter name shares the instrument prefix but is not itself an instrument.
    protected override IReadOnlySet<string> NonInstrumentLiterals { get; } =
        new HashSet<string>(StringComparer.Ordinal)
        {
            "orleans.lattice.backup",
        };
}
