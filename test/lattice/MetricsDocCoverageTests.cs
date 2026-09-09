using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Asserts every instrument declared in the core <c>Orleans.Lattice</c> source is
/// documented, by its exact dotted name, in both the metrics catalog and the
/// instrument-to-panel reference map, reusing the shared
/// <see cref="MetricsDocCoverageTestsBase"/> drift guard so a new core instrument
/// cannot ship without a documentation entry.
/// </summary>
[TestFixture]
public sealed class MetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    protected override IEnumerable<string> SourceRoots => new[] { "src/lattice" };

    protected override IEnumerable<string> DocRelativePaths => new[]
    {
        "docs/lattice/metrics.md",
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    };

    // Dotted orleans.lattice.* literals that are not meter instruments: the core
    // meter name is "orleans.lattice" (no trailing segment, so it never matches the
    // scan regex), "orleans.lattice.events" is the change-feed stream namespace,
    // and "orleans.lattice.predicate.v1" is a runtime projection provider key.
    protected override IReadOnlySet<string> NonInstrumentLiterals { get; } =
        new HashSet<string>(StringComparer.Ordinal)
        {
            "orleans.lattice.events",
            "orleans.lattice.predicate.v1",
        };

    // The panel-map backlog this fixture once allow-listed is closed: every core
    // instrument is now carried through into both documents, so there is no
    // IntentionallyUndocumented override. A stale exemption is not inert - it
    // silently disarms the guard for the names it covers, which is the same
    // failure as never enrolling a package at all. Re-add one only with a reason
    // and an owner, and delete it the moment the row lands.
}
