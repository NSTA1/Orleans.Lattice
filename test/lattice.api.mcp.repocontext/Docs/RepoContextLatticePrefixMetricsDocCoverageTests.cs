using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Docs;

/// <summary>
/// Asserts that every instrument this package publishes under the
/// <c>lattice.repocontext.*</c> naming scheme is named in the <c>Emitted instruments</c>
/// table of <c>docs/lattice.api.mcp.repocontext/retrieval-economics.md</c> and in the
/// dashboards panel map.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a second fixture for one package.</b>
/// <see cref="RepoContextMetricsDocCoverageTests"/> guards the package's
/// <c>repocontext.*</c> instruments, but each fixture in the guard family scans a
/// single instrument-name prefix, and the enrolment gate is keyed by package rather
/// than by prefix. An instrument this already-enrolled package publishes under a
/// second prefix was therefore visible to neither: the memory-restore counter,
/// <c>lattice.repocontext.memory.restore</c>, shipped with no documentation row while
/// the table claimed to list every instrument the package publishes. This fixture
/// extends the same two-way guard to the second prefix.
/// </para>
/// <para>
/// Carries no category, matching <see cref="RepoContextMetricsDocCoverageTests"/>.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextLatticePrefixMetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    /// <inheritdoc />
    protected override string InstrumentNamePrefix => "lattice.repocontext";

    /// <inheritdoc />
    protected override IEnumerable<string> SourceRoots { get; } =
        ["src/lattice.api.mcp.repocontext"];

    /// <inheritdoc />
    protected override IEnumerable<string> DocRelativePaths { get; } =
    [
        "docs/lattice.api.mcp.repocontext/retrieval-economics.md",
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    ];
}
