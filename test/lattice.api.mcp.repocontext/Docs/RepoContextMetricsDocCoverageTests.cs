using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Docs;

/// <summary>
/// Asserts that every instrument this package publishes is named in the
/// <c>Emitted instruments</c> table of
/// <c>docs/lattice.api.mcp.repocontext/retrieval-economics.md</c>, which claims to
/// list the whole surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this guard exists.</b> That table was authored by reading the source, so
/// it could only ever be as complete as the reading was: it shipped naming five of
/// the package's eight instruments while asserting that one scraper subscription
/// covers the whole surface. An incomplete list under a totality claim is worse
/// than no list, because a reader wiring a dashboard from it gets a silently
/// partial one - and measured absence is precisely what this document exists to
/// explain.
/// </para>
/// <para>
/// <b>Why no existing guard caught it.</b>
/// <see cref="MetricsDocCoverageTestsBase"/> already enforced this for the core
/// library, but its scan was hardwired to the <c>orleans.lattice.*</c> naming
/// scheme. This package publishes under <c>repocontext.*</c>, so the shared guard
/// matched nothing here and no fixture could have covered it. The prefix is now a
/// virtual on the base, which is the reusable half of the fix: any future package
/// on its own naming scheme opts in by overriding it rather than by hand-copying a
/// scanner. The base's non-empty assertion is the anti-vacuity floor - a prefix
/// that matched nothing would fail there rather than pass silently.
/// </para>
/// <para>
/// Carries no category, matching <c>MetricsDocCoverageTests</c> and
/// <c>RepoContextContainerDocumentationTests</c>: a cheap documentation-drift guard
/// belongs in the default non-chaos lane, not behind the slower
/// <c>[Category("Docs")]</c> Roslyn snippet tier.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextMetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    /// <inheritdoc />
    protected override string InstrumentNamePrefix => "repocontext";

    /// <inheritdoc />
    protected override IEnumerable<string> SourceRoots { get; } =
        ["src/lattice.api.mcp.repocontext"];

    /// <inheritdoc />
    protected override IEnumerable<string> DocRelativePaths { get; } =
        ["docs/lattice.api.mcp.repocontext/retrieval-economics.md"];
}
