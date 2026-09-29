using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Every test project the coverage lane measures must reference the
/// <c>coverlet.collector</c> package, which supplies the
/// <c>XPlat Code Coverage</c> data collector.
/// <para>
/// <b>What went wrong.</b> <c>Orleans.Lattice.Api.Apps.Grpc.Tests</c> was created
/// without it. vstest logs <i>"Could not find data collector 'XPlat Code Coverage'"</i>
/// and still passes, so the package was silently missing from main's coverage report
/// from its first night. The coverage lane now withholds the whole upload when a suite
/// produces no report (<see cref="CiCoverageUploadCompletenessTests"/>). That turned
/// the gap into a stalled coverage report, found only by a nightly run. This fixture
/// moves the check to the pull request that adds the project.
/// </para>
/// </summary>
[TestFixture]
public sealed class CoverageCollectorReferenceTests
{
    /// <summary>
    /// The coverage lane selects <c>test/**/*Tests.csproj</c> except these two; keep
    /// this list in step with the <c>find</c> in <c>.github/workflows/coverage.yml</c>.
    /// </summary>
    private static readonly string[] ExcludedSegments = ["microbench", "azure-throughput-silo"];

    [Test]
    public void Every_measured_test_project_references_the_coverage_collector()
    {
        var root = HygieneRepository.FindRepoRoot();
        var projects = Directory
            .EnumerateFiles(Path.Combine(root, "test"), "*Tests.csproj", SearchOption.AllDirectories)
            .Where(p => !p.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Any(segment => ExcludedSegments.Contains(segment, StringComparer.Ordinal)))
            .ToList();

        Assert.That(projects, Has.Count.GreaterThan(10),
            "expected to find the repository's test projects; if this scan matches almost nothing, "
            + "it has stopped guarding them");

        var missing = projects
            .Where(p => !File.ReadAllText(p).Contains("\"coverlet.collector\"", StringComparison.Ordinal))
            .Select(p => Path.GetRelativePath(root, p).Replace('\\', '/'))
            .ToList();

        Assert.That(missing, Is.Empty,
            "these test projects do not reference coverlet.collector, so the nightly coverage lane "
            + "cannot measure them and withholds main's whole Codecov upload. Add "
            + "<PackageReference Include=\"coverlet.collector\" ...> as in any sibling test project.");
    }
}
