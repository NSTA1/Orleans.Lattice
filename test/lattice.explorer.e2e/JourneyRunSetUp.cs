using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests;

/// <summary>
/// Runs once around every fixture of this assembly: when the run ends it writes the run's
/// committed coverage hits to <c>artifacts/coverage-hits.json</c>, the evidence the coverage gate
/// unions across the isolated and story runs (#5361).
/// </summary>
[SetUpFixture]
public sealed class JourneyRunSetUp
{
    /// <summary>Where the run's hits are written.</summary>
    public static string HitsPath => Path.Combine(JourneyPaths.ProjectDirectory, "artifacts", CoverageHitsDocument.FileName);

    /// <summary>Writes the run's hits.</summary>
    [OneTimeTearDown]
    public void WriteCoverageHits() =>
        CoverageRecorder.Default.WriteTo(HitsPath, JourneyRunModes.Name(JourneyRunModes.Current));
}
