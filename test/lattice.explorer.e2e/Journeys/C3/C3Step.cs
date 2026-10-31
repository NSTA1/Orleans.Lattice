using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>
/// One numbered step of C3 (<c>C3.&lt;n&gt;</c>): a nested coverage scope, so a later step's
/// failure keeps this step's evidence, and the step's checkpoints, filed in the journey's
/// manifest as step <c>c3-&lt;n&gt;</c>.
/// </summary>
internal sealed class C3Step : IDisposable
{
    /// <summary>The journey id the screenshots are filed under.</summary>
    public const string JourneyId = "c3";

    private readonly CoverageScope _scope;

    private C3Step(int number, string title)
    {
        Id = $"c3-{number}";
        _scope = Coverage.BeginScope($"C3.{number}");
        TestContext.Progress.WriteLine($"C3.{number} - {title}");
    }

    /// <summary>The step id in the manifest.</summary>
    public string Id { get; }

    /// <summary>Opens step <paramref name="number"/>.</summary>
    /// <param name="number">The step number from #5391.</param>
    /// <param name="title">The step's title.</param>
    public static C3Step Begin(int number, string title) => new(number, title);

    /// <summary>Captures and checks one checkpoint of this step in <paramref name="session"/>.</summary>
    /// <param name="session">The persona session whose page is in the state to capture.</param>
    /// <param name="substep">The substep id.</param>
    /// <param name="title">The caption: what the state shows.</param>
    public Task<JourneyManifestEntry> ShotAsync(PersonaSession session, string substep, string title) =>
        session.SubstepAsync(JourneyRecorder.For(JourneyId), Id, substep, title);

    /// <summary>Commits the step's coverage hits: every assertion in it has passed.</summary>
    public void Complete() => _scope.Complete();

    /// <inheritdoc />
    public void Dispose() => _scope.Dispose();
}
