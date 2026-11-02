using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A2;

/// <summary>
/// Runs A2's numbered steps: each in a coverage scope of its own (<c>A2.n</c>), so a later
/// step's failure keeps the evidence of the steps before it, and each substep a checkpoint
/// filed in the journey's manifest under the step id <c>a2-n</c>.
/// </summary>
/// <param name="session">olivia's session.</param>
internal sealed class A2Steps(PersonaSession session)
{
    /// <summary>The journey id: the folder name, lower case.</summary>
    public const string JourneyId = "a2";

    private readonly JourneyRecorder _recorder = JourneyRecorder.For(JourneyId);
    private string? _step;

    /// <summary>Runs step <paramref name="number"/>.</summary>
    /// <param name="number">The step number, as the issue numbers it.</param>
    /// <param name="title">What the step does.</param>
    /// <param name="body">The step.</param>
    public async Task RunAsync(int number, string title, Func<Task> body)
    {
        ArgumentNullException.ThrowIfNull(body);
        using var scope = Coverage.BeginScope($"A2.{number}");
        _step = $"{JourneyId}-{number}";
        TestContext.Progress.WriteLine($"A2.{number} {title}");
        try
        {
            await body();
        }
        finally
        {
            _step = null;
        }

        scope.Complete();
    }

    /// <summary>A checkpoint of the running step: a screenshot in the manifest, then its checks.</summary>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="title">The caption: what the state shows.</param>
    public Task ShotAsync(string substep, string title) =>
        session.SubstepAsync(_recorder, _step ?? throw new InvalidOperationException("A checkpoint is taken inside a step."), substep, title);
}
