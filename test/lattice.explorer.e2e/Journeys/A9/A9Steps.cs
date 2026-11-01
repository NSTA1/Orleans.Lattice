using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>
/// Files A9's checkpoints in its journey (<c>a9</c>) under the step that is running, and
/// gives each step a coverage scope of its own, so a later step's failure keeps the earlier
/// steps' evidence.
/// </summary>
/// <param name="session">Olivia's session.</param>
internal sealed class A9Steps(PersonaSession session)
{
    /// <summary>The journey id the screenshots are filed under.</summary>
    public const string JourneyId = "a9";

    private readonly JourneyRecorder _recorder = JourneyRecorder.For(JourneyId);
    private string? _step;

    /// <summary>Runs step <c>A9.<paramref name="number"/></c> inside its own coverage scope.</summary>
    /// <param name="number">The step number.</param>
    /// <param name="title">The step's title.</param>
    /// <param name="body">The step.</param>
    public async Task StepAsync(int number, string title, Func<Task> body)
    {
        _step = $"a9-{number}";
        TestContext.Out.WriteLine($"A9.{number} - {title}");
        using var scope = Coverage.BeginScope($"A9.{number}");
        await body();
        scope.Complete();
    }

    /// <summary>Captures and checks a checkpoint of the running step.</summary>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="title">What the screenshot shows.</param>
    public Task SubstepAsync(string substep, string title) => SubstepAsync(session, substep, title);

    /// <summary>Captures and checks a checkpoint of the running step in another persona's session.</summary>
    /// <param name="who">The session to photograph.</param>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="title">What the screenshot shows.</param>
    public Task SubstepAsync(PersonaSession who, string substep, string title) =>
        who.SubstepAsync(_recorder, _step ?? throw new InvalidOperationException("No A9 step is running."), substep, title);
}
