using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U2;

/// <summary>
/// One numbered step of U2 (<c>U2.n</c>): its checkpoints are filed under step id
/// <c>u2-n</c> in the journey's recorder, and the coverage hits its page objects record
/// commit only when the whole step succeeds.
/// </summary>
internal sealed class U2Step
{
    private readonly PersonaSession _session;
    private readonly JourneyRecorder _recorder;

    private U2Step(PersonaSession session, JourneyRecorder recorder, int number, string title)
    {
        _session = session;
        _recorder = recorder;
        Number = number;
        Title = title;
    }

    /// <summary>The step number, as in the issue.</summary>
    public int Number { get; }

    /// <summary>The step's title.</summary>
    public string Title { get; }

    /// <summary>The step id the checkpoints are filed under.</summary>
    public string Id => $"u2-{Number}";

    /// <summary>Runs step <paramref name="number"/> in its own coverage scope.</summary>
    /// <param name="session">The persona's session.</param>
    /// <param name="number">The step number.</param>
    /// <param name="title">The step's title.</param>
    /// <param name="body">The step.</param>
    public static async Task RunAsync(PersonaSession session, int number, string title, Func<U2Step, Task> body)
    {
        using var scope = Coverage.BeginScope($"U2.{number}");
        TestContext.Progress.WriteLine($"U2.{number} - {title}");
        await body(new U2Step(session, JourneyRecorder.For(U2SchemaOwnerJourney.JourneyId), number, title));
        scope.Complete();
    }

    /// <summary>Captures and checks one distinct UI state of the step.</summary>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="title">What the state shows.</param>
    /// <param name="options">Further masks, or <see langword="null"/>.</param>
    public Task SubstepAsync(string substep, string title, SubstepOptions? options = null) =>
        _session.SubstepAsync(_recorder, Id, substep, title, options);
}
