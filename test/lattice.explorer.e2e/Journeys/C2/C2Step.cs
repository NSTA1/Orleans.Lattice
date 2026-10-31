using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>
/// One numbered step of C2 (<c>C2.&lt;n&gt;</c> in #5390): its checkpoints are filed in the
/// <c>c2</c> manifest under step id <c>c2-&lt;n&gt;</c>, and its coverage hits commit in a scope
/// of its own, so a later step's failure keeps the earlier steps' evidence.
/// </summary>
/// <param name="number">The step's number.</param>
/// <param name="title">The step's title.</param>
internal sealed class C2Step(int number, string title)
{
    /// <summary>The journey id the screenshots are filed under.</summary>
    public const string JourneyId = "c2";

    /// <summary>The step id, such as <c>c2-1</c>.</summary>
    public string Id { get; } = $"{JourneyId}-{number}";

    /// <summary>The step's name in the issue, such as <c>C2.1</c>.</summary>
    public string Name { get; } = $"C2.{number}";

    /// <summary>The step's title.</summary>
    public string Title { get; } = title;

    /// <summary>Captures and checks one checkpoint of this step in <paramref name="session"/>.</summary>
    /// <param name="session">The persona's session.</param>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="caption">What the state shows.</param>
    /// <param name="options">Further masks, or <see langword="null"/>.</param>
    public Task<JourneyManifestEntry> CaptureAsync(PersonaSession session, string substep, string caption, SubstepOptions? options = null) =>
        session.SubstepAsync(JourneyRecorder.For(JourneyId), Id, substep, caption, options);

    /// <summary>Runs <paramref name="body"/> inside the step's coverage scope, which commits only when it finishes.</summary>
    /// <param name="body">The step.</param>
    public async Task RunAsync(Func<C2Step, Task> body)
    {
        ArgumentNullException.ThrowIfNull(body);
        TestContext.Progress.WriteLine($"{Name} - {Title}");
        using var scope = Coverage.BeginScope(Name);
        await body(this);
        scope.Complete();
    }

    /// <summary>Writes a line to the step log, prefixed with the step's name.</summary>
    /// <param name="message">What happened.</param>
    public void Log(string message) => TestContext.Progress.WriteLine($"{Name}: {message}");
}
