using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C1;

/// <summary>
/// C1's steps and checkpoints: each numbered step of issue #5389 runs inside its own coverage
/// scope, and each distinct UI state inside it is a checkpoint filed under step
/// <c>c1-&lt;n&gt;</c> and captioned <c>C1.&lt;n&gt;.&lt;m&gt; - &lt;what it shows&gt;</c>.
/// </summary>
internal sealed class C1Steps
{
    /// <summary>The journey id the screenshots and manifest are filed under.</summary>
    public const string JourneyId = "c1";

    private readonly JourneyRecorder _recorder = JourneyRecorder.For(JourneyId);
    private int _step;
    private int _substep;

    /// <summary>Runs step <paramref name="number"/> inside its own coverage scope.</summary>
    /// <param name="number">The step's number in the issue.</param>
    /// <param name="title">The step's title.</param>
    /// <param name="body">The step.</param>
    public async Task StepAsync(int number, string title, Func<Task> body)
    {
        _step = number;
        _substep = 0;
        TestContext.Progress.WriteLine($"C1.{number} - {title}");
        using var scope = Coverage.BeginScope($"C1.{number}");
        await body();
        scope.Complete();
    }

    /// <summary>Captures and checks the next checkpoint of the running step.</summary>
    /// <param name="session">The persona's session in the state to capture.</param>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="caption">What the state shows.</param>
    public Task CaptureAsync(PersonaSession session, string substep, string caption)
    {
        if (_step == 0)
        {
            throw new InvalidOperationException("A C1 checkpoint is taken inside a step.");
        }

        _substep++;
        return session.SubstepAsync(_recorder, $"c1-{_step}", substep, $"C1.{_step}.{_substep} - {caption}");
    }
}
