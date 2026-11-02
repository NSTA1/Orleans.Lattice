using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A4;

/// <summary>
/// A4's numbered steps and their substeps. A chapter runs as one test, so a step cannot be a
/// <see cref="JourneyStepAttribute"/> test method: each step is instead a coverage scope named
/// <c>A4.&lt;n&gt;</c> (so a later step's failure keeps an earlier step's evidence), and each of
/// its substeps a checkpoint filed in journey <c>a4</c> under step <c>a4-&lt;n&gt;</c>, captioned
/// <c>A4.&lt;n&gt;.&lt;k&gt; - &lt;title&gt;</c>.
/// </summary>
/// <param name="session">The persona session whose page each checkpoint captures.</param>
internal sealed class A4Steps(PersonaSession session)
{
    /// <summary>The journey id the screenshots are filed under.</summary>
    public const string JourneyId = "a4";

    private readonly JourneyRecorder _recorder = JourneyRecorder.For(JourneyId);
    private int? _step;
    private int _substep;

    /// <summary>Runs step <paramref name="number"/> inside its own coverage scope.</summary>
    /// <param name="number">The step number, as the issue numbers it.</param>
    /// <param name="body">The step.</param>
    public async Task StepAsync(int number, Func<Task> body)
    {
        ArgumentNullException.ThrowIfNull(body);
        _step = number;
        _substep = 0;
        using var scope = Coverage.BeginScope($"A4.{number}");
        await body();
        scope.Complete();
        _step = null;
    }

    /// <summary>Captures and checks the current step's next substep.</summary>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="title">What the state shows.</param>
    public Task SubstepAsync(string substep, string title)
    {
        var step = _step ?? throw new InvalidOperationException("A substep is taken inside a step.");
        return session.SubstepAsync(_recorder, $"a4-{step}", substep, $"A4.{step}.{++_substep} - {title}");
    }
}
