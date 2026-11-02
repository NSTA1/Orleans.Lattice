using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A3;

/// <summary>
/// A3's steps: each numbered step of #5374 runs in its own coverage scope (so a later step's
/// failure keeps the earlier steps' evidence) and files its substeps under step id
/// <c>a3-{n}</c> in journey <c>a3</c>'s manifest, in the order they are taken.
/// </summary>
/// <param name="session">olivia's session.</param>
internal sealed class A3Steps(PersonaSession session)
{
    /// <summary>The journey id the screenshots are filed under.</summary>
    public const string JourneyId = "a3";

    private readonly JourneyRecorder _recorder = JourneyRecorder.For(JourneyId);
    private int _step;

    /// <summary>Runs step <paramref name="number"/>; its coverage hits commit only when it finishes.</summary>
    /// <param name="number">The step number of #5374.</param>
    /// <param name="body">The step.</param>
    public async Task StepAsync(int number, Func<Task> body)
    {
        _step = number;
        using var scope = Coverage.BeginScope($"A3.{number}");
        await body();
        scope.Complete();
    }

    /// <summary>A checkpoint of the running step: a screenshot in the manifest, then axe, the console and the hosts' logs.</summary>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="title">What the state shows.</param>
    /// <param name="options">Further masks, or <see langword="null"/>.</param>
    public Task SubstepAsync(string substep, string title, SubstepOptions? options = null)
    {
        if (_step == 0)
        {
            throw new InvalidOperationException($"Substep {substep} was taken outside a step.");
        }

        return session.SubstepAsync(_recorder, $"a3-{_step}", substep, title, options);
    }
}
