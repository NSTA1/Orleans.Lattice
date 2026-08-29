using Microsoft.Coyote;
using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.SystematicTesting;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Coyote;

/// <summary>
/// A concurrency model that <see cref="CoyoteModelHarness"/> drives under
/// systematic schedule exploration. An implementation expresses the concurrent
/// scenario as explicit cooperative interleaving driven by the supplied
/// <see cref="ICoyoteRuntime"/>'s controlled nondeterminism (for example
/// <see cref="ICoyoteRuntime.RandomBoolean()"/>), and asserts its safety
/// property with <see cref="Microsoft.Coyote.Specifications.Specification.Assert(bool, string, object)"/>.
/// </summary>
/// <remarks>
/// The model is re-run once per exploration iteration <b>on the same instance</b>,
/// so it must build all of its own per-iteration state as locals on each
/// <see cref="Run(ICoyoteRuntime)"/> call and hold no mutable state - instance or
/// static - between runs. A field may hold only immutable configuration; a mutable
/// helper such as a <c>FaultBudget</c> must be constructed inside <see cref="Run"/>,
/// never stored on the instance, or it leaks (drains) across schedules and silently
/// destroys exploration coverage. Because the harness does not apply
/// <c>coyote rewrite</c>, real <c>Task</c>/<c>await</c> interleavings are not
/// controlled - drive every scheduling choice through the runtime instead.
/// </remarks>
public interface ICoyoteModel
{
    /// <summary>
    /// Executes one iteration of the model against the controlled
    /// <paramref name="runtime"/>. Called repeatedly by the harness under
    /// different explored schedules.
    /// </summary>
    void Run(ICoyoteRuntime runtime);
}

/// <summary>
/// The outcome of a systematic exploration: how many iterations ran, how many
/// distinct safety-property violations were found, the human-readable bug
/// reports, and a replayable trace of the first violating schedule (empty when
/// none was found).
/// </summary>
public readonly record struct CoyoteExplorationResult(
    int Iterations,
    int BugsFound,
    IReadOnlyCollection<string> BugReports,
    string ReproducibleTrace);

/// <summary>
/// Reusable Coyote exploration harness for Orleans.Lattice concurrency models.
/// It wraps the <see cref="TestingEngine"/> plumbing (configuration, run,
/// report extraction, reproducible-trace capture) so a model author writes only
/// an <see cref="ICoyoteModel"/> and one assertion call, and every model gets a
/// consistent, deterministic-on-failure exploration without re-plumbing the
/// engine.
/// </summary>
public static class CoyoteModelHarness
{
    /// <summary>Default number of schedules explored per assertion.</summary>
    public const int DefaultIterations = 1000;

    /// <summary>Default upper bound on scheduling steps per iteration.</summary>
    public const int DefaultMaxSteps = 200;

    /// <summary>
    /// Explores up to <paramref name="iterations"/> schedules of
    /// <paramref name="model"/> and reports what was found, without asserting.
    /// </summary>
    public static CoyoteExplorationResult Explore(
        ICoyoteModel model,
        int iterations = DefaultIterations,
        int maxSteps = DefaultMaxSteps)
    {
        ArgumentNullException.ThrowIfNull(model);

        var configuration = Configuration.Create()
            .WithTestingIterations((uint)iterations)
            .WithMaxSchedulingSteps((uint)maxSteps);

        using var engine = TestingEngine.Create(configuration, model.Run);
        engine.Run();

        var report = engine.TestReport;
        return new CoyoteExplorationResult(
            iterations,
            report.NumOfFoundBugs,
            report.BugReports.ToArray(),
            report.NumOfFoundBugs > 0 ? engine.ReproducibleTrace : string.Empty);
    }

    /// <summary>
    /// Asserts that no schedule of <paramref name="model"/> violates its safety
    /// property. On failure, fails the test with the bug report and the
    /// replayable trace of the first violating schedule.
    /// </summary>
    public static void AssertNoInterleavingViolation(
        ICoyoteModel model,
        int iterations = DefaultIterations,
        int maxSteps = DefaultMaxSteps)
    {
        var result = Explore(model, iterations, maxSteps);
        if (result.BugsFound > 0)
        {
            Assert.Fail(
                $"Coyote found {result.BugsFound} interleaving violation(s) in {result.Iterations} iterations.\n" +
                $"Bug: {string.Join("\n", result.BugReports)}\n" +
                $"Reproducible trace:\n{result.ReproducibleTrace}");
        }
    }

    /// <summary>
    /// Asserts that at least one schedule of <paramref name="model"/> violates
    /// its safety property. Use this to prove a model actually catches a known
    /// regression (a guard removed / a live read reintroduced): if it finds no
    /// violation, the model has stopped exercising the race and the test fails.
    /// </summary>
    public static void AssertInterleavingViolationFound(
        ICoyoteModel model,
        int iterations = DefaultIterations,
        int maxSteps = DefaultMaxSteps)
    {
        var result = Explore(model, iterations, maxSteps);
        Assert.That(
            result.BugsFound,
            Is.GreaterThan(0),
            $"Expected Coyote to find an interleaving violation in {result.Iterations} iterations, but none was found. " +
            "The model may no longer exercise the race it is meant to catch.");
    }
}
