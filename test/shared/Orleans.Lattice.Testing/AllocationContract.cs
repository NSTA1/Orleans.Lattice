using System.Diagnostics;
using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Preconditions for steady-state allocation assertions.
/// <para>
/// A fixture that measures an <c>async</c> path and asserts it allocates
/// nothing (or almost nothing) per call is asserting a contract that only
/// holds in an <b>optimized</b> build. Roslyn emits an async method's state
/// machine as a <b>struct</b> under <c>&lt;Optimize&gt;</c> and as a
/// <b>class</b> without it, so in a Debug build every call to every async
/// method heap-allocates its state machine whether or not it ever suspends.
/// The usual reasoning - "a method that completes synchronously does not box
/// its state machine" - is a statement about the optimized build only.
/// </para>
/// <para>
/// That artifact is <b>deterministic and per-call</b>, so none of the defences
/// a careful allocation harness already has can see through it: it scales with
/// the loop, so a differential measurement does not cancel it; it happens on
/// every iteration, so a minimum across attempts does not drive it to zero;
/// and it is unaffected by warm-up, because it is a compilation decision
/// rather than a tiering one. The result is a clean, repeatable, non-zero
/// figure that looks exactly like a genuine per-iteration allocation in the
/// code under test.
/// </para>
/// <para>
/// It is also easy to misdiagnose as a <i>hardware</i> difference. The
/// measurement is bimodal rather than marginal - a state machine is either
/// heap-allocated or it is not - so it presents as one developer measuring
/// zero and another measuring a large constant, with the budget in the empty
/// gap between them, while CI (which always builds Release) stays green. The
/// variable is how each developer invoked the build, not which processor they
/// ran it on. This has cost this repository a full triage cycle once already
/// (issue #2540), which is why the precondition is asserted here rather than
/// left implicit in a comment.
/// </para>
/// </summary>
public static class AllocationContract
{
    /// <summary>
    /// Reports whether an assembly was compiled with optimizations disabled,
    /// and therefore emits async state machines as heap-allocated classes.
    /// </summary>
    /// <param name="assembly">The assembly to inspect.</param>
    /// <returns><see langword="true"/> when the assembly is unoptimized.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="assembly"/> is null.</exception>
    public static bool IsUnoptimized(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);

        // Release emits DebuggableAttribute without DisableOptimizations (or,
        // for some SDKs, omits it); Debug emits it with the flag set. An
        // assembly carrying no attribute at all was compiled optimized.
        var debuggable = assembly.GetCustomAttribute<DebuggableAttribute>();
        return debuggable is not null && debuggable.IsJITOptimizerDisabled;
    }

    /// <summary>
    /// Returns those of <paramref name="assemblies"/> that were compiled
    /// without optimizations, in the order supplied.
    /// </summary>
    /// <param name="assemblies">The assemblies to inspect.</param>
    /// <returns>The unoptimized assemblies, empty when every one is optimized.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="assemblies"/>, or any element, is null.</exception>
    public static IReadOnlyList<Assembly> UnoptimizedAmong(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        var unoptimized = new List<Assembly>();
        foreach (var assembly in assemblies)
        {
            ArgumentNullException.ThrowIfNull(assembly, nameof(assemblies));
            if (IsUnoptimized(assembly))
            {
                unoptimized.Add(assembly);
            }
        }

        return unoptimized;
    }

    /// <summary>
    /// Asserts that every assembly whose async state machines the measurement
    /// depends on was compiled optimized, so a steady-state allocation figure
    /// measures the code under test rather than the compiler's Debug layout.
    /// <para>
    /// Locally this <b>ignores</b> the test, which NUnit reports as a visible
    /// <c>Skipped</c>. It deliberately does not use <c>Assert.Inconclusive</c>,
    /// which NUnit counts as neither passed, failed, nor skipped and which
    /// would let the gate vanish from every summary counter while the run
    /// still printed a pass.
    /// </para>
    /// <para>
    /// Under <c>GITHUB_ACTIONS</c> it <b>fails</b> instead. CI builds every
    /// project with <c>--configuration Release</c>, so an unoptimized assembly
    /// there means the pipeline is misconfigured, and an allocation gate that
    /// quietly skips itself in CI still reads as coverage.
    /// </para>
    /// </summary>
    /// <param name="assemblies">
    /// The assemblies declaring the async methods on the measured path -
    /// typically the product assembly under test and the test assembly itself,
    /// since an <c>async</c> lambda in the fixture is a state machine too.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="assemblies"/>, or any element, is null.</exception>
    public static void RequireOptimizedBuild(params Assembly[] assemblies)
    {
        var unoptimized = UnoptimizedAmong(assemblies);
        if (unoptimized.Count == 0)
        {
            return;
        }

        var names = string.Join(", ", unoptimized.Select(assembly => assembly.GetName().Name));
        var message =
            $"This steady-state allocation contract requires an optimized build, but {names} "
            + "was compiled with optimizations disabled. Roslyn emits async state machines as classes "
            + "rather than structs without <Optimize>, so every async call heap-allocates its state "
            + "machine whether or not it suspends, and the measurement reports that fixed compiler cost "
            + "instead of the path under test. Re-run with '-c Release'.";

        if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") is { Length: > 0 })
        {
            Assert.Fail(
                message
                + " This ran in CI, where every project is built with '--configuration Release', so an "
                + "unoptimized assembly means the pipeline is misconfigured and this gate is not "
                + "measuring anything.");
        }

        Assert.Ignore(message);
    }
}
