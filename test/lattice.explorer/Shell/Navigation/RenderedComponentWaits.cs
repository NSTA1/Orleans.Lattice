using Bunit;
using Microsoft.AspNetCore.Components;
using NUnit.Framework.Internal;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>
/// Waits for a rendered component to satisfy NUnit assertions, re-checking on
/// every render.
/// </summary>
/// <remarks>
/// bUnit's own <c>WaitForAssertion</c> cannot be used with NUnit 4: a failed
/// <c>Assert.That</c> records the failure on the test result before it throws,
/// so an early, expected miss fails the test even when a later render passes.
/// Here every intermediate check runs in an isolated NUnit context, and the
/// assertions run once more for real when the state is reached (or the wait
/// gives up), so only the final verdict is recorded. The wait is driven by
/// renders, not by elapsed time; the timeout only bounds a test that would
/// otherwise hang.
/// </remarks>
internal static class RenderedComponentWaits
{
    /// <summary>Waits until <paramref name="assertions"/> pass on a render, then asserts them.</summary>
    /// <typeparam name="TComponent">The component type.</typeparam>
    /// <param name="cut">The rendered component.</param>
    /// <param name="assertions">The assertions.</param>
    public static void WaitUntil<TComponent>(this IRenderedComponent<TComponent> cut, Action assertions)
        where TComponent : IComponent
    {
        try
        {
            cut.WaitForState(() => Passes(assertions), TimeSpan.FromSeconds(10));
        }
        catch (global::Bunit.Extensions.WaitForHelpers.WaitForFailedException)
        {
            // Fall through: the assertions below report what is still wrong.
        }

        assertions();
    }

    private static bool Passes(Action assertions)
    {
        using (new TestExecutionContext.IsolatedContext())
        {
            try
            {
                assertions();
                return TestExecutionContext.CurrentContext.CurrentResult.AssertionResults.Count == 0;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
