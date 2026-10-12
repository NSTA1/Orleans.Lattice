using System.Collections.Concurrent;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Fails a scripted number of joint-coordinator degrade calls made on behalf of a
/// named view, so a test can interrupt a staging-backstop rebuild between the
/// backstop discarding a registered cross-tree batch and the coordinator
/// recording its degradation.
/// </summary>
internal sealed class CrossTreeDegradeFaultFilter : IIncomingGrainCallFilter
{
    private static readonly ConcurrentDictionary<string, int> Remaining = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, int> Calls = new(StringComparer.Ordinal);

    /// <summary>Fails the next <paramref name="count"/> degrade calls for <paramref name="viewName"/>.</summary>
    public static void FailNext(string viewName, int count) => Remaining[viewName] = count;

    /// <summary>Degrade calls observed for <paramref name="viewName"/>, failed or not.</summary>
    public static int CallCount(string viewName) => Calls.GetValueOrDefault(viewName);

    public Task Invoke(IIncomingGrainCallContext context)
    {
        if (context.InterfaceMethod.DeclaringType == typeof(IViewCrossTreeCoordinatorGrain)
            && context.InterfaceMethod.Name == nameof(IViewCrossTreeCoordinatorGrain.RegisterDegradedAsync)
            && context.Request.GetArgument(0) is string viewName)
        {
            Calls.AddOrUpdate(viewName, 1, (_, n) => n + 1);
            while (Remaining.TryGetValue(viewName, out var left) && left > 0)
            {
                if (Remaining.TryUpdate(viewName, left - 1, left))
                {
                    throw new InvalidOperationException($"Injected degrade failure for view '{viewName}'.");
                }
            }
        }

        return context.Invoke();
    }
}
