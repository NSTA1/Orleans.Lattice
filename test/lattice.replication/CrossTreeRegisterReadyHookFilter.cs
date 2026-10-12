using System.Collections.Concurrent;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Runs a scripted action once, immediately before the next joint-coordinator
/// readiness registration a named view's maintainer sends, so a test can let a
/// peer participant complete the joint flip between this view applying an
/// ordinary write and re-registering its slice.
/// </summary>
internal sealed class CrossTreeRegisterReadyHookFilter(IGrainFactory grainFactory) : IOutgoingGrainCallFilter
{
    private static readonly ConcurrentDictionary<string, Func<IGrainFactory, Task>> Hooks = new(StringComparer.Ordinal);

    /// <summary>Runs <paramref name="hook"/> before the next registration sent for <paramref name="viewName"/>.</summary>
    public static void BeforeNextRegistration(string viewName, Func<IGrainFactory, Task> hook) => Hooks[viewName] = hook;

    /// <summary><c>true</c> while the hook armed for <paramref name="viewName"/> has not yet run.</summary>
    public static bool IsArmed(string viewName) => Hooks.ContainsKey(viewName);

    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        if (context.InterfaceMethod.DeclaringType == typeof(IViewCrossTreeCoordinatorGrain)
            && context.InterfaceMethod.Name == nameof(IViewCrossTreeCoordinatorGrain.RegisterReadyAsync)
            && context.Request.GetArgument(0) is ViewCrossTreeReadiness readiness
            && Hooks.TryRemove(readiness.ViewName, out var hook))
        {
            await hook(grainFactory);
        }

        await context.Invoke();
    }
}
