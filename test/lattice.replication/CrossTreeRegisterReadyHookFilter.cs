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
    private static readonly ConcurrentDictionary<string, Func<IGrainFactory, Task>> RepairHooks = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Func<IGrainFactory, Task>> OriginalResponses = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Func<IGrainFactory, Task>> RepairResponses = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, byte> AckResponses = new(StringComparer.Ordinal);

    /// <summary>Runs <paramref name="hook"/> before the next registration sent for <paramref name="viewName"/>.</summary>
    public static void BeforeNextRegistration(string viewName, Func<IGrainFactory, Task> hook) => Hooks[viewName] = hook;

    /// <summary><c>true</c> while the hook armed for <paramref name="viewName"/> has not yet run.</summary>
    public static bool IsArmed(string viewName) => Hooks.ContainsKey(viewName);

    public static void BeforeNextRepair(string viewName, Func<IGrainFactory, Task> hook) => RepairHooks[viewName] = hook;
    public static void AfterNextRegistration(string viewName, Func<IGrainFactory, Task> hook) => OriginalResponses[viewName] = hook;
    public static void AfterNextRepair(string viewName, Func<IGrainFactory, Task> hook) => RepairResponses[viewName] = hook;
    public static void LoseNextAcknowledgementResponse(string viewName) => AckResponses[viewName] = 0;

    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        var original = context.InterfaceMethod.DeclaringType == typeof(IViewCrossTreeCoordinatorGrain)
            && context.InterfaceMethod.Name == nameof(IViewCrossTreeCoordinatorGrain.RegisterReadyAsync)
            ? context.Request.GetArgument(0) as ViewCrossTreeReadiness
            : null;
        var acknowledgedView = context.InterfaceMethod.DeclaringType == typeof(IViewCrossTreeCoordinatorGrain)
            && context.InterfaceMethod.Name == nameof(IViewCrossTreeCoordinatorGrain.AcknowledgeRepairAsync)
            ? context.Request.GetArgument(0) as string
            : null;
        if (original is not null && Hooks.TryRemove(original.ViewName, out var hook))
        {
            await hook(grainFactory);
        }

        var repair = context.InterfaceMethod.DeclaringType == typeof(IViewCrossTreeCoordinatorGrain)
            && context.InterfaceMethod.Name == nameof(IViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync)
            ? context.Request.GetArgument(0) as ViewCrossTreeReadiness
            : null;
        if (repair is not null && RepairHooks.TryRemove(repair.ViewName, out var before))
        {
            await before(grainFactory);
        }

        await context.Invoke();
        if (original is not null && OriginalResponses.TryRemove(original.ViewName, out var originalResponse))
        {
            await originalResponse(grainFactory);
        }

        if (repair is not null && RepairResponses.TryRemove(repair.ViewName, out var after))
        {
            await after(grainFactory);
        }

        if (acknowledgedView is not null && AckResponses.TryRemove(acknowledgedView, out _))
        {
            throw new TimeoutException("Injected lost durable repair acknowledgement response.");
        }
    }
}
