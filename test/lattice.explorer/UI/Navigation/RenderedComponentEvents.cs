using Bunit;
using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// Fires a DOM event on an element found in the same turn of the renderer as the
/// event, and waits for the event's handler.
/// </summary>
/// <remarks>
/// <para>
/// bUnit's synchronous triggers (<c>Click</c>, <c>Input</c>, <c>Submit</c>) do not
/// wait while the renderer is busy. When a component finishes an asynchronous load
/// on another thread - <c>ClusterWalReclamation</c>'s read on the WAL page, say -
/// the trigger is queued behind that render and returns at once, before its own
/// handler and re-render have run. The test's next step then finds elements in the
/// DOM as it was before that re-render, and its event goes to a handler id the
/// re-render has since replaced: bUnit either throws
/// <c>UnknownEventHandlerIdException</c>, or, when the stale dispatch fails off
/// the test thread, drops the event without a word (issue #4630).
/// </para>
/// <para>
/// Running the find and the dispatch together on the renderer's dispatcher means
/// no render can land between them, and awaiting the dispatch means the handler,
/// and every render it causes, has run before the test's next step.
/// </para>
/// </remarks>
internal static class RenderedComponentEvents
{
    /// <summary>
    /// Runs <paramref name="trigger"/> - a find and an asynchronous bUnit trigger
    /// such as <c>InputAsync</c> - on the renderer's dispatcher, and waits for it.
    /// </summary>
    /// <typeparam name="TComponent">The component type.</typeparam>
    /// <param name="cut">The rendered component.</param>
    /// <param name="trigger">Finds the element in <paramref name="cut"/> and fires the event on it.</param>
    /// <returns>A task that completes when the event's handler has run.</returns>
    public static Task FireAsync<TComponent>(this IRenderedComponent<TComponent> cut, Func<IRenderedComponent<TComponent>, Task> trigger)
        where TComponent : IComponent
    {
        ArgumentNullException.ThrowIfNull(cut);
        ArgumentNullException.ThrowIfNull(trigger);
        return cut.InvokeAsync(() => trigger(cut));
    }
}
