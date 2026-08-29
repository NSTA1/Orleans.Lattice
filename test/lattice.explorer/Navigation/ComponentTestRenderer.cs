using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.RenderTree;
using Microsoft.Extensions.Logging;

// The render-tree frame types are framework-internal by policy, but reading
// them is the only way to assert what a component actually rendered without
// taking a third-party component-test dependency. The tests below are pinned to
// this repository's own components, so a future framework change breaks them
// loudly here rather than silently in production.
#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// A minimal in-memory <see cref="Renderer"/> for component tests: it renders a
/// component, exposes its current render-tree frames, and dispatches an event to
/// a handler found in them.
/// <para>
/// Everything runs on the renderer's own dispatcher and every await in the
/// components under test completes synchronously, so a test drives a transition
/// and reads the result with no delay, no polling, and no dependence on timing.
/// </para>
/// </summary>
internal sealed class ComponentTestRenderer(IServiceProvider services, ILoggerFactory loggerFactory)
    : Renderer(services, loggerFactory)
{
    private readonly Dispatcher _dispatcher = Dispatcher.CreateDefault();

    /// <summary>Every exception the renderer surfaced, so a test can assert none escaped.</summary>
    public List<Exception> Exceptions { get; } = [];

    /// <inheritdoc />
    public override Dispatcher Dispatcher => _dispatcher;

    /// <summary>
    /// Instantiates <typeparamref name="TComponent"/> through the renderer - so
    /// its <c>[Inject]</c> properties are satisfied from the container - runs
    /// <paramref name="configure"/> on it, then renders it as a root.
    /// <para>
    /// The root render is started but not awaited to quiescence. A component
    /// whose initialization awaits work that never completes - a plugin gate
    /// that never answers, which is exactly what the isolation tests cover -
    /// has still rendered its first batch by the time the dispatcher returns,
    /// and waiting for quiescence would hang on precisely that case.
    /// </para>
    /// </summary>
    public async Task<(int Id, TComponent Component)> RenderAsync<TComponent>(
        ParameterView parameters,
        Action<TComponent>? configure = null)
        where TComponent : IComponent
    {
        var id = 0;
        TComponent component = default!;
        var render = Task.CompletedTask;

        await Dispatcher.InvokeAsync(() =>
        {
            component = (TComponent)InstantiateComponent(typeof(TComponent));
            configure?.Invoke(component);
            id = AssignRootComponentId(component);
            render = RenderRootComponentAsync(id, parameters);
        });

        if (render.IsCompleted)
        {
            await render;
        }
        else
        {
            // Never leave a still-pending render unobserved.
            _ = render.ContinueWith(
                static faulted => _ = faulted.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted,
                TaskScheduler.Default);
        }

        return (id, component);
    }

    /// <summary>Runs <paramref name="action"/> on the renderer's dispatcher.</summary>
    public Task OnDispatcherAsync(Action action) => Dispatcher.InvokeAsync(action);

    /// <summary>Reads the buttons currently rendered by <paramref name="componentId"/>.</summary>
    public IReadOnlyList<RenderedButton> Buttons(int componentId)
    {
        var buttons = new List<RenderedButton>();
        var frames = GetCurrentRenderTreeFrames(componentId);
        var array = frames.Array;

        for (var i = 0; i < frames.Count; i++)
        {
            if (array[i].FrameType != RenderTreeFrameType.Element
                || !string.Equals(array[i].ElementName, "button", StringComparison.Ordinal))
            {
                continue;
            }

            var end = i + array[i].ElementSubtreeLength;
            string? cssClass = null;
            string? title = null;
            var disabled = false;
            ulong clickHandler = 0;
            var text = string.Empty;

            for (var j = i + 1; j < end; j++)
            {
                switch (array[j].FrameType)
                {
                    case RenderTreeFrameType.Attribute:
                        switch (array[j].AttributeName)
                        {
                            case "class":
                                cssClass = array[j].AttributeValue as string;
                                break;
                            case "title":
                                title = array[j].AttributeValue as string;
                                break;
                            case "disabled":
                                disabled = array[j].AttributeValue is not null;
                                break;
                            case "onclick":
                                clickHandler = array[j].AttributeEventHandlerId;
                                break;
                        }

                        break;
                    case RenderTreeFrameType.Text:
                        text += array[j].TextContent;
                        break;
                }
            }

            buttons.Add(new RenderedButton(text.Trim(), cssClass, title, disabled, clickHandler));
        }

        return buttons;
    }

    /// <summary>
    /// The components <paramref name="componentId"/> currently renders as
    /// children, in render order.
    /// </summary>
    public IReadOnlyList<IComponent> ChildComponents(int componentId)
    {
        var children = new List<IComponent>();
        var frames = GetCurrentRenderTreeFrames(componentId);

        for (var i = 0; i < frames.Count; i++)
        {
            if (frames.Array[i].FrameType == RenderTreeFrameType.Component
                && frames.Array[i].Component is { } component)
            {
                children.Add(component);
            }
        }

        return children;
    }

    /// <summary>Whether <paramref name="componentId"/> currently renders an element named <paramref name="elementName"/>.</summary>
    public bool RendersElement(int componentId, string elementName)
    {
        var frames = GetCurrentRenderTreeFrames(componentId);
        for (var i = 0; i < frames.Count; i++)
        {
            if (frames.Array[i].FrameType == RenderTreeFrameType.Element
                && string.Equals(frames.Array[i].ElementName, elementName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Dispatches a click to <paramref name="handlerId"/>.</summary>
    public Task ClickAsync(ulong handlerId) =>
        Dispatcher.InvokeAsync(() => DispatchEventAsync(handlerId, null!, EventArgs.Empty));

    /// <inheritdoc />
    protected override void HandleException(Exception exception) => Exceptions.Add(exception);

    /// <inheritdoc />
    protected override Task UpdateDisplayAsync(in RenderBatch renderBatch) => Task.CompletedTask;

    /// <summary>One rendered <c>button</c> element, flattened to what a navigation test asserts on.</summary>
    /// <param name="Text">The button's text content.</param>
    /// <param name="Class">Its <c>class</c> attribute, or <see langword="null"/>.</param>
    /// <param name="Title">Its <c>title</c> attribute, or <see langword="null"/> when it carries none.</param>
    /// <param name="Disabled">Whether it rendered the <c>disabled</c> attribute.</param>
    /// <param name="ClickHandlerId">The event-handler id of its <c>onclick</c> binding.</param>
    internal readonly record struct RenderedButton(
        string Text,
        string? Class,
        string? Title,
        bool Disabled,
        ulong ClickHandlerId)
    {
        /// <summary>Whether the button carries the active-tab marker class.</summary>
        public bool IsActive => Class?.Contains("is-active", StringComparison.Ordinal) == true;
    }
}
