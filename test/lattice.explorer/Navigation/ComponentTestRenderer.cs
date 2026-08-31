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
            string? ariaSelected = null;
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
                            case "aria-selected":
                                ariaSelected = array[j].AttributeValue as string;
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

            buttons.Add(new RenderedButton(text.Trim(), cssClass, title, ariaSelected, disabled, clickHandler));
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

    /// <summary>
    /// The first <typeparamref name="TComponent"/> rendered anywhere beneath
    /// <paramref name="rootComponentId"/> (inclusive of its direct children),
    /// with the id it is rendered under, or <see langword="null"/> when the tree
    /// currently holds none.
    /// <para>
    /// A depth-first walk over the current frames, so a test can assert on a
    /// component the subject renders indirectly - the tab strip a panel
    /// delegates to, or the view a <see cref="DynamicComponent"/> mounts -
    /// without reaching into the framework's internals from the test itself.
    /// </para>
    /// </summary>
    public (int Id, TComponent Component)? FindComponent<TComponent>(int rootComponentId)
        where TComponent : class, IComponent
    {
        var frames = GetCurrentRenderTreeFrames(rootComponentId);

        for (var i = 0; i < frames.Count; i++)
        {
            if (frames.Array[i].FrameType != RenderTreeFrameType.Component)
            {
                continue;
            }

            var childId = frames.Array[i].ComponentId;
            if (frames.Array[i].Component is TComponent match)
            {
                return (childId, match);
            }

            var nested = FindComponent<TComponent>(childId);
            if (nested is not null)
            {
                return nested;
            }
        }

        return null;
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

    /// <summary>
    /// The text content of every element named <paramref name="elementName"/>
    /// that <paramref name="componentId"/> currently renders, optionally
    /// restricted to those whose <c>class</c> attribute contains
    /// <paramref name="classContains"/>.
    /// </summary>
    /// <remarks>
    /// The shell's demoted area entries are inert names rather than controls, so
    /// they are not buttons and a test needs to read them as elements. Kept
    /// alongside <see cref="Buttons"/> so both read the same settled frames.
    /// </remarks>
    /// <param name="componentId">The component whose frames to read.</param>
    /// <param name="elementName">The element name to match.</param>
    /// <param name="classContains">A class fragment to require, or <see langword="null"/> for any.</param>
    public IReadOnlyList<string> ElementTexts(
        int componentId,
        string elementName,
        string? classContains = null)
    {
        var texts = new List<string>();
        var frames = GetCurrentRenderTreeFrames(componentId);
        var array = frames.Array;

        for (var i = 0; i < frames.Count; i++)
        {
            if (array[i].FrameType != RenderTreeFrameType.Element
                || !string.Equals(array[i].ElementName, elementName, StringComparison.Ordinal))
            {
                continue;
            }

            var end = i + array[i].ElementSubtreeLength;
            string? cssClass = null;
            var text = string.Empty;

            for (var j = i + 1; j < end; j++)
            {
                switch (array[j].FrameType)
                {
                    case RenderTreeFrameType.Attribute
                        when string.Equals(array[j].AttributeName, "class", StringComparison.Ordinal):
                        cssClass = array[j].AttributeValue as string;
                        break;
                    case RenderTreeFrameType.Text:
                        text += array[j].TextContent;
                        break;
                }
            }

            if (classContains is null
                || cssClass?.Contains(classContains, StringComparison.Ordinal) == true)
            {
                texts.Add(text.Trim());
            }
        }

        return texts;
    }

    /// <summary>
    /// The value of <paramref name="attributeName"/> on the first element named
    /// <paramref name="elementName"/> that <paramref name="componentId"/>
    /// renders, or <see langword="null"/> when there is no such element or it
    /// carries no such attribute.
    /// </summary>
    /// <param name="componentId">The component whose frames to read.</param>
    /// <param name="elementName">The element name to match.</param>
    /// <param name="attributeName">The attribute to read.</param>
    public string? ElementAttribute(int componentId, string elementName, string attributeName)
    {
        var frames = GetCurrentRenderTreeFrames(componentId);
        var array = frames.Array;

        for (var i = 0; i < frames.Count; i++)
        {
            if (array[i].FrameType != RenderTreeFrameType.Element
                || !string.Equals(array[i].ElementName, elementName, StringComparison.Ordinal))
            {
                continue;
            }

            var end = i + array[i].ElementSubtreeLength;
            for (var j = i + 1; j < end; j++)
            {
                if (array[j].FrameType == RenderTreeFrameType.Attribute
                    && string.Equals(array[j].AttributeName, attributeName, StringComparison.Ordinal))
                {
                    return array[j].AttributeValue as string;
                }
            }

            return null;
        }

        return null;
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
    /// <param name="AriaSelected">
    /// Its <c>aria-selected</c> attribute verbatim, or <see langword="null"/>
    /// when it carries none. Read as the literal string rather than a
    /// <see langword="bool"/> on purpose: <c>aria-selected</c> is an enumerated
    /// ARIA attribute, so <c>"true"</c> / <c>"false"</c> and an absent or empty
    /// value are three distinguishable outcomes, and only a test that can tell
    /// them apart catches the boolean-attribute rendering issue #1793 found.
    /// </param>
    /// <param name="Disabled">Whether it rendered the <c>disabled</c> attribute.</param>
    /// <param name="ClickHandlerId">The event-handler id of its <c>onclick</c> binding.</param>
    internal readonly record struct RenderedButton(
        string Text,
        string? Class,
        string? Title,
        string? AriaSelected,
        bool Disabled,
        ulong ClickHandlerId)
    {
        /// <summary>Whether the button carries the active-tab marker class.</summary>
        public bool IsActive => Class?.Contains("is-active", StringComparison.Ordinal) == true;
    }
}
