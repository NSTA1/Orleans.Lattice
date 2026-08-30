using System.Runtime.ExceptionServices;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.RenderTree;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// BL0006 warns that the RenderTree types are not intended for use outside the
// Blazor framework, because their shape may change between releases. This is
// test-only infrastructure and the trade is deliberate: reading the render tree
// is the only way to dispatch a real DOM event at a component without taking a
// third-party component-testing dependency, and it is what lets the design
// system's selection, drawer, overflow, and keyboard behaviour be tested rather
// than only its static markup. The repository builds against a pinned framework
// reference, so a shape change surfaces as a compile error here at upgrade time
// rather than as silent drift.
#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Renders a design-system component and dispatches real DOM events at it, so a
/// test can exercise selection, drawer and overflow toggling, and keyboard
/// navigation rather than only the static markup.
/// </summary>
/// <remarks>
/// Built on the framework's own <see cref="Renderer"/>, so the design system
/// needs no extra component-testing dependency. Every interaction is driven
/// explicitly by the test - there is no timer, no clock, and no background work
/// - so a run is deterministic and never order- or timing-dependent.
/// </remarks>
internal sealed class DesignSystemInteractiveHarness : IAsyncDisposable
{
    private readonly ServiceProvider _provider;
    private readonly EventDispatchRenderer _renderer;
    private readonly int _rootComponentId;

    private DesignSystemInteractiveHarness(
        ServiceProvider provider,
        EventDispatchRenderer renderer,
        int rootComponentId)
    {
        _provider = provider;
        _renderer = renderer;
        _rootComponentId = rootComponentId;
    }

    /// <summary>
    /// Renders <typeparamref name="TComponent"/> with the supplied parameters
    /// and returns a harness positioned over the result.
    /// </summary>
    /// <typeparam name="TComponent">The component to render.</typeparam>
    /// <param name="parameters">The component parameters, keyed by parameter name.</param>
    /// <returns>The harness.</returns>
    public static async Task<DesignSystemInteractiveHarness> RenderAsync<TComponent>(
        IDictionary<string, object?> parameters)
        where TComponent : IComponent, new()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var provider = services.BuildServiceProvider();
        var renderer = new EventDispatchRenderer(provider, provider.GetRequiredService<ILoggerFactory>());

        var componentId = await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var id = renderer.AssignRoot(new TComponent());
            await renderer.RenderRootAsync(id, ParameterView.FromDictionary(parameters));
            return id;
        });

        return new DesignSystemInteractiveHarness(provider, renderer, componentId);
    }

    /// <summary>
    /// Every element currently rendered, in document order, flattened across
    /// child components.
    /// </summary>
    /// <returns>The rendered elements.</returns>
    public IReadOnlyList<RenderedElement> Elements()
    {
        var elements = new List<RenderedElement>();
        Collect(_rootComponentId, elements);
        return elements;
    }

    /// <summary>
    /// The single element matching <paramref name="predicate"/>.
    /// </summary>
    /// <param name="predicate">The match to apply.</param>
    /// <returns>The matching element.</returns>
    public RenderedElement Element(Func<RenderedElement, bool> predicate)
    {
        var matches = Elements().Where(predicate).ToArray();

        Assert.That(matches, Has.Length.EqualTo(1),
            "expected exactly one matching element, found " + matches.Length);

        return matches[0];
    }

    /// <summary>
    /// Dispatches a click at the single element matching
    /// <paramref name="predicate"/>.
    /// </summary>
    /// <param name="predicate">The match to apply.</param>
    public Task ClickAsync(Func<RenderedElement, bool> predicate) =>
        DispatchAsync(predicate, "onclick", EventArgs.Empty);

    /// <summary>
    /// Dispatches a key press at the single element matching
    /// <paramref name="predicate"/>.
    /// </summary>
    /// <param name="predicate">The match to apply.</param>
    /// <param name="key">The key value, for example <c>ArrowRight</c>.</param>
    public Task KeyDownAsync(Func<RenderedElement, bool> predicate, string key) =>
        DispatchAsync(predicate, "onkeydown", new Microsoft.AspNetCore.Components.Web.KeyboardEventArgs { Key = key });

    private async Task DispatchAsync(
        Func<RenderedElement, bool> predicate, string eventName, EventArgs args)
    {
        var element = Element(predicate);

        Assert.That(element.EventHandlerIds.ContainsKey(eventName), Is.True,
            $"element <{element.Name}> has no {eventName} handler");

        await _renderer.Dispatcher.InvokeAsync(
            () => _renderer.DispatchEventAsync(element.EventHandlerIds[eventName], null!, args));
    }

    private void Collect(int componentId, List<RenderedElement> elements)
    {
        var frames = _renderer.Frames(componentId);
        CollectRange(frames.Array, 0, frames.Count, elements);
    }

    private void CollectRange(RenderTreeFrame[] frames, int start, int end, List<RenderedElement> elements)
    {
        var index = start;
        while (index < end)
        {
            ref var frame = ref frames[index];
            switch (frame.FrameType)
            {
                case RenderTreeFrameType.Element:
                {
                    var subtreeEnd = index + frame.ElementSubtreeLength;
                    var element = new RenderedElement(frame.ElementName);

                    var child = index + 1;
                    while (child < subtreeEnd && frames[child].FrameType == RenderTreeFrameType.Attribute)
                    {
                        ref var attribute = ref frames[child];
                        if (attribute.AttributeEventHandlerId != 0)
                        {
                            element.EventHandlerIds[attribute.AttributeName] = attribute.AttributeEventHandlerId;
                        }
                        else
                        {
                            element.Attributes[attribute.AttributeName] = attribute.AttributeValue?.ToString() ?? string.Empty;
                        }

                        child++;
                    }

                    element.Text = TextOf(frames, child, subtreeEnd);
                    elements.Add(element);

                    CollectRange(frames, child, subtreeEnd, elements);
                    index = subtreeEnd;
                    break;
                }

                case RenderTreeFrameType.Component:
                {
                    Collect(frame.ComponentId, elements);
                    index += frame.ComponentSubtreeLength;
                    break;
                }

                case RenderTreeFrameType.Region:
                {
                    CollectRange(frames, index + 1, index + frame.RegionSubtreeLength, elements);
                    index += frame.RegionSubtreeLength;
                    break;
                }

                default:
                    index++;
                    break;
            }
        }
    }

    private static string TextOf(RenderTreeFrame[] frames, int start, int end)
    {
        var text = string.Empty;
        for (var i = start; i < end; i++)
        {
            if (frames[i].FrameType == RenderTreeFrameType.Text)
            {
                text += frames[i].TextContent;
            }
        }

        return text.Trim();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _renderer.Dispose();
        await _provider.DisposeAsync();
    }

    /// <summary>One rendered element: its tag, attributes, text, and handlers.</summary>
    /// <param name="name">The element's tag name.</param>
    internal sealed class RenderedElement(string name)
    {
        /// <summary>The element's tag name.</summary>
        public string Name { get; } = name;

        /// <summary>The element's non-event attributes.</summary>
        public Dictionary<string, string> Attributes { get; } = new(StringComparer.Ordinal);

        /// <summary>The element's event handler ids, keyed by event attribute name.</summary>
        public Dictionary<string, ulong> EventHandlerIds { get; } = new(StringComparer.Ordinal);

        /// <summary>The element's own text content.</summary>
        public string Text { get; set; } = string.Empty;

        /// <summary>Whether the element carries <paramref name="cssClass"/>.</summary>
        /// <param name="cssClass">The class to look for.</param>
        /// <returns><see langword="true"/> when the class is present.</returns>
        public bool HasClass(string cssClass) =>
            Attributes.TryGetValue("class", out var value)
            && value.Split(' ', StringSplitOptions.RemoveEmptyEntries).Contains(cssClass);

        /// <summary>The element's attribute value, or null when absent.</summary>
        /// <param name="name">The attribute name.</param>
        /// <returns>The attribute value, or null.</returns>
        public string? Attribute(string name) =>
            Attributes.TryGetValue(name, out var value) ? value : null;
    }

    /// <summary>
    /// A minimal renderer that keeps the current render tree in memory and lets
    /// a test dispatch events into it.
    /// </summary>
    private sealed class EventDispatchRenderer(IServiceProvider services, ILoggerFactory loggerFactory)
        : Renderer(services, loggerFactory)
    {
        public override Dispatcher Dispatcher { get; } = Dispatcher.CreateDefault();

        public int AssignRoot(IComponent component) => AssignRootComponentId(component);

        public Task RenderRootAsync(int componentId, ParameterView parameters) =>
            RenderRootComponentAsync(componentId, parameters);

        public ArrayRange<RenderTreeFrame> Frames(int componentId) =>
            GetCurrentRenderTreeFrames(componentId);

        protected override void HandleException(Exception exception) =>
            ExceptionDispatchInfo.Capture(exception).Throw();

        protected override Task UpdateDisplayAsync(in RenderBatch renderBatch) => Task.CompletedTask;
    }
}

#pragma warning restore BL0006
