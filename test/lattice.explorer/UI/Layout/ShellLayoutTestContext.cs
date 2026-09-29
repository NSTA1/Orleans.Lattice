using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Design.Slots;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The bUnit context for rendering the whole layout: the chrome's services, the
/// chrome module set up in loose mode, and helpers to render a page body, move
/// the measured width band, and press the global shortcut - each through the
/// same .NET callbacks the module calls.
/// </summary>
public abstract class ShellLayoutTestContext : ShellChromeTestContext
{
    /// <summary>Sets the chrome module up in loose mode.</summary>
    protected ShellLayoutTestContext()
    {
        Module = JSInterop.SetupModule(ShellChromeAssets.ModuleSpecifier);
        Module.Mode = JSRuntimeMode.Loose;
    }

    /// <summary>The chrome module's interop, for verifying its calls.</summary>
    internal BunitJSModuleInterop Module { get; }

    /// <summary>A page body that marks where it rendered.</summary>
    internal static RenderFragment PageBody { get; } = builder =>
    {
        builder.OpenElement(0, "p");
        builder.AddAttribute(1, "id", "page-body");
        builder.AddContent(2, "The page");
        builder.CloseElement();
    };

    /// <summary>Renders the layout around <see cref="PageBody"/>.</summary>
    internal IRenderedComponent<ShellLayout> RenderLayout() =>
        Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, PageBody));

    /// <summary>Re-renders the layout as the router does after a navigation.</summary>
    /// <param name="cut">The rendered layout.</param>
    /// <param name="relative">Where to navigate first.</param>
    internal void NavigateAndRender(IRenderedComponent<ShellLayout> cut, string relative)
    {
        Navigation.NavigateTo(relative);
        cut.Render(parameters => parameters.Add(layout => layout.Body, PageBody));
    }

    /// <summary>Moves the measured width band: 0 compact, 1 medium, 2 expanded.</summary>
    /// <param name="cut">The rendered layout.</param>
    /// <param name="band">The band.</param>
    internal Task SetBandAsync(IRenderedComponent<ShellLayout> cut, int band) =>
        cut.InvokeAsync(() => Callbacks().OnViewportBand(band));

    /// <summary>Presses <c>/</c> or Ctrl+K, as the module reports it.</summary>
    /// <param name="cut">The rendered layout.</param>
    internal Task PressShortcutAsync(IRenderedComponent<ShellLayout> cut) =>
        cut.InvokeAsync(() => Callbacks().OpenAddressLine());

    /// <summary>The callbacks the layout handed the module when it began observing its width.</summary>
    internal ShellLayoutCallbacks Callbacks()
    {
        var invocation = Module.Invocations["observeViewport"].Single();
        return ((DotNetObjectReference<ShellLayoutCallbacks>)invocation.Arguments[1]!).Value;
    }

    /// <summary>A header.connection contribution that marks where it rendered.</summary>
    public sealed class ConnectionProbe : ComponentBase
    {
        /// <inheritdoc />
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "span");
            builder.AddAttribute(1, "data-probe", "connection");
            builder.AddContent(2, "eu-west connected");
            builder.CloseElement();
        }
    }

    /// <summary>A header.identity contribution that marks where it rendered.</summary>
    public sealed class IdentityProbe : ComponentBase
    {
        /// <inheritdoc />
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "span");
            builder.AddAttribute(1, "data-probe", "identity");
            builder.AddContent(2, "Dana Okafor");
            builder.CloseElement();
        }
    }

    /// <summary>An overlay.session contribution that marks where it rendered.</summary>
    public sealed class OverlayProbe : ComponentBase
    {
        /// <inheritdoc />
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "span");
            builder.AddAttribute(1, "data-probe", "overlay");
            builder.CloseElement();
        }
    }

    /// <summary>Contributes the three slot probes.</summary>
    internal void AddSlotProbes()
    {
        Services.AddShellSlot<ConnectionProbe>(ShellSlotNames.HeaderConnection);
        Services.AddShellSlot<IdentityProbe>(ShellSlotNames.HeaderIdentity);
        Services.AddShellSlot<OverlayProbe>(ShellSlotNames.OverlaySession);
    }
}
