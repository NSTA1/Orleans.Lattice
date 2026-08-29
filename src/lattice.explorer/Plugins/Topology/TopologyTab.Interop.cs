using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.Plugins.Topology;

/// <summary>
/// The topology surface's browser-side concern: the lifetime of the pan / zoom
/// module and the home-view reset. Split from the load and layout path so the
/// interop teardown - the part that has to tolerate a vanished circuit - reads
/// on its own.
/// </summary>
public partial class TopologyTab
{
    /// <summary>
    /// The pan / zoom module, served from this plugin's own packaged static web
    /// assets rather than the shell's, so the surface ships everything it needs.
    /// </summary>
    private const string InteropModulePath =
        "./_content/Orleans.Lattice.Explorer.Plugins.Topology/topology-interop.js";

    private IJSObjectReference? _module;
    private string? _appliedViewKey;
    private bool _canvasReady;

    private string ViewKey => $"{Selection.Id}|{_showLeaves}|{_graph?.Nodes.Count ?? 0}|{_homeViewBox}";

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        var hasCanvas = _graph is not null && _graph.Nodes.Count > 0;
        if (hasCanvas != _canvasReady)
        {
            _canvasReady = hasCanvas;
            StateHasChanged();
        }

        if (!hasCanvas)
        {
            return;
        }

        try
        {
            _module ??= await JS.InvokeAsync<IJSObjectReference>("import", InteropModulePath);
            await _module.InvokeVoidAsync("attach", _svg);

            if (_appliedViewKey != ViewKey)
            {
                _appliedViewKey = ViewKey;
                await _module.InvokeVoidAsync("home", _svg);
            }
        }
        catch (JSDisconnectedException)
        {
            // The circuit went away (navigation/teardown); nothing to do.
        }
        catch (OperationCanceledException)
        {
            // Render raced with disposal.
        }
    }

    private async Task ResetViewAsync()
    {
        if (_module is null)
        {
            return;
        }

        try
        {
            await _module.InvokeVoidAsync("reset", _svg);
        }
        catch (JSDisconnectedException)
        {
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing && _module is not null)
        {
            var module = _module;
            _module = null;
            _ = DisposeModuleAsync(module);
        }

        base.Dispose(disposing);
    }

    private static async Task DisposeModuleAsync(IJSObjectReference module)
    {
        try
        {
            await module.DisposeAsync();
        }
        catch (JSDisconnectedException)
        {
        }
        catch (OperationCanceledException)
        {
        }
    }
}
