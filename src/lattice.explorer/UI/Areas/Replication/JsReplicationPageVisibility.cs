using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// Observes the page's visibility through the area's module, scoped per circuit.
/// Without script (a prerender, a dropped circuit) the page reads as visible, so a
/// refresh cadence keeps running rather than silently stopping.
/// </summary>
internal sealed class JsReplicationPageVisibility : IReplicationPageVisibility, IAsyncDisposable
{
    private readonly IJSRuntime _js;
    private DotNetObjectReference<JsReplicationPageVisibility>? _self;
    private IJSObjectReference? _module;
    private IJSObjectReference? _observation;
    private bool _started;
    private volatile bool _visible = true;

    /// <summary>Creates the observer over the circuit's script runtime.</summary>
    /// <param name="js">The circuit's script runtime.</param>
    public JsReplicationPageVisibility(IJSRuntime js)
    {
        ArgumentNullException.ThrowIfNull(js);
        _js = js;
    }

    /// <inheritdoc />
    public bool IsVisible => _visible;

    /// <inheritdoc />
    public event Action? Changed;

    /// <inheritdoc />
    public async ValueTask StartAsync()
    {
        if (_started)
        {
            return;
        }

        _started = true;
        try
        {
            _self = DotNetObjectReference.Create(this);
            _module = await _js.InvokeAsync<IJSObjectReference>("import", ReplicationAssets.ModuleSpecifier).ConfigureAwait(false);
            _observation = await _module.InvokeAsync<IJSObjectReference?>("observeVisibility", _self).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is JSDisconnectedException or JSException or InvalidOperationException or TaskCanceledException)
        {
            // No script: the page stays "visible" and the cadence keeps running.
            _started = false;
        }
    }

    /// <summary>Called by the module whenever the document's visibility changes.</summary>
    /// <param name="visible">Whether the document is now visible.</param>
    [JSInvokable]
    public void OnVisibilityChanged(bool visible)
    {
        if (_visible == visible)
        {
            return;
        }

        _visible = visible;
        Changed?.Invoke();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_observation is not null)
            {
                await _observation.InvokeVoidAsync("dispose").ConfigureAwait(false);
                await _observation.DisposeAsync().ConfigureAwait(false);
            }

            if (_module is not null)
            {
                await _module.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is JSDisconnectedException or JSException or TaskCanceledException)
        {
            // The circuit has gone; so has the module.
        }

        _self?.Dispose();
    }
}
