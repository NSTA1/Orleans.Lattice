using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.UI.Detail;

/// <summary>
/// The base a per-selection plugin's view derives from. It supersedes the
/// retired <c>DetailTabBase</c>: what a view <em>is</em> is now declared by the
/// plugin contract (<see cref="Orleans.Lattice.Explorer.Plugins.IExplorerPlugin"/>),
/// and this type carries only the two things the host cannot express through
/// <c>DynamicComponent</c> parameters - the selection the view renders and the
/// cancellation token tied to its lifetime.
/// <para>
/// The host mounts exactly one view per selection plugin at a time and re-mounts
/// it whenever the selection or the active plugin changes, so a view receives a
/// stable <see cref="Selection"/> for its lifetime and resets its own state on
/// each change. <see cref="TabToken"/> is cancelled when the view is disposed
/// (on selection change or tab switch), so in-flight loads against the previous
/// selection are abandoned. Views obtain the state-API connection and other
/// services through dependency injection.
/// </para>
/// </summary>
public abstract class SelectionPluginViewBase : ComponentBase, IDisposable
{
    private readonly CancellationTokenSource _cts = new();

    /// <summary>The selected tree or view this view renders. Stable for its lifetime.</summary>
    [Parameter]
    [EditorRequired]
    public CatalogItem Selection { get; set; } = default!;

    /// <summary>
    /// A token that is cancelled when this view instance is disposed. Views pass
    /// it to their loads so work for a superseded selection is cancelled.
    /// </summary>
    protected CancellationToken TabToken => _cts.Token;

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Cancels the view token. Override to release additional resources, calling
    /// the base implementation.
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        if (!_cts.IsCancellationRequested)
        {
            _cts.Cancel();
        }

        _cts.Dispose();
    }
}
