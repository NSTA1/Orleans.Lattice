using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.UI.Detail;

/// <summary>
/// The contract every detail tab implements. The shell mounts exactly one tab at
/// a time and re-mounts it whenever the selection changes, so a tab receives a
/// stable <see cref="Selection"/> for its lifetime and resets its own state on
/// each selection change. <see cref="TabToken"/> is cancelled when the tab is
/// disposed (on selection change or tab switch), so in-flight loads against the
/// previous selection are abandoned. Tabs obtain the state-API connection and
/// other services through dependency injection.
/// </summary>
public abstract class DetailTabBase : ComponentBase, IDisposable
{
    private readonly CancellationTokenSource _cts = new();

    /// <summary>The selected tree or view this tab renders. Stable for the tab's lifetime.</summary>
    [Parameter]
    [EditorRequired]
    public CatalogItem Selection { get; set; } = default!;

    /// <summary>
    /// A token that is cancelled when this tab instance is disposed. Tabs pass it
    /// to their loads so work for a superseded selection is cancelled.
    /// </summary>
    protected CancellationToken TabToken => _cts.Token;

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Cancels the tab token. Override to release additional resources, calling
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
