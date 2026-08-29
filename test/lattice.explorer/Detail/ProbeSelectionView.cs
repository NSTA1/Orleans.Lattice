using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Tests.Detail;

/// <summary>
/// Records which per-selection views the panel mounted and disposed, in order,
/// so a test can assert on what actually rendered rather than on what the panel
/// says it would render.
/// </summary>
/// <remarks>
/// Resolved from the container by the probe views themselves, so it works for a
/// view the panel mounts indirectly through <see cref="DynamicComponent"/>.
/// </remarks>
internal sealed class SelectionViewLog
{
    /// <summary>Every probe view instance that initialized, in mount order.</summary>
    public List<ProbeSelectionView> Mounted { get; } = [];

    /// <summary>Every probe view instance that was disposed, in disposal order.</summary>
    public List<ProbeSelectionView> Disposed { get; } = [];

    /// <summary>The types mounted, in mount order.</summary>
    public IReadOnlyList<Type> MountedTypes => [.. Mounted.Select(view => view.GetType())];
}

/// <summary>
/// A stand-in per-selection plugin view. It derives from the real
/// <see cref="SelectionPluginViewBase"/>, so a test exercises the actual
/// parameter and cancellation contract the six shipped surfaces inherit rather
/// than a lookalike.
/// </summary>
internal abstract class ProbeSelectionView : SelectionPluginViewBase
{
    /// <summary>The shared mount and disposal log, resolved from the container.</summary>
    [Inject]
    public SelectionViewLog Log { get; set; } = default!;

    /// <summary>The selection this instance was mounted with.</summary>
    public CatalogItem? MountedSelection { get; private set; }

    /// <summary>The lifetime token, exposed so a test can assert it is cancelled on re-mount.</summary>
    public CancellationToken Token => TabToken;

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        MountedSelection = Selection;
        Log.Mounted.Add(this);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Log.Disposed.Add(this);
        }

        base.Dispose(disposing);
    }
}

/// <summary>The first stand-in view, so one surface can be told from another.</summary>
internal sealed class AlphaProbeView : ProbeSelectionView;

/// <summary>The second stand-in view.</summary>
internal sealed class BetaProbeView : ProbeSelectionView;

/// <summary>The third stand-in view.</summary>
internal sealed class GammaProbeView : ProbeSelectionView;
