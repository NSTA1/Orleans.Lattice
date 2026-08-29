namespace Orleans.Lattice.Explorer.Plugins.Selection;

/// <summary>
/// The default <see cref="ISelectionNestedSurfaceRegistry"/>: it indexes every
/// registered <see cref="ISelectionNestedSurface"/> by id once, at construction,
/// so a lookup on a render path is a dictionary probe rather than a scan.
/// </summary>
public sealed class SelectionNestedSurfaceRegistry : ISelectionNestedSurfaceRegistry
{
    private readonly Dictionary<string, Type> _views;

    /// <summary>
    /// Indexes <paramref name="surfaces"/> by id. A later contribution for an id
    /// already present is ignored, so a package that registers itself from more
    /// than one composition helper is idempotent rather than a hard failure on a
    /// render path.
    /// </summary>
    /// <param name="surfaces">The registered nested surfaces. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="surfaces"/> is <see langword="null"/>.</exception>
    public SelectionNestedSurfaceRegistry(IEnumerable<ISelectionNestedSurface> surfaces)
    {
        ArgumentNullException.ThrowIfNull(surfaces);

        _views = new Dictionary<string, Type>(StringComparer.Ordinal);
        foreach (var surface in surfaces)
        {
            if (surface is null)
            {
                continue;
            }

            _views.TryAdd(surface.SurfaceId, surface.ViewType);
        }
    }

    /// <inheritdoc />
    public Type? Find(string surfaceId)
    {
        ArgumentNullException.ThrowIfNull(surfaceId);
        return _views.TryGetValue(surfaceId, out var view) ? view : null;
    }
}
