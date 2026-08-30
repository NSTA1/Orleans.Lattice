namespace Orleans.Lattice.Explorer.Plugins.Selection;

/// <summary>
/// A view one per-selection surface renders <em>inside</em> another, contributed
/// by a package that neither surface references.
/// <para>
/// The tier's strip is not the only place a surface appears. The value
/// drill-down surface, for example, opens the revision timeline for the key an
/// operator has drilled into, in its own detail panel, behind a per-row History
/// button - it is not a tab and never has been. Rendering that inline would
/// normally force the two packages to reference each other; this seam is how
/// they avoid it. The hosting surface asks the registry for a nested view by
/// stable id and renders whatever <see cref="Type"/> comes back, exactly as the
/// shell renders a plugin, and shows no affordance at all when nothing is
/// registered.
/// </para>
/// <para>
/// A nested view receives the same <see cref="SelectionPluginViewBase.Selection"/>
/// parameter a tier view does, so it derives from the same base and inherits the
/// same cancel-on-dispose contract.
/// </para>
/// </summary>
public interface ISelectionNestedSurface
{
    /// <summary>
    /// The stable id this view is contributed under, from
    /// <see cref="SelectionNestedSurfaceKeys"/>. Compared with
    /// <see cref="StringComparer.Ordinal"/>, so casing is significant.
    /// </summary>
    string SurfaceId { get; }

    /// <summary>
    /// The component type the hosting surface renders. Typed as
    /// <see cref="Type"/> so the registry carries no compile-time dependency on
    /// the contributing package.
    /// </summary>
    Type ViewType { get; }
}
