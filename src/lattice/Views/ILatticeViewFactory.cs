namespace Orleans.Lattice;

/// <summary>
/// Resolves <see cref="ILatticeView"/> handles that are pre-wired to the host's
/// view-maintenance infrastructure.
/// <para>
/// This is the entry point for opening a materialised view. The factory captures
/// the injectable <see cref="ILatticeReplicationContext"/> seam (views require a
/// WAL provider, registered by <c>AddLattice</c> and optionally backed by a
/// durable provider), so the underlying maintainer, commit-log reader, and cursor
/// registry are sourced from server configuration rather than threaded through
/// every call site.
/// </para>
/// <para>
/// Registered as a singleton by <c>ISiloBuilder.AddLatticeViews(...)</c> in the
/// core <c>Orleans.Lattice</c> package.
/// </para>
/// </summary>
public interface ILatticeViewFactory
{
    /// <summary>
    /// Opens the materialised view named <paramref name="viewName"/> over
    /// <paramref name="source"/>, ensuring its maintainer is active and binding
    /// the supplied <paramref name="definition"/>'s projection.
    /// </summary>
    /// <param name="source">The source tree the view is derived from.</param>
    /// <param name="viewName">The logical view name; the view tree is resolved as <c>view-{viewName}</c>.</param>
    /// <param name="definition">The view definition carrying the projection.</param>
    ILatticeView Create(ILattice source, string viewName, LatticeViewDefinition definition);
}
