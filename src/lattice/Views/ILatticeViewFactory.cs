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

    /// <summary>
    /// Opens a read handle for a materialised view that <b>already exists</b>,
    /// resolved by name from the view catalog, the startup declarations, or the
    /// durable runtime-view registry - without re-supplying the source tree or the
    /// view definition. Use this when a caller only needs to <em>read</em> a view
    /// (or observe its lag / force a rebuild) and does not hold the projection: the
    /// returned <see cref="ILatticeView"/> resolves the maintainer's active
    /// generation on each read, so it is the supported alternative to binding the
    /// backing <c>view-{name}</c> tree directly (which a rebuild can leave stale).
    /// <para>
    /// Returns <see langword="null"/> when no view named <paramref name="viewName"/>
    /// is registered (it was never created, or was deleted). This does not activate
    /// or create anything; the maintainer comes online lazily on the first read
    /// through the handle.
    /// </para>
    /// </summary>
    /// <param name="viewName">The logical view name; the view tree is resolved as <c>view-{viewName}</c>.</param>
    /// <param name="cancellationToken">Cancellation token observed during the durable-registry lookup.</param>
    Task<ILatticeView?> GetAsync(string viewName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the materialised view named <paramref name="viewName"/>: stops and
    /// decommissions its maintainer (unregistering the keepalive reminder,
    /// releasing the source WAL pin, and clearing the durable checkpoint), deletes
    /// every backing <c>view-{viewName}</c> / <c>view-{viewName}#g{N}</c>
    /// generation through the standard tree-deletion machinery, and removes the
    /// view's catalog entry and durable runtime registration. After deletion the
    /// view name is free to be re-created and no orphaned reminder, checkpoint, or
    /// tree remains.
    /// <para>
    /// Deleting a view that does not exist (or was already deleted) is an
    /// idempotent no-op. Deleting a view that was declared at startup via
    /// <c>AddLatticeViews</c> is rejected with an
    /// <see cref="InvalidOperationException"/>, because the declaration would
    /// re-create the view on the next silo start.
    /// </para>
    /// </summary>
    /// <param name="viewName">The logical view name to delete.</param>
    /// <param name="cancellationToken">Cancellation token observed during teardown.</param>
    Task DeleteAsync(string viewName, CancellationToken cancellationToken = default);
}
