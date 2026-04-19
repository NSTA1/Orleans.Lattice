using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for resizing a tree by changing its <see cref="LatticeOptions.MaxLeafKeys"/>
/// and/or <see cref="LatticeOptions.MaxInternalChildren"/>. One activation exists per tree,
/// keyed by <c>{treeId}</c>.
/// <para>
/// Resize uses an offline snapshot to create a new physical tree with the desired sizing,
/// then swaps the tree alias so that reads and writes are redirected to the new tree.
/// The old physical tree is soft-deleted and will be purged after the configured
/// <see cref="LatticeOptions.SoftDeleteDuration"/>. During the soft-delete window,
/// the resize can be undone with <see cref="UndoResizeAsync"/>.
/// </para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.ITreeResizeGrain)]
public interface ITreeResizeGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a resize of the tree. An offline snapshot is taken to a new
    /// physical tree with the specified sizing. Once the snapshot completes,
    /// the tree alias is swapped and the old physical tree is soft-deleted.
    /// <para>
    /// Idempotent — if a resize is already in progress with the same parameters,
    /// the call is a no-op. If a resize is in progress with different parameters,
    /// an <see cref="InvalidOperationException"/> is thrown.
    /// </para>
    /// </summary>
    /// <param name="newMaxLeafKeys">The new maximum number of keys per leaf node. Must be greater than 1.</param>
    /// <param name="newMaxInternalChildren">The new maximum number of children per internal node. Must be greater than 2.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown if a resize is already in progress with different parameters.
    /// </exception>
    Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren);

    /// <summary>
    /// Runs the resize operation synchronously — processes all remaining phases
    /// in a single call. Intended for integration testing and manual triggers.
    /// Must be called after <see cref="ResizeAsync"/> to start the operation.
    /// </summary>
    Task RunResizePassAsync();

    /// <summary>
    /// Undoes the most recent resize by recovering the old physical tree,
    /// removing the alias, restoring the original registry configuration,
    /// and deleting the new snapshot tree. Only available while the old tree
    /// is still within its soft-delete window (before purge).
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if no completed resize exists to undo, or if the old tree has
    /// already been purged.
    /// </exception>
    Task UndoResizeAsync();

    /// <summary>
    /// Returns <c>true</c> if no resize is in progress — either the most recent
    /// resize has completed or no resize has ever been initiated (vacuously complete).
    /// </summary>
    Task<bool> IsCompleteAsync();
}
