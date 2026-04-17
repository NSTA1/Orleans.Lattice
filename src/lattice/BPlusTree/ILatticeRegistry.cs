using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal grain that manages the tree registry — a Lattice tree
/// (<see cref="LatticeConstants.RegistryTreeId"/>) whose keys are user tree IDs
/// and whose values are serialized <see cref="State.TreeRegistryEntry"/> records.
/// <para>
/// Provides tree existence checks, per-tree configuration overrides, and
/// enumeration of all known tree IDs. The registry tree itself uses the
/// <see cref="LatticeConstants.SystemTreePrefix"/> and is excluded from
/// self-registration.
/// </para>
/// Key format: singleton — use <see cref="LatticeConstants.RegistryTreeId"/> as the grain key.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface ILatticeRegistry : IGrainWithStringKey
{
    /// <summary>
    /// Registers a tree in the registry. If the tree already exists, this is a no-op
    /// (existing config is preserved). Must be called before the first data write to
    /// the tree succeeds.
    /// </summary>
    /// <param name="treeId">The tree ID to register.</param>
    /// <param name="entry">
    /// Optional configuration overrides. Pass <c>null</c> to register with default options.
    /// </param>
    Task RegisterAsync(string treeId, State.TreeRegistryEntry? entry = null);

    /// <summary>
    /// Updates the registry entry for a tree, replacing any previous
    /// configuration overrides. If the tree is not yet registered, it is
    /// registered with the given entry (upsert semantics).
    /// </summary>
    Task UpdateAsync(string treeId, State.TreeRegistryEntry entry);

    /// <summary>
    /// Removes a tree from the registry. Idempotent — no-op if the tree is not registered.
    /// </summary>
    Task UnregisterAsync(string treeId);

    /// <summary>Returns <c>true</c> if the tree is registered.</summary>
    Task<bool> ExistsAsync(string treeId);

    /// <summary>
    /// Returns the <see cref="State.TreeRegistryEntry"/> for the given tree,
    /// or <c>null</c> if not registered.
    /// </summary>
    Task<State.TreeRegistryEntry?> GetEntryAsync(string treeId);

    /// <summary>
    /// Returns all registered tree IDs in sorted order.
    /// </summary>
    Task<IReadOnlyList<string>> GetAllTreeIdsAsync();

    /// <summary>
    /// Sets a tree alias so that the logical <paramref name="treeId"/> maps to
    /// <paramref name="physicalTreeId"/>. All subsequent reads and writes routed
    /// through <see cref="ILattice"/> will target the physical tree instead.
    /// <para>
    /// Only a single level of indirection is allowed — <paramref name="physicalTreeId"/>
    /// must not itself be aliased. Throws <see cref="InvalidOperationException"/> if
    /// this constraint would be violated.
    /// </para>
    /// </summary>
    Task SetAliasAsync(string treeId, string physicalTreeId);

    /// <summary>
    /// Removes the alias for <paramref name="treeId"/>, restoring it to use
    /// itself as the physical tree ID. No-op if no alias is set.
    /// </summary>
    Task RemoveAliasAsync(string treeId);

    /// <summary>
    /// Resolves the physical tree ID for the given logical <paramref name="treeId"/>.
    /// Returns <paramref name="treeId"/> itself if no alias is set.
    /// </summary>
    Task<string> ResolveAsync(string treeId);
}
