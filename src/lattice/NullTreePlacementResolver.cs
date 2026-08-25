namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ITreePlacementResolver"/>: resolves every tree to
/// <see cref="TreePhysicalPlacement.Default"/> - the catalog's baseline WAL provider
/// key and no placement filter. Registered by <c>AddLattice</c> as the safe default
/// so a consumer of the seam always resolves an instance even when the tenancy
/// add-on is not registered, preserving core's byte-for-byte behaviour. The tenancy
/// package replaces it with the real placement-reading implementation.
/// </summary>
/// <remarks>
/// The baseline result and its wrapping <see cref="ValueTask{TResult}"/> are cached
/// in a <c>static readonly</c> field, so every call returns the same
/// synchronously-completed result with no per-call allocation.
/// </remarks>
internal sealed class NullTreePlacementResolver : ITreePlacementResolver
{
    private static readonly ValueTask<TreePhysicalPlacement> DefaultResult =
        new(TreePhysicalPlacement.Default);

    /// <inheritdoc />
    public bool TryResolveForRegistration(string treeId, out TreePhysicalPlacement placement)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        placement = TreePhysicalPlacement.Default;
        return true;
    }

    /// <inheritdoc />
    public ValueTask<TreePhysicalPlacement> ResolveForRegistrationAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return DefaultResult;
    }
}
