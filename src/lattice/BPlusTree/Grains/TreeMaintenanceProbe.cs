namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Reads whether any whole-tree maintenance operation (resize, reshard, merge or
/// snapshot) is in flight for a tree by asking the four coordinator grains
/// directly, rather than through the corresponding <see cref="ILattice"/> status
/// verbs.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="ILattice"/> verbs (<c>IsResizeCompleteAsync</c>,
/// <c>IsReshardCompleteAsync</c>, <c>IsMergeCompleteAsync</c>,
/// <c>IsSnapshotCompleteAsync</c>) each forward to exactly one of these
/// coordinators after a reserved-namespace guard and an access-gate / tenant-read
/// admission check. The in-silo autonomic pollers (the hot-shard monitor and the
/// shard-healing orchestrator) only ever call them under
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>, where both the gate
/// and the admission check are bypassed, so the <see cref="ILattice"/> hop
/// contributes nothing but a second grain call per verb. Going straight to the
/// coordinators halves the grain calls of every maintenance probe (eight serial
/// calls to four on the steady-state idle path) and stops a background timer from
/// queuing turns on the tree's user-facing <see cref="ILattice"/> activation.
/// </para>
/// <para>
/// The reserved-namespace guard is retained so a probe against an internal
/// system tree still fails exactly as the <see cref="ILattice"/> verb did, and the
/// verbs are evaluated in the same order with the same short-circuit, so a busy
/// resize still spares the remaining three coordinators.
/// </para>
/// </remarks>
internal static class TreeMaintenanceProbe
{
    /// <summary>
    /// Returns <see langword="true"/> when any of resize, reshard, merge or
    /// snapshot is in flight for <paramref name="treeId"/>.
    /// </summary>
    /// <exception cref="LatticeReservedTreeNamespaceException">
    /// <paramref name="treeId"/> is in the reserved internal system-tree namespace,
    /// matching the rejection the equivalent <see cref="ILattice"/> verbs raise.
    /// </exception>
    public static async Task<bool> IsMaintenanceInFlightAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(treeId);

        ThrowIfSystemTree(treeId);

        return !await grainFactory.GetGrain<ITreeResizeGrain>(treeId).IsIdleAsync()
            || !await grainFactory.GetGrain<ITreeReshardGrain>(treeId).IsIdleAsync()
            || !await grainFactory.GetGrain<ITreeMergeGrain>(treeId).IsCompleteAsync()
            || !await grainFactory.GetGrain<ITreeSnapshotGrain>(treeId).IsIdleAsync();
    }

    /// <summary>
    /// Probes snapshot first, then merge, without calling back into a reshard or
    /// consolidation coordinator that is itself waiting on this probe.
    /// </summary>
    public static async Task<bool> IsSnapshotOrMergeInFlightAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(treeId);
        ThrowIfSystemTree(treeId);

        return !await grainFactory.GetGrain<ITreeSnapshotGrain>(treeId).IsIdleAsync()
            || !await grainFactory.GetGrain<ITreeMergeGrain>(treeId).IsCompleteAsync();
    }

    private static void ThrowIfSystemTree(string treeId)
    {
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            throw new LatticeReservedTreeNamespaceException(
                treeId,
                $"Tree ID '{treeId}' is reserved for internal Lattice system trees and cannot be addressed via the public ILattice surface. Choose a tree name that does not start with '{LatticeConstants.SystemTreePrefix}'.");
    }
}
