using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafCursorReporter"/> implementation that forwards
/// every report and unregister to the silo-registered
/// <see cref="IWalCursorRegistry"/>. Wired up by
/// <see cref="LatticeServiceCollectionExtensions.AddWalCursorRegistry"/>
/// so a host that opts into the cursor registry automatically promotes
/// every leaf grain to a first-class WAL consumer; a host without the
/// registry leaves the registration absent and the leaf grain skips the
/// report path entirely (the partial <c>BPlusLeafGrain.CursorRegistry</c>
/// resolves the reporter as a nullable service and no-ops when null).
/// </summary>
internal sealed class LeafCursorReporter(
    IWalCursorRegistry registry) : ILeafCursorReporter
{
    /// <inheritdoc />
    public Task ReportAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        CancellationToken cancellationToken)
        => registry.ReportCursorAsync(treeName, consumerId, cursor, cancellationToken);

    /// <inheritdoc />
    public Task UnregisterAsync(
        string treeName,
        string consumerId,
        CancellationToken cancellationToken)
        => registry.UnregisterAsync(treeName, consumerId, cancellationToken);

    /// <inheritdoc />
    public async Task UnregisterTreeAsync(
        string treeName,
        CancellationToken cancellationToken)
    {
        // Snapshot under the registry's own lock (cheap O(consumers
        // per tree)) and filter to the leaf-materialiser prefix so
        // peer / custom consumers registered against the tree are
        // left alone. Only runs at terminal lifecycle events
        // (tree-deletion purge), so the snapshot+iterate cost is
        // amortised over the lifetime of the tree.
        var snapshot = await registry.SnapshotAsync(treeName, cancellationToken).ConfigureAwait(false);
        if (snapshot.Count == 0)
            return;

        var prefix = ILeafCursorReporter.MaterialiserConsumerIdPrefix + treeName + "_";
        for (var i = 0; i < snapshot.Count; i++)
        {
            var consumerId = snapshot[i].ConsumerId;
            if (consumerId.StartsWith(prefix, StringComparison.Ordinal))
            {
                await registry.UnregisterAsync(treeName, consumerId, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
