using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Adapters;

/// <summary>
/// Adapter that forwards <see cref="ILeafCursorReporter"/> calls from the
/// core <see cref="BPlusLeafGrain"/> hot path to the replication
/// package's <see cref="ILatticeReplicationCursorRegistry"/>. Registered
/// in DI by <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// so a host that adds replication automatically promotes every leaf
/// grain to a first-class WAL consumer, while a host without replication
/// leaves the registration absent and the leaf grain skips the report
/// path entirely.
/// </summary>
internal sealed class LeafCursorReporter(
    ILatticeReplicationCursorRegistry registry) : ILeafCursorReporter
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