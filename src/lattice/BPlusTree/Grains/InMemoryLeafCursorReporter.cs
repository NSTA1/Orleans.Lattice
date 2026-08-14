using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Lightweight default <see cref="ILeafCursorReporter"/> registered by
/// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> so that every
/// leaf-as-materialiser publishes its applied checkpoint frontier into the
/// always-on in-memory <see cref="IWalCursorRegistry"/> on every deployment -
/// which is what the WAL saturation sampler reads to compute materialiser
/// drain lag. Reporting the in-memory cursor is the only thing the drain-lag
/// back-pressure input needs, so wiring it by default makes that signal live
/// for every write workload rather than only on hosts that opt into the
/// materialiser / replication stack.
/// <para>
/// This reporter deliberately does <b>only</b> the cheap in-memory work: it
/// forwards reports and unregistrations to the registry and treats all of the
/// durable-pin methods as no-ops. The durable cross-restart GC trim-floor
/// backstop (the sharded cluster-wide <see cref="IWalMaterialiserPinGrain"/>
/// store, which carries real write amplification) stays opt-in: a host that
/// trims the WAL through the GC calls
/// <see cref="LatticeServiceCollectionExtensions.AddWalCursorRegistry"/>
/// (directly, or transitively through <c>AddLatticeViews</c> /
/// <c>AddLatticeReplication</c> / the Azure-table storage package), which
/// <c>Replace</c>s this default with the durable-pin-aware
/// <see cref="LeafCursorReporter"/>. A host that never trims has nothing to
/// protect, so the no-op durable path is correct - and matches the documented
/// "implementations with no durable backing treat this as a no-op" contract.
/// </para>
/// </summary>
internal sealed class InMemoryLeafCursorReporter(IWalCursorRegistry registry) : ILeafCursorReporter
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
        // In-memory equivalent of LeafCursorReporter.UnregisterTreeAsync: drop
        // only the leaf-materialiser-prefixed cursors for the tree, leaving any
        // peer / custom consumer registered against it intact. There is no
        // durable pin store to clear in this lightweight reporter.
        var snapshot = await registry.SnapshotAsync(treeName, cancellationToken).ConfigureAwait(false);

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

    /// <inheritdoc />
    public void NoteDurableMaterialiserFrontier(
        string treeName,
        string consumerId,
        HybridLogicalClock frontier,
        long checkpointOffset)
    {
        // No durable backing: the cross-restart GC trim-floor pin is provided
        // by the full LeafCursorReporter, wired only through AddWalCursorRegistry.
    }

    /// <inheritdoc />
    public Task SeedDurableMaterialiserBlockAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock frontier,
        CancellationToken cancellationToken)
        => Task.CompletedTask;

    /// <inheritdoc />
    public Task SeedDurableMaterialiserBlockManyAsync(
        string treeName,
        IReadOnlyList<MaterialiserPinReport> reports,
        CancellationToken cancellationToken)
        => Task.CompletedTask;

    /// <inheritdoc />
    public Task FlushDurableMaterialiserFrontierAsync(
        string treeName,
        IReadOnlyList<MaterialiserPinReport> reports,
        CancellationToken cancellationToken)
        => Task.CompletedTask;
}
