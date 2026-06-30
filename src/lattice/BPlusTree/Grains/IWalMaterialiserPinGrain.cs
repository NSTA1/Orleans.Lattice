using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Cluster-wide durable registry of leaf-materialiser WAL checkpoint pins for a
/// single tree, keyed by the tree id. A single activation per tree persists the
/// highest durable checkpoint <see cref="HybridLogicalClock"/> each leaf has
/// reached so the per-shard WAL garbage collector can floor its trim point
/// under the slowest leaf's durable frontier <b>even across a full silo or
/// cluster restart</b>.
/// <para>
/// This is the durability backstop for the in-memory
/// <see cref="IWalCursorRegistry"/>: that registry is process-local and is
/// wiped on restart, after which a forward consumer (for example the
/// replication shipper) re-reports its durably-persisted, further-advanced
/// cursor before the dormant leaves have re-activated and re-reported their
/// own (lower) pins. Without this grain the GC would compute its trim floor
/// over the forward consumer alone and trim past the leaf's durable
/// checkpoint, losing the committed-but-not-yet-checkpointed WAL tail. The GC
/// consults this grain only for consumers <i>missing</i> from the in-memory
/// registry, so steady-state trimming (every leaf present) is unchanged.
/// </para>
/// <para>
/// Writes are made off the foreground/checkpoint hot path: the leaf reports its
/// frontier fire-and-forget and coalesced through
/// <see cref="ILeafCursorReporter.NoteDurableMaterialiserFrontier"/>. A stale
/// (older) durable pin is always GC-safe - it only retains more WAL - so the
/// durable record never needs up-to-the-millisecond accuracy.
/// </para>
/// </summary>
[Alias(TypeAliases.IWalMaterialiserPinGrain)]
internal interface IWalMaterialiserPinGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records (monotonic-max merge) the durable checkpoint
    /// <paramref name="frontier"/> for <paramref name="consumerId"/>. A
    /// report whose frontier is not strictly greater than the stored value
    /// is coalesced (no write), preserving the monotonic, never-rolls-back
    /// contract. A <see cref="HybridLogicalClock.Zero"/> frontier seeds a
    /// "block" pin for a leaf that has activated but never checkpointed.
    /// </summary>
    /// <param name="consumerId">Stable leaf-materialiser consumer id. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="frontier">Highest HLC the leaf has durably checkpointed.</param>
    Task ReportAsync(string consumerId, HybridLogicalClock frontier);

    /// <summary>
    /// Coalesced batch form of <see cref="ReportAsync"/>: records a
    /// monotonic-max merge for every report in <paramref name="reports"/> in a
    /// single grain round-trip. Each report is merged exactly as
    /// <see cref="ReportAsync"/> would merge it (a frontier not strictly
    /// greater than the stored value is coalesced). The durable write is
    /// debounced through the pin store's coalescing window, so a burst of
    /// reports collapses to at most one <c>WriteStateAsync</c> per window.
    /// </summary>
    /// <param name="reports">The pin reports to merge. Each report's consumer id must not be <see langword="null"/> or whitespace.</param>
    Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports);

    /// <summary>
    /// Durable batch seed: records a monotonic-max merge for every report in
    /// <paramref name="reports"/> and <b>awaits</b> a single
    /// <c>WriteStateAsync</c> covering the whole batch (and any pending
    /// coalesced advances). Used by a leaf at birth to plant
    /// <see cref="HybridLogicalClock.Zero"/> "block" pins for all of its WAL
    /// partitions <em>before</em> its data becomes reachable in the WAL,
    /// collapsing what was one awaited durable write per partition into one
    /// awaited write for the batch routed to this shard.
    /// </summary>
    /// <param name="reports">The pin reports to seed durably. Each report's consumer id must not be <see langword="null"/> or whitespace.</param>
    Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports);

    /// <summary>
    /// Returns a snapshot of every durable leaf-materialiser pin for this
    /// tree, keyed by consumer id. The WAL GC reads this to floor its trim
    /// point under the slowest durable leaf frontier for consumers absent
    /// from the in-memory cursor registry.
    /// </summary>
    Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync();

    /// <summary>
    /// Removes <paramref name="consumerId"/>'s durable pin. Idempotent: a
    /// no-op when the consumer has no pin. Reserved for terminal lifecycle
    /// events (leaf eviction during a purge) so a deleted leaf does not pin
    /// the WAL forever.
    /// </summary>
    /// <param name="consumerId">Stable leaf-materialiser consumer id to remove. Must not be <see langword="null"/> or whitespace.</param>
    Task RemoveAsync(string consumerId);

    /// <summary>
    /// Clears every durable pin for this tree. Reserved for terminal
    /// tree-lifecycle events (tree deletion / purge) so the per-shard WAL
    /// GC is no longer floored by stale leaf pins after the tree's data has
    /// been removed. Idempotent: a no-op when no pins are stored.
    /// </summary>
    Task ClearAsync();
}
