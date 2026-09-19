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
    /// Returns a snapshot of every durable leaf-materialiser checkpoint
    /// <b>offset</b> for this tree, keyed by consumer id. The WAL GC reads this
    /// to floor its trim point under the lowest durably-applied offset so a
    /// low-HLC / high-offset entry (a tombstone-compaction reap that re-emits an
    /// old timestamp at a new WAL offset) is never trimmed before the slowest
    /// leaf has applied it - a case the HLC-space <see cref="GetPinsAsync"/>
    /// floor alone cannot cover because the WAL is not HLC-monotonic in offset
    /// once reaps are in play.
    /// <para>
    /// A <c>-1</c> value is <b>excluded</b> from the floor rather than pinning
    /// it, and an absent consumer does not constrain it either (it contributes
    /// no entry to minimise over). <c>-1</c> means "this partition has no
    /// WAL-replay dependency", which arises three ways, and it is worth knowing
    /// which because only two of them are covered by a block pin:
    /// <list type="bullet">
    /// <item><description>
    /// A <b>genuinely empty</b> partition - no durable checkpoint and no live
    /// cache row - has no committed prefix to lose, so nothing needs retaining.
    /// <c>BPlusLeafGrain.ResolveDurablePinForPartition</c> reports this case
    /// with the leaf's <em>real</em> frontier and a <c>-1</c> offset, so it
    /// carries <b>no</b> block pin. It does not need one.
    /// </description></item>
    /// <item><description>
    /// A <b>data-bearing but not durably recoverable</b> partition (never
    /// checkpointed, or checkpointed with no snapshot covering the prefix)
    /// retains a <see cref="HybridLogicalClock.Zero"/> block pin alongside its
    /// <c>-1</c>. That pin is what enforces retention here, by disabling the
    /// cursor branch of the GC predicate outright - see <see cref="GetPinsAsync"/>.
    /// </description></item>
    /// <item><description>
    /// A <b>split sibling</b> whose entries arrived by in-memory handoff from
    /// the donor rather than by WAL replay.
    /// </description></item>
    /// </list>
    /// </para>
    /// <para>
    /// <b>The exclusion is a handoff, not a gap - and not a second independent
    /// guard.</b> Where a <c>-1</c> is covered, it is covered by the HLC block
    /// pin and by nothing on this seam: the offset floor is abstaining, on the
    /// assumption that the block pin is doing the work. Lifting a block pin on
    /// a partition still reporting <c>-1</c> therefore removes <em>both</em>
    /// protections at once and authorises a trim over a prefix the materialiser
    /// has never replayed. Read a tree whose offsets are uniformly <c>-1</c> as
    /// "retention is resting entirely on the HLC plane", never as "two planes
    /// agree it is safe".
    /// </para>
    /// <para>
    /// Absence is <b>not</b> the same state as a reported <c>-1</c>: a reported
    /// <c>-1</c> comes from a leaf that is participating and has told us it owes
    /// nothing, whereas an absent leaf has told us nothing at all and carries no
    /// HLC cover from this seam. Absence is deliberately left non-constraining -
    /// treating it as offset <c>0</c> would pin the WAL forever for any
    /// permanently-departed leaf - which is a known limitation of the floor
    /// (issue #2314), not a property to rely on.
    /// </para>
    /// </summary>
    Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync();

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
