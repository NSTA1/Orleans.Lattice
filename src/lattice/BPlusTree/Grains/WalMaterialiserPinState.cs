using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable persisted state of the <see cref="IWalMaterialiserPinGrain"/>: the
/// per-consumer leaf-materialiser checkpoint frontiers for a single tree.
/// <para>
/// Each entry maps a leaf-materialiser <c>consumerId</c> (the same stable id
/// the leaf reports to the in-memory <see cref="IWalCursorRegistry"/>, of the
/// form <c>{MaterialiserConsumerIdPrefix}{treeName}_{leafGrainId}</c>, optionally
/// partition-suffixed) to the highest <see cref="HybridLogicalClock"/> that
/// leaf has durably checkpointed. The map survives a full silo/cluster restart
/// so the WAL GC can floor its trim point under the slowest leaf's durable
/// checkpoint even before that leaf has re-activated and re-reported into the
/// process-local registry.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalMaterialiserPinState)]
internal sealed class WalMaterialiserPinState
{
    /// <summary>
    /// Durable grain-state name under which the pin store is persisted (in the
    /// <see cref="LatticeOptions.StorageProviderName"/> provider). Defined once
    /// here so the <see cref="WalMaterialiserPinGrain"/>'s
    /// <c>[PersistentState]</c> attribute and the
    /// <see cref="LeafCursorReporter"/>'s teardown direct-store fallback address
    /// the identical durable slot.
    /// </summary>
    public const string StateName = "wal-materialiser-pins";

    /// <summary>
    /// The durable leaf-materialiser pins for this tree, keyed by the
    /// leaf's stable consumer id. The stored value is each leaf's highest
    /// durable checkpoint frontier; <see cref="HybridLogicalClock.Zero"/>
    /// marks a leaf that has activated but never checkpointed (a "block"
    /// pin that keeps the WAL head retained for that leaf until it
    /// advances).
    /// </summary>
    [Id(0)]
    public Dictionary<string, HybridLogicalClock> Pins { get; set; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// The durable leaf-materialiser checkpoint <b>offsets</b> for this tree,
    /// keyed by the same leaf consumer id as <see cref="Pins"/>. The stored
    /// value is each leaf's highest durably-applied WAL offset; <c>-1</c> marks
    /// a leaf that has activated but never applied anything (a "block" pin).
    /// The WAL GC floors its trim point under the lowest offset here so a
    /// low-HLC / high-offset entry (a tombstone-compaction reap re-emitting an
    /// old timestamp at a new offset) is never trimmed before the slowest leaf
    /// has applied it - a case the HLC-space <see cref="Pins"/> floor alone
    /// cannot protect because it is not monotonic in offset. Persisted parallel
    /// to <see cref="Pins"/> under a distinct <see cref="IdAttribute"/> so
    /// state written before this field existed deserialises with an empty map
    /// and is treated conservatively (no offset floor) until the leaves
    /// re-report.
    /// </summary>
    [Id(1)]
    public Dictionary<string, long> Offsets { get; set; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// The <see cref="LatticeOptions.WalMaterialiserPinBuckets"/> value in force
    /// when this slot was last written, recorded so an activation can discover a
    /// layout wider than its own configuration and read the slots it would
    /// otherwise not know to look for.
    /// <para>
    /// Without this, <b>lowering</b> the bucket count would strand every pin
    /// living in a now-out-of-range slot. A stranded pin is invisible to the
    /// trim floor, which is the dangerous direction: the WAL GC could trim past
    /// a leaf that has not yet re-activated to re-report, reintroducing the
    /// cold-restart <c>LeafProjectionStaleException</c> the durable pin store
    /// exists to prevent. Recording the width makes the read self-healing in
    /// both directions - the activation reads the wider of the persisted and
    /// configured layouts, merges everything it finds, and consolidates into the
    /// configured layout on its next write.
    /// </para>
    /// <para>
    /// Zero (the value state written before this field existed deserialises to,
    /// and the value written by the default single-slot layout) means "no
    /// bucketing", so a pre-bucketing deployment reads exactly as it always did.
    /// </para>
    /// </summary>
    [Id(2)]
    public int PersistedBucketCount { get; set; }
}
