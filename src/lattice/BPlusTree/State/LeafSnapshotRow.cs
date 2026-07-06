using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A single key/value row inside a <see cref="LeafSnapshotBlob"/>.
/// Carries the canonical <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> encoded byte-row
/// exactly as it appears in the leaf grain's per-activation entry
/// cache - no typed CRDT round-trip happens on the snapshot capture
/// or restore path. The blob is therefore a verbatim copy of the
/// projection at the captured WAL offset.
/// <para>
/// <see cref="MergeMode"/> is the durable per-key convergence discriminator:
/// the <see cref="LatticeMergeMode"/> the key was last written under, folded
/// through from the mutation <c>Mode</c> on the write/replay path. It is
/// <see langword="null"/> for a plain last-writer-wins key and for any row
/// persisted before the discriminator existed, so a reader that cares about
/// per-key mode (the backup capture engine) falls back to the declared tree
/// mode whenever it is <see langword="null"/> - making legacy rows
/// indistinguishable from today's coarse tree-mode labelling.
/// </para>
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.LeafSnapshotRow)]
internal readonly record struct LeafSnapshotRow(
    [property: Id(0)] string Key,
    [property: Id(1)] LwwValue<byte[]> Value,
    [property: Id(2)] LatticeMergeMode? MergeMode = null);
