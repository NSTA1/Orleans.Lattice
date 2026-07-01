using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A single key/value row inside a <see cref="LeafSnapshotBlob"/>.
/// Carries the canonical <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> encoded byte-row
/// exactly as it appears in the leaf grain's per-activation entry
/// cache - no typed CRDT round-trip happens on the snapshot capture
/// or restore path. The blob is therefore a verbatim copy of the
/// projection at the captured WAL offset.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.LeafSnapshotRow)]
internal readonly record struct LeafSnapshotRow(
    [property: Id(0)] string Key,
    [property: Id(1)] LwwValue<byte[]> Value);
