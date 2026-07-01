namespace Orleans.Lattice;

/// <summary>
/// One participating tree's slice of a cross-tree atomic write: the logical
/// tree to write into, the key/value entries to commit on that tree, and an
/// optional server-side guard predicate evaluated against each key's pre-saga
/// value during the prepare phase.
/// <para>
/// Passed as a list to
/// <see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync"/>.
/// Every <see cref="TreeId"/> in the list must be distinct - a single
/// cross-tree operation touches each tree at most once.
/// </para>
/// <para>
/// <b>Mutability / safe-copy.</b> This type carries mutable reference-typed
/// members (the <c>Entries</c> list, each entry's value <c>byte[]</c>,
/// the optional mutable <see cref="LatticePredicateNode"/>
/// <see cref="Predicate"/>, and the optional <see cref="EntryDeltas"/> list of
/// per-entry delta buffers), so it is deliberately <b>not</b> marked
/// <c>[Immutable]</c>. Orleans elides the same-silo deep copy on any
/// <c>[Immutable]</c> type, which would alias the caller's list and buffers
/// straight into the coordinator grain's persisted state; leaving the type
/// copy-eligible forces Orleans to deep-copy it across the grain-proxy
/// boundary. The non-aliasing contract is pinned by
/// <c>ImmutableRecordCopyAliasingTests</c>.
/// </para>
/// <para>
/// <b>Per-entry deltas.</b> <see cref="EntryDeltas"/> (when non-null) is
/// aligned 1:1 with <c>Entries</c>: <c>EntryDeltas[i]</c> is the
/// opaque, Orleans-serialised typed CRDT delta to ride the atomic write for
/// <c>Entries[i]</c>, or <see langword="null"/> for a plain last-writer-wins
/// value write. The plain <c>Set</c> / <c>SetWhere</c> builder methods leave
/// it <see langword="null"/>; the internal flag-CRDT membership staging path
/// and the public <c>Set(LatticeStagedCrdtWrite)</c> builder overload (which
/// couples a typed CRDT mutation prepared by a CRDT accessor's <c>Stage*</c>
/// method) populate it.
/// </para>
/// <para>
/// <b>Per-entry deletes.</b> <see cref="EntryDeletes"/> (when non-null) is
/// aligned 1:1 with <c>Entries</c>: <c>EntryDeletes[i]</c> is
/// <see langword="true"/> when <c>Entries[i]</c> is a <b>retraction
/// (tombstone) delete</b> that rides the all-or-nothing batch alongside the
/// upserts, or <see langword="false"/> for a plain value upsert. A delete
/// entry's value buffer is ignored (the builder stages an empty buffer). The
/// whole list is <see langword="null"/> when the slice carries only upserts
/// (the common case, and the only case the <c>Set</c> / <c>SetWhere</c> builder
/// methods produce); <see cref="LatticeAtomicWriteBuilder.Delete(string)"/>
/// populates it. Carrying deletes inside the atomic op lets a re-key
/// projection (a row moving from view key A to view key B) flip the upsert at
/// B and the delete at A as a single visibility change.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreeBatch)]
public readonly record struct LatticeTreeBatch(
    [property: Id(0)] string TreeId,
    [property: Id(1)] List<KeyValuePair<string, byte[]>> Entries,
    [property: Id(2)] LatticePredicateNode? Predicate = null,
    [property: Id(3)] List<byte[]?>? EntryDeltas = null,
    [property: Id(4)] List<bool>? EntryDeletes = null);
