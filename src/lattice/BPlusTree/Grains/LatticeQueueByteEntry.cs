namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Wire-return shape for a single entry parked on a cluster-internal
/// queue grain: the monotonic id assigned at enqueue time plus the opaque
/// serialized payload. The typed client facade
/// (<see cref="ILatticeQueue{T}"/>) deserializes <see cref="Value"/> into
/// the caller's type, so the grain itself never depends on the payload's
/// CLR shape and the value need not be Orleans-serializable.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeQueueByteEntry)]
[Immutable]
internal readonly record struct LatticeQueueByteEntry
{
    /// <summary>Monotonic per-queue identifier assigned at enqueue time.</summary>
    [Id(0)]
    public long EntryId { get; init; }

    /// <summary>Opaque serialized payload bytes for the entry.</summary>
    [Id(1)]
    public byte[] Value { get; init; }
}
