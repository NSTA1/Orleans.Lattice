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

    /// <summary>
    /// Compares two entries by value, with <see cref="Value"/> compared by
    /// content. The compiler-generated record-struct equality compares the
    /// <see cref="Value"/> byte array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical entries built from
    /// independently allocated but byte-identical payloads - and, in particular,
    /// an entry and its post-serialization self - would otherwise never compare
    /// equal.
    /// </summary>
    /// <param name="other">The entry to compare against.</param>
    public bool Equals(LatticeQueueByteEntry other) =>
        EntryId == other.EntryId
        && BytesEqual(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(EntryId);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
