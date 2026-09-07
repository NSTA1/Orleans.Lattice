namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A sequenced WAL entry projected as the pre-encoded byte payload the
/// canonical <see cref="IWalRecordEncoder"/> wrote at append time.
/// Returned in bulk by <see cref="IWalShardGrain.ReadShippingAsync"/>
/// so the replication shipper can hand the bytes verbatim to the
/// outbound framing-only transport seam without paying a per-send
/// Orleans envelope serialize. The shipper recovers the typed
/// <see cref="WalRecord"/> for filter predicates by calling
/// <see cref="IWalRecordEncoder.Decode(System.ReadOnlySpan{byte})"/>
/// on <see cref="EncodedPayload"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalShardShippingEntry)]
[Immutable]
internal readonly record struct WalShardShippingEntry
{
    /// <summary>The per-shard, monotonically-increasing sequence number assigned at append time.</summary>
    [Id(0)] public long Sequence { get; init; }

    /// <summary>
    /// The pre-encoded payload bytes for this entry, byte-for-byte
    /// identical to whatever the canonical
    /// <see cref="IWalRecordEncoder"/> emitted when the entry was
    /// originally appended to the WAL. Borrowed for the duration of
    /// the surrounding grain call; consumers must not retain
    /// references past the call completion.
    /// </summary>
    [Id(1)] public byte[] EncodedPayload { get; init; }

    /// <summary>
    /// Compares two entries by value, with <see cref="EncodedPayload"/> compared
    /// by content. The compiler-generated record-struct equality compares the
    /// <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical entries built from
    /// independently allocated but byte-identical payloads - and, in particular,
    /// an entry and its post-serialization self - would otherwise never compare
    /// equal.
    /// </summary>
    /// <param name="other">The entry to compare against.</param>
    public bool Equals(WalShardShippingEntry other) =>
        Sequence == other.Sequence
        && BytesEqual(EncodedPayload, other.EncodedPayload);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Sequence);
        if (EncodedPayload is { } payload)
        {
            hash.AddBytes(payload);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}