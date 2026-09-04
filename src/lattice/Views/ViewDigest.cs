namespace Orleans.Lattice;

/// <summary>
/// A deterministic, order-independent content fingerprint of a materialised
/// view's live (key, value) entries, used to detect drift between a view and the
/// source state it is derived from (see <see cref="ILatticeView.ReconcileAsync"/>
/// and <see cref="ILatticeView.ComputeDigestAsync"/>).
/// <para>
/// The digest mirrors the <see cref="LeafProjectionDigest"/> algebra - every
/// entry's <c>(key, value)</c> tuple is hashed to a 16-byte XxHash128
/// contribution and the contributions are XOR-folded into a running hash; the
/// final <see cref="Hash"/> is the XxHash128 of <c>(running_xor || entryCount)</c>.
/// The XOR fold is commutative and self-inverse, so the digest is independent of
/// the order entries are scanned in. Unlike <see cref="LeafProjectionDigest"/> the
/// fold deliberately covers only the materialised content - key and value bytes -
/// and excludes the per-entry hybrid-logical-clock / origin / vector-clock
/// metadata, because a view re-derived from current source state re-stamps those
/// with fresh clocks: two logically identical views must compare equal even
/// though their entries were written at different wall-clock moments. Aggregation
/// views fold only their materialised group values, skipping the reserved
/// internal accumulator / inverse / membership rows.
/// </para>
/// <para>
/// The digest is a drift-detection fingerprint computed with a non-cryptographic
/// hash, not an authentication tag. It does not cover per-entry time-to-live, so
/// two views whose entries differ only in remaining TTL compare equal.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewDigest)]
[Immutable]
public readonly record struct ViewDigest
{
    /// <summary>The XxHash128 hash bytes (16 bytes) over the view's materialised content.</summary>
    [Id(0)] public byte[] Hash { get; init; }

    /// <summary>
    /// Number of live materialised entries folded into <see cref="Hash"/>
    /// (excluding any reserved aggregation internal rows). Reported alongside the
    /// hash so a mismatch can be triaged: a different entry count points to a
    /// missing or extra view entry, an identical count with a different hash
    /// points to a value difference.
    /// </summary>
    [Id(1)] public long EntryCount { get; init; }

    /// <summary>
    /// Whether this digest is byte-for-byte equal to <paramref name="other"/>:
    /// the same <see cref="EntryCount"/> and the same <see cref="Hash"/> bytes.
    /// This is the value-equality implementation that <see cref="Equals(ViewDigest)"/>
    /// and the <c>==</c> operator delegate to; it is retained as an explicitly
    /// named drift-check alias.
    /// </summary>
    /// <param name="other">The digest to compare against.</param>
    public bool ContentEquals(ViewDigest other) =>
        EntryCount == other.EntryCount
        && (Hash is null ? other.Hash is null : other.Hash is not null && Hash.AsSpan().SequenceEqual(other.Hash));

    /// <summary>
    /// Compares two digests by value. Overrides the compiler-generated
    /// record-struct equality, which compares the <see cref="Hash"/> array with
    /// <see cref="EqualityComparer{T}.Default"/> - reference equality - so two
    /// structurally identical digests (or a digest and its post-serialization
    /// self) would otherwise never compare equal, defeating the drift check this
    /// type exists for.
    /// </summary>
    /// <param name="other">The digest to compare against.</param>
    public bool Equals(ViewDigest other) => ContentEquals(other);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(EntryCount);
        if (Hash is { } bytes)
        {
            hash.AddBytes(bytes);
        }

        return hash.ToHashCode();
    }
}
