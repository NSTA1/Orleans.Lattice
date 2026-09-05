namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One portable unit of the repository-context store: a single enumerated
/// <c>(key, value, optional vector, optional embedding-space tag)</c> tuple,
/// captured by the portability primitive and serialized into a snapshot stream.
/// <para>
/// The type is deliberately generic over the payload: <see cref="Value"/> is the
/// opaque Orleans-serialized CRDT record bytes exactly as they sit in the store,
/// and <see cref="Vector"/> / <see cref="EmbeddingSpace"/> are an optional,
/// opaque vector payload carried alongside the record. The portability primitive
/// never inspects or interprets these bytes, so a snapshot round-trips a record
/// whose concrete shape is owned by another package (the vector record and its
/// embedding-space tag land in a sibling issue) without a compile-time
/// dependency on that shape.
/// </para>
/// <para>
/// Being <c>[GenerateSerializer]</c> with a stable <c>[Alias]</c>, the record
/// reuses the repository's wire-format stability guarantee: a snapshot written by
/// one host is readable by another regardless of the durability profile that
/// produced it.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.SnapshotRecord)]
[Immutable]
internal sealed record RepoContextSnapshotRecord
{
    /// <summary>The store key this record was enumerated under.</summary>
    [Id(0)]
    public string Key { get; init; } = string.Empty;

    /// <summary>
    /// The opaque, Orleans-serialized CRDT value bytes exactly as stored under
    /// <see cref="Key"/>. Never interpreted by the portability primitive.
    /// </summary>
    [Id(1)]
    public byte[] Value { get; init; } = [];

    /// <summary>
    /// The optional, opaque vector payload associated with <see cref="Key"/>, or
    /// <see langword="null"/> when the record carries no vector. Treated as opaque
    /// bytes so the primitive stays independent of the concrete vector record.
    /// </summary>
    [Id(2)]
    public byte[]? Vector { get; init; }

    /// <summary>
    /// The optional embedding-space tag naming the space
    /// <see cref="Vector"/> lives in, or <see langword="null"/> when there is no
    /// vector. An opaque string that the primitive round-trips verbatim.
    /// </summary>
    [Id(3)]
    public string? EmbeddingSpace { get; init; }

    /// <summary>
    /// Compares two records by value, with <see cref="Value"/> and
    /// <see cref="Vector"/> compared by content. The compiler-generated record
    /// equality compares the <see cref="byte"/> arrays with
    /// <see cref="EqualityComparer{T}.Default"/> (reference equality), so two
    /// structurally identical records - and, in particular, a record and its
    /// post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The record to compare against.</param>
    public bool Equals(RepoContextSnapshotRecord? other) =>
        other is not null
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && string.Equals(EmbeddingSpace, other.EmbeddingSpace, StringComparison.Ordinal)
        && BytesEqual(Value, other.Value)
        && BytesEqual(Vector, other.Vector);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Key, StringComparer.Ordinal);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        if (Vector is { } vector)
        {
            hash.AddBytes(vector);
        }

        hash.Add(EmbeddingSpace, StringComparer.Ordinal);
        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
