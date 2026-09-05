namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// An opaque vector payload the portability primitive carries alongside a record
/// without interpreting it: the raw <see cref="Vector"/> bytes and the optional
/// <see cref="EmbeddingSpace"/> tag naming the space they live in.
/// <para>
/// The concrete vector record and its space-tag semantics are owned by a sibling
/// package; this value type keeps the enumeration and snapshot surface generic
/// over "an optional vector" so it can be built and tested independently of that
/// shape.
/// </para>
/// </summary>
internal readonly record struct RepoContextVectorPayload
{
    /// <summary>Creates a payload from raw vector bytes and an optional space tag.</summary>
    /// <param name="vector">The opaque vector bytes. Must not be <see langword="null"/>.</param>
    /// <param name="embeddingSpace">The optional embedding-space tag, or <see langword="null"/>.</param>
    public RepoContextVectorPayload(byte[] vector, string? embeddingSpace)
    {
        ArgumentNullException.ThrowIfNull(vector);
        Vector = vector;
        EmbeddingSpace = embeddingSpace;
    }

    /// <summary>The opaque vector bytes.</summary>
    public byte[] Vector { get; }

    /// <summary>The optional embedding-space tag, or <see langword="null"/>.</summary>
    public string? EmbeddingSpace { get; }

    /// <summary>
    /// Compares two payloads by value, with <see cref="Vector"/> compared by
    /// content. The compiler-generated record-struct equality compares
    /// <see cref="Vector"/> with <see cref="EqualityComparer{T}.Default"/>, which
    /// for a <see cref="byte"/> array is reference equality, so two payloads
    /// carrying structurally identical vector bytes would otherwise never compare
    /// equal - mirroring the sibling <see cref="RepoContextSnapshotRecord"/>, whose
    /// opaque byte payloads are likewise compared by content.
    /// </summary>
    /// <param name="other">The payload to compare against.</param>
    public bool Equals(RepoContextVectorPayload other) =>
        string.Equals(EmbeddingSpace, other.EmbeddingSpace, StringComparison.Ordinal)
        && BytesEqual(Vector, other.Vector);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
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
