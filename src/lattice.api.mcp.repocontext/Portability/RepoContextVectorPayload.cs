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
}
