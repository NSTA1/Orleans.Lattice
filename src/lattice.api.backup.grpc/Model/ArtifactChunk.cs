namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// One ordered chunk of an artifact's bytes, streamed by the server-streaming
/// export-artifact RPC. The facade yields the artifact as a sequence of
/// <see cref="ReadOnlyMemory{T}"/> segments; each is carried on the wire as a
/// single <see cref="Data"/> chunk so a large artifact streams with bounded
/// memory instead of being materialized whole.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.ArtifactChunk)]
[Immutable]
public sealed record ArtifactChunk
{
    /// <summary>The chunk's bytes, in artifact order.</summary>
    [Id(0)] public required byte[] Data { get; init; }

    /// <summary>
    /// Compares two chunks by value, with <see cref="Data"/> compared by content.
    /// The compiler-generated record equality compares the <see cref="byte"/>
    /// array with <see cref="EqualityComparer{T}.Default"/> (reference equality),
    /// so two structurally identical chunks - and, in particular, a chunk and its
    /// post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The chunk to compare against.</param>
    public bool Equals(ArtifactChunk? other) =>
        other is not null
        && BytesEqual(Data, other.Data);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Data is { } data)
        {
            hash.AddBytes(data);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
