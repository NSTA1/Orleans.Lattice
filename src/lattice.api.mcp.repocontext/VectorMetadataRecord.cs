namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The metadata record for a single stored vector, held in the dedicated
/// <see cref="RepoContextTrees.VectorMetadata"/> tree at the key
/// <c>repo/{repoId}/vec/{vectorId}</c> (see
/// <see cref="RepoContextKeys.Vector(string, string)"/>). It binds a vector's
/// identity to its immutable embedding-space tag and to the store-of-record data
/// it was derived from, so any derived projection can rebuild the vector
/// deterministically and never be mistaken for the authoritative copy.
/// <para>
/// <b>Store of record.</b> The WAL-backed B+ tree that holds this record and the
/// matching <see cref="VectorPayloadRecord"/> is the sole authoritative store of
/// vectors and payloads. The in-box kNN scan, any future ANN index, and any
/// external vector service are <b>derived projections</b>: rebuilt from
/// enumeration of these trees, never an authoritative second copy, and safe to
/// discard and regenerate at any time.
/// </para>
/// <para>
/// <see cref="RepoId"/> and <see cref="VectorId"/> are immutable identity derived
/// from the key; <see cref="Space"/> is the immutable
/// <see cref="EmbeddingSpaceTag"/> stamped at write time and preserved across
/// merges. The projection scalars (<see cref="SourceKey"/>,
/// <see cref="ContentAddress"/>, <see cref="CreatedAt"/>) are last-writer-wins
/// registers and <see cref="Attributes"/> is an observed-remove map of additional
/// last-writer-wins scalars, so concurrent writers converge without loss. Merge
/// with <see cref="Merge(VectorMetadataRecord, VectorMetadataRecord)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.VectorMetadataRecord)]
internal sealed record VectorMetadataRecord
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>The per-repository vector identifier - immutable identity carried in the key.</summary>
    [Id(1)]
    public string VectorId { get; init; } = string.Empty;

    /// <summary>
    /// The immutable embedding-space tag stamped when the vector was written.
    /// Preserved unchanged across merges (recovered from the other replica only
    /// when this side is the never-stamped default).
    /// </summary>
    [Id(2)]
    public EmbeddingSpaceTag Space { get; init; }

    /// <summary>
    /// Last-writer-wins key of the store-of-record record this vector was derived
    /// from (for example a file or symbol key), so the vector can be regenerated
    /// deterministically from authoritative content.
    /// </summary>
    [Id(3)]
    public BoundedRegister SourceKey { get; init; } = new();

    /// <summary>
    /// Last-writer-wins content address of the immutable payload in the
    /// <see cref="RepoContextTrees.VectorPayload"/> tree that holds the vector
    /// components (see <see cref="VectorPayloadRecord"/>).
    /// </summary>
    [Id(4)]
    public BoundedRegister ContentAddress { get; init; } = new();

    /// <summary>Last-writer-wins creation timestamp (integer-encoded scalar, e.g. UTC ticks).</summary>
    [Id(5)]
    public BoundedRegister CreatedAt { get; init; } = new();

    /// <summary>
    /// Observed-remove map of additional last-writer-wins scalar attributes keyed
    /// by name (for example the source chunk ordinal or a passage/query role tag),
    /// so extra metadata can be added over time and still converge under
    /// concurrent writes.
    /// </summary>
    [Id(6)]
    public OrMap<string, BoundedRegister> Attributes { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same vector-metadata record. Identity
    /// and the immutable <see cref="Space"/> are preserved from
    /// <paramref name="left"/> (falling back to <paramref name="right"/> only when
    /// the left side is unset); every mutable field is folded through its CRDT
    /// join, so the result is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static VectorMetadataRecord Merge(VectorMetadataRecord left, VectorMetadataRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new VectorMetadataRecord
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            VectorId = left.VectorId.Length != 0 ? left.VectorId : right.VectorId,
            Space = left.Space.IsSpecified ? left.Space : right.Space,
            SourceKey = BoundedRegister.Merge(left.SourceKey, right.SourceKey),
            ContentAddress = BoundedRegister.Merge(left.ContentAddress, right.ContentAddress),
            CreatedAt = BoundedRegister.Merge(left.CreatedAt, right.CreatedAt),
            Attributes = OrMap<string, BoundedRegister>.Merge(left.Attributes, right.Attributes),
        };
    }
}
