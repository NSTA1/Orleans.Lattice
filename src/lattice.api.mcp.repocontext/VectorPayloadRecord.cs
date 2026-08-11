namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The immutable, content-addressed payload record for a stored vector, held in
/// the dedicated <see cref="RepoContextTrees.VectorPayload"/> tree at the key
/// <c>repo/{repoId}/vpay/{contentAddress}</c> (see
/// <see cref="RepoContextKeys.VectorPayload(string, string)"/>). The key is the
/// content address of the payload, so there is exactly one payload per key and it
/// is never rewritten.
/// <para>
/// <b>Content-addressed and grow-only.</b> The vector components are carried in a
/// grow-only <see cref="GSet"/> so a merge is the set union of byte-identical
/// payloads - idempotent and convergent by construction. Because the key is the
/// content address, a rebuild of any derived projection re-derives the same
/// payload under the same key, which is why the store of record is the sole
/// authority and every external index or cloud copy is a discardable, regenerable
/// projection rather than an authoritative second copy.
/// </para>
/// <para>
/// <see cref="RepoId"/> and <see cref="ContentAddress"/> are immutable identity
/// derived from the key; <see cref="Space"/> is the immutable
/// <see cref="EmbeddingSpaceTag"/> stamped at write time. Merge with
/// <see cref="Merge(VectorPayloadRecord, VectorPayloadRecord)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.VectorPayloadRecord)]
internal sealed record VectorPayloadRecord
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>
    /// The content address of the payload - immutable identity carried in the key,
    /// so one payload maps to exactly one key.
    /// </summary>
    [Id(1)]
    public string ContentAddress { get; init; } = string.Empty;

    /// <summary>
    /// The immutable embedding-space tag stamped when the payload was written.
    /// Preserved unchanged across merges (recovered from the other replica only
    /// when this side is the never-stamped default).
    /// </summary>
    [Id(2)]
    public EmbeddingSpaceTag Space { get; init; }

    /// <summary>
    /// Grow-only, content-addressed set carrying the immutable payload bytes (the
    /// serialized vector components). Because the containing key is the content
    /// address, this holds a single byte-identical element across replicas and its
    /// union merge is idempotent; the grow-only shape guarantees a payload observed
    /// by any replica is never lost.
    /// </summary>
    [Id(3)]
    public GSet Payload { get; init; } = new();

    /// <summary>
    /// Creates a payload record carrying <paramref name="payload"/> under the given
    /// content-addressed identity and embedding-space tag.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="contentAddress">The content address that keys the payload. Must not be <see langword="null"/>.</param>
    /// <param name="space">The immutable embedding-space tag to stamp.</param>
    /// <param name="payload">The immutable payload bytes. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public static VectorPayloadRecord Create(
        string repoId, string contentAddress, EmbeddingSpaceTag space, byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(contentAddress);
        ArgumentNullException.ThrowIfNull(payload);
        var content = new GSet();
        content.Add(payload);
        return new VectorPayloadRecord
        {
            RepoId = repoId,
            ContentAddress = contentAddress,
            Space = space,
            Payload = content,
        };
    }

    /// <summary>
    /// Lattice merge of two replicas of the same payload record. Identity and the
    /// immutable <see cref="Space"/> are preserved from <paramref name="left"/>
    /// (falling back to <paramref name="right"/> only when the left side is unset);
    /// <see cref="Payload"/> is folded through its grow-only union, so the result
    /// is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static VectorPayloadRecord Merge(VectorPayloadRecord left, VectorPayloadRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new VectorPayloadRecord
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            ContentAddress = left.ContentAddress.Length != 0 ? left.ContentAddress : right.ContentAddress,
            Space = left.Space.IsSpecified ? left.Space : right.Space,
            Payload = GSet.Merge(left.Payload, right.Payload),
        };
    }
}
