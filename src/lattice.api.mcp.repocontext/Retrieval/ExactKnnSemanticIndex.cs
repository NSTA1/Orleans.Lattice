using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The shipped default <see cref="IRepoContextSemanticIndex"/>: a brute-force
/// exact k-nearest-neighbour search over the vectors a repository holds
/// authoritatively. It range-scans the <see cref="RepoContextTrees.VectorMetadata"/>
/// tree for the repository, hydrates the vectors' immutable payloads from the
/// <see cref="RepoContextTrees.VectorPayload"/> tree - one batched multi-get per
/// metadata page rather than a round-trip per vector, deduplicating the
/// content-addressed payloads a page shares - and ranks the candidates
/// with <see cref="RepoContextKnnRanker"/>. Because it enumerates the store of
/// record directly it always reflects the current live set with perfect recall -
/// the correct default at the local scale the repository-context surface targets.
/// <para>
/// The index is a <b>derived projection</b>: it holds no state of its own and is
/// safe to run against a store rebuilt from enumeration at any time. It is
/// fail-closed on embedding space - a stored vector whose space does not match the
/// query is skipped by the ranker before any distance is computed - so a
/// mixed-space store never produces a meaningless score. A host that needs
/// approximate search at larger scale binds its own implementation of the seam
/// instead; that engine is a separate future effort and is not built here.
/// </para>
/// </summary>
internal sealed class ExactKnnSemanticIndex : IRepoContextSemanticIndex
{
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;

    /// <summary>Creates the exact kNN index.</summary>
    /// <param name="grainFactory">The grain factory used to reach the reserved vector trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode vector records. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public ExactKnnSemanticIndex(IGrainFactory grainFactory, Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        _grainFactory = grainFactory;
        _serializer = serializer;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<RepoContextVectorMatch>> SearchAsync(
        string repoId,
        ReadOnlyMemory<float> query,
        EmbeddingSpaceTag querySpace,
        int k,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(k);

        var candidates = await GatherAsync(repoId, querySpace, cancellationToken).ConfigureAwait(false);
        return RepoContextKnnRanker.Rank(query, querySpace, candidates, k);
    }

    private async Task<List<RepoContextVectorCandidate>> GatherAsync(
        string repoId, EmbeddingSpaceTag querySpace, CancellationToken cancellationToken)
    {
        var metadataTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var payloadTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorPayload);
        var prefix = RepoContextKeys.VectorsPrefix(repoId);
        var candidates = new List<RepoContextVectorCandidate>();

        // A payload is content-addressed, so many vectors (chunks and symbols that
        // hash to identical bytes) can share one payload key. Decode each distinct
        // payload at most once across the whole scan and reuse the decoded array by
        // reference - the ranker only reads it.
        var decoded = new Dictionary<string, float[]>(StringComparer.Ordinal);

        string? token = null;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await RepoContextPortability
                .EnumerateAsync(metadataTree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);

            // Pass 1: decode the page's metadata, keep only vectors in the query's
            // embedding space, and record the payload key each survivor needs.
            var pending = new List<PendingVector>(page.Records.Count);
            List<string>? toFetch = null;
            foreach (var record in page.Records)
            {
                if (record.Value is null)
                {
                    continue;
                }

                var metadata = _serializer.Deserialize<VectorMetadataRecord>(record.Value);
                if (!VectorSpaceGuard.Matches(metadata.Space, querySpace))
                {
                    continue;
                }

                var contentAddress = RepoContextValues.ReadString(metadata.ContentAddress);
                if (contentAddress is null)
                {
                    continue;
                }

                var payloadKey = RepoContextKeys.VectorPayload(metadata.RepoId, contentAddress);
                var sourceKey = RepoContextValues.ReadString(metadata.SourceKey) ?? string.Empty;
                pending.Add(new PendingVector(metadata.VectorId, sourceKey, payloadKey, metadata.Space));

                if (!decoded.ContainsKey(payloadKey))
                {
                    (toFetch ??= new List<string>()).Add(payloadKey);
                }
            }

            // Pass 2: fetch every payload this page still needs in a single batched
            // fan-out to the payload shards, instead of one serial round-trip per
            // vector. Deduplicate keys so a payload shared within the page is asked
            // for once.
            if (toFetch is { Count: > 0 })
            {
                var distinct = toFetch.Count == 1 ? toFetch : toFetch.Distinct(StringComparer.Ordinal).ToList();
                var fetched = await payloadTree.GetManyAsync(distinct, cancellationToken).ConfigureAwait(false);
                foreach (var (payloadKey, payloadBytes) in fetched)
                {
                    var vector = DecodePayload(payloadBytes);
                    if (vector is not null)
                    {
                        decoded[payloadKey] = vector;
                    }
                }
            }

            // Pass 3: emit a candidate per surviving vector, reusing decoded payloads.
            // A vector whose payload could not be loaded is dropped, exactly as the
            // prior per-vector load did.
            foreach (var item in pending)
            {
                if (decoded.TryGetValue(item.PayloadKey, out var vector))
                {
                    candidates.Add(new RepoContextVectorCandidate(
                        item.VectorId, item.SourceKey, vector, item.Space));
                }
            }

            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);

        return candidates;
    }

    private float[]? DecodePayload(byte[] payloadBytes)
    {
        var payload = _serializer.Deserialize<VectorPayloadRecord>(payloadBytes);
        var encoded = FirstElement(payload.Payload);
        return encoded is null ? null : VectorCodec.Decode(encoded);
    }

    private readonly record struct PendingVector(
        string VectorId, string SourceKey, string PayloadKey, EmbeddingSpaceTag Space);

    private static byte[]? FirstElement(GSet payload)
    {
        foreach (var element in payload.Elements)
        {
            return Convert.FromBase64String(element);
        }

        return null;
    }
}
