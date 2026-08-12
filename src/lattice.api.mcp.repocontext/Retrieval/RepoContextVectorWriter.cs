using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Persists a source's embedding onto the reserved vector trees using a
/// content-addressed presence-key layout, so a re-embed under real churn keeps
/// the <b>live</b> footprint bounded and lets the per-tree compactor reclaim the
/// space of a retired vector.
/// <para>
/// For each source (a file or symbol key) the writer stamps a stable
/// <see cref="VectorCodec.SourceId(string)"/> and stores one metadata record per
/// live embedding under the presence key <c>{sourceId}.{contentAddress}</c> in
/// the <see cref="RepoContextTrees.VectorMetadata"/> tree. On re-embed, the new
/// content address yields a new presence key and every prior presence key for the
/// same source is <b>deleted</b>, so the source has exactly one live vector at a
/// time and the deletions become tree-level tombstones the compactor collects -
/// never a single ever-growing value. The immutable, content-addressed payload
/// lands once per content address in the <see cref="RepoContextTrees.VectorPayload"/>
/// tree (deduplicated and never rewritten), and the low-churn set of live source
/// identifiers is folded into a bounded add-wins <see cref="VectorMembershipRecord"/>
/// in the <see cref="RepoContextTrees.VectorMembership"/> tree.
/// </para>
/// </summary>
internal sealed class RepoContextVectorWriter
{
    /// <summary>The single membership collection every source identifier is a member of.</summary>
    internal const string SourceCollection = "sources";

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;

    /// <summary>Creates the vector writer.</summary>
    /// <param name="grainFactory">The grain factory used to reach the reserved vector trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode and re-encode vector records. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextVectorWriter(IGrainFactory grainFactory, Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        _grainFactory = grainFactory;
        _serializer = serializer;
    }

    /// <summary>
    /// Stores <paramref name="vector"/> as the current embedding of
    /// <paramref name="sourceKey"/>, retiring any prior embedding of the same
    /// source. The payload is content-addressed and written at most once; the
    /// metadata presence key is created; stale presence keys for the source are
    /// deleted; and the source identifier is added to the bounded membership set.
    /// </summary>
    /// <param name="repoId">The repository the vector belongs to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKey">The canonical record key the vector was derived from. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space the vector was produced in. Must not be <see langword="null"/>.</param>
    /// <param name="vector">The vector components to store.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The per-repository vector identifier the embedding was stored under.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/>,
    /// <paramref name="sourceKey"/>, or <paramref name="space"/> is null.</exception>
    public async Task<string> StoreAsync(
        string repoId,
        string sourceKey,
        EmbeddingSpace space,
        ReadOnlyMemory<float> vector,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(sourceKey);
        ArgumentNullException.ThrowIfNull(space);

        var tag = EmbeddingSpaceTag.FromSpace(space);
        var payload = VectorCodec.Encode(vector);
        var contentAddress = VectorCodec.ContentAddress(payload);
        var sourceId = VectorCodec.SourceId(sourceKey);
        var vectorId = $"{sourceId}.{contentAddress}";

        await WritePayloadAsync(repoId, contentAddress, tag, payload, cancellationToken).ConfigureAwait(false);
        await WriteMetadataAsync(repoId, vectorId, sourceKey, contentAddress, tag, cancellationToken)
            .ConfigureAwait(false);
        await RetireStaleAsync(repoId, sourceId, keep: vectorId, cancellationToken).ConfigureAwait(false);
        await AddMemberAsync(repoId, sourceId, cancellationToken).ConfigureAwait(false);

        return vectorId;
    }

    private async Task WritePayloadAsync(
        string repoId, string contentAddress, EmbeddingSpaceTag tag, byte[] payload, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorPayload);
        var key = RepoContextKeys.VectorPayload(repoId, contentAddress);

        var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            // Immutable and content-addressed: the payload already present is
            // byte-identical, so there is nothing to rewrite.
            return;
        }

        var record = VectorPayloadRecord.Create(repoId, contentAddress, tag, payload);
        await tree.SetAsync(key, _serializer.SerializeToArray(record), cancellationToken).ConfigureAwait(false);
    }

    private async Task WriteMetadataAsync(
        string repoId,
        string vectorId,
        string sourceKey,
        string contentAddress,
        EmbeddingSpaceTag tag,
        CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var key = RepoContextKeys.Vector(repoId, vectorId);
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        var record = new VectorMetadataRecord
        {
            RepoId = repoId,
            VectorId = vectorId,
            Space = tag,
            SourceKey = RepoContextValues.Lww(sourceKey, clock),
            ContentAddress = RepoContextValues.Lww(contentAddress, clock),
            CreatedAt = RepoContextValues.Lww(DateTime.UtcNow.Ticks, clock),
        };

        var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        var merged = existing is null
            ? record
            : VectorMetadataRecord.Merge(record, _serializer.Deserialize<VectorMetadataRecord>(existing));
        await tree.SetAsync(key, _serializer.SerializeToArray(merged), cancellationToken).ConfigureAwait(false);
    }

    private async Task RetireStaleAsync(
        string repoId, string sourceId, string keep, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var prefix = $"{RepoContextKeys.VectorsPrefix(repoId)}{sourceId}.";

        string? token = null;
        var stale = new List<string>();
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await RepoContextPortability
                .EnumerateAsync(tree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);

            foreach (var record in page.Records)
            {
                if (RepoContextKeys.TryParse(record.Key, out var parsed)
                    && parsed.Kind == RepoContextRecordKind.VectorMetadata
                    && !string.Equals(parsed.VectorId, keep, StringComparison.Ordinal))
                {
                    stale.Add(record.Key);
                }
            }

            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);

        foreach (var key in stale)
        {
            await tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task AddMemberAsync(string repoId, string sourceId, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var key = RepoContextKeys.VectorMembership(repoId, SourceCollection);

        var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        var record = existing is null
            ? new VectorMembershipRecord { RepoId = repoId, Collection = SourceCollection }
            : _serializer.Deserialize<VectorMembershipRecord>(existing);

        var element = System.Text.Encoding.UTF8.GetBytes(sourceId);
        if (record.Members.Contains(element))
        {
            // The source is a stable identifier, so a re-embed re-adds the same
            // member: the set is already correct and needs no rewrite.
            return;
        }

        record.Members.Add(element, Guid.NewGuid().ToString("N"), 0L);
        await tree.SetAsync(key, _serializer.SerializeToArray(record), cancellationToken).ConfigureAwait(false);
    }
}
