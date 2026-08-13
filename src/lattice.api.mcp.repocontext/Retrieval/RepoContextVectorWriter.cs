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

        // Membership is recorded by the caller once per embed batch (see
        // AddMembersAsync), not per store: folding presence for every file would
        // read-modify-write the whole growing membership record thousands of times
        // over a large back-fill. The caller flushes a batch's membership after its
        // vectors land, so an interrupted run leaves at most one batch of vectors
        // unrecorded (re-embedded, idempotently, on the next pass).
        return vectorId;
    }

    /// <summary>
    /// Retires the entire live embedding of <paramref name="sourceKey"/>: every
    /// metadata presence key for the source is deleted and its identifier is
    /// observed-removed from the membership set. Used when the source itself is
    /// removed (its file was pruned), so a deleted file naturally drops its vector
    /// and the membership set stays an honest tally of live embeddings. The
    /// immutable, content-addressed payload is left for the per-tree compactor to
    /// reclaim, since it may be shared by another source with identical content.
    /// Idempotent: retiring a source with no live vector is a no-op.
    /// </summary>
    /// <param name="repoId">The repository the source belongs to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKey">The canonical record key whose embedding to retire. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the retirement.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="sourceKey"/> is null.</exception>
    public async Task RetireAsync(string repoId, string sourceKey, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(sourceKey);

        var sourceId = VectorCodec.SourceId(sourceKey);

        await DeleteVectorsAsync(repoId, sourceId, cancellationToken).ConfigureAwait(false);
        await RemoveMemberAsync(repoId, sourceId, cancellationToken).ConfigureAwait(false);
    }

    private async Task DeleteVectorsAsync(string repoId, string sourceId, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);

        // Every presence key for the source shares the prefix "{sourceId}." within
        // the repository's vector range, so a single range delete retires the whole
        // source in one call - no prefix scan and no per-key point deletes.
        var prefix = $"{RepoContextKeys.VectorsPrefix(repoId)}{sourceId}.";
        var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);
        if (endExclusive is null)
        {
            return;
        }

        await tree.DeleteRangeAsync(prefix, endExclusive, cancellationToken).ConfigureAwait(false);
    }
    private async Task WritePayloadAsync(
        string repoId, string contentAddress, EmbeddingSpaceTag tag, byte[] payload, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorPayload);
        var key = RepoContextKeys.VectorPayload(repoId, contentAddress);

        // Immutable and content-addressed: a present payload is byte-identical,
        // so a presence probe is enough. ExistsAsync avoids transferring the
        // (multi-kilobyte) embedding back across the grain boundary just to
        // discover it is already stored.
        if (await tree.ExistsAsync(key, cancellationToken).ConfigureAwait(false))
        {
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

    /// <summary>
    /// Records embedded presence for a whole batch of sources in a single
    /// membership read-modify-write. The membership set holds only the 16-character
    /// source identifiers derived from <paramref name="sourceKeys"/>, never the
    /// embeddings themselves. Folding presence per source would re-read and rewrite
    /// the whole growing membership record once per file - thousands of cycles over
    /// a large back-fill - so a caller stores a batch's vectors and then calls this
    /// once to fold all of that batch's identifiers in with one read and, at most,
    /// one write. Re-adding an identifier that is already a live member is a no-op,
    /// and a batch whose sources are all already present writes nothing.
    /// </summary>
    /// <param name="repoId">The repository the sources belong to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKeys">The canonical record keys whose embeddings just landed. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="sourceKeys"/> is null.</exception>
    public async Task AddMembersAsync(
        string repoId, IReadOnlyList<string> sourceKeys, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(sourceKeys);

        if (sourceKeys.Count == 0)
        {
            return;
        }

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var key = RepoContextKeys.VectorMembership(repoId, SourceCollection);

        var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        var record = existing is null
            ? new VectorMembershipRecord { RepoId = repoId, Collection = SourceCollection }
            : _serializer.Deserialize<VectorMembershipRecord>(existing);

        var added = false;
        foreach (var sourceKey in sourceKeys)
        {
            var element = System.Text.Encoding.UTF8.GetBytes(VectorCodec.SourceId(sourceKey));
            if (record.Members.Contains(element))
            {
                // A stable identifier: a re-embed re-adds the same member, so the
                // set is already correct for this source and needs no change.
                continue;
            }

            record.Members.Add(element, Guid.NewGuid().ToString("N"), 0L);
            added = true;
        }

        if (!added)
        {
            // Every source in the batch was already a live member (an idempotent
            // re-run), so the set is unchanged and needs no rewrite.
            return;
        }

        await tree.SetAsync(key, _serializer.SerializeToArray(record), cancellationToken).ConfigureAwait(false);
    }

    private async Task RemoveMemberAsync(string repoId, string sourceId, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var key = RepoContextKeys.VectorMembership(repoId, SourceCollection);

        var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (existing is null)
        {
            // No membership record yet: nothing was ever embedded for this repo.
            return;
        }

        var record = _serializer.Deserialize<VectorMembershipRecord>(existing);
        var element = System.Text.Encoding.UTF8.GetBytes(sourceId);
        if (!record.Members.Remove(element))
        {
            // The source was not a live member (never embedded, or already
            // retired), so the set is already correct and needs no rewrite.
            return;
        }

        await tree.SetAsync(key, _serializer.SerializeToArray(record), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads the set of embedded source identifiers for <paramref name="repoId"/>
    /// as the raw add-wins membership set, so a caller can probe presence with
    /// <see cref="OrSet.Contains(byte[])"/> without materialising a separate
    /// collection. The membership record carries only 16-character source
    /// identifiers - never the embeddings themselves - so this read never
    /// transfers a vector payload across the grain boundary; it is a single read
    /// per repository, not one per source. Returns an empty set when the
    /// repository has embedded nothing yet.
    /// </summary>
    /// <param name="repoId">The repository whose embedded source identifiers to load. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The live membership set of embedded source identifiers.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public async Task<OrSet> LoadEmbeddedMembersAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var key = RepoContextKeys.VectorMembership(repoId, SourceCollection);

        var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return existing is null
            ? new OrSet()
            : _serializer.Deserialize<VectorMembershipRecord>(existing).Members;
    }
}
