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
/// live embedding under the presence key <c>{sourceId}.{unit}.{contentAddress}</c>
/// in the <see cref="RepoContextTrees.VectorMetadata"/> tree, where <c>unit</c> is
/// the zero-based ordinal of a passage within the source. A file is embedded as
/// several overlapping windows and a symbol as a single passage, so a source may
/// hold more than one live vector - one per unit - all grouped under the same
/// <c>{sourceId}.</c> prefix. On re-embed the writer stores the fresh set of units
/// and <b>deletes every prior presence key for the source that is not in that
/// set</b>, so a source that changed content, gained, or lost passages ends the
/// write holding exactly its current vectors and the superseded keys become
/// tree-level tombstones the compactor collects - never a single ever-growing
/// value. The immutable, content-addressed payload
/// lands once per content address in the <see cref="RepoContextTrees.VectorPayload"/>
/// tree (deduplicated and never rewritten), and each live source is recorded as its
/// own presence key <c>{sourceId}</c> in the
/// <see cref="RepoContextTrees.VectorMembership"/> tree - one tiny keyed row per
/// source rather than a single aggregate set - so a write touches only the sources
/// that changed and never reads or re-ships a growing whole-set value.
/// </para>
/// <para>
/// <b>Config-only multi-cluster.</b> All three vector trees are keyed per unit
/// (payload per content address, metadata per vector, membership per source), so
/// enabling cross-cluster replication is pure configuration: enrol the trees in the
/// replication package and each write ships only its own small keyed delta, never a
/// whole-set blob, letting one cluster compute the (expensive) embedding index once
/// and replicate it rather than every cluster re-deriving it. Membership presence is
/// <b>always</b> an enable-wins <see cref="OrFlag"/> - single-cluster and replicated
/// hosts share one on-disk format, so the layout never depends on whether replication
/// happens to be enabled and turning replication on needs no re-index. A source
/// embedded on one cluster and pruned on another therefore converges add-wins by CRDT
/// merge rather than by a re-embed, and the gap scanner stays a purely local heal for
/// interrupted runs, never load-bearing for cross-cluster convergence.
/// </para>
/// </summary>
internal sealed class RepoContextVectorWriter
{
    /// <summary>The dot-authoring replica id used when the host is not a replicated
    /// cluster and the replication seam reports no local replica id. A fixed value is
    /// safe because a single-cluster host is a single logical writer; once replication
    /// is enabled the seam supplies a real, per-cluster id and the union-based flag
    /// merge folds the two authors together with no migration.</summary>
    private const string LocalReplicaFallback = "local";

    /// <summary>
    /// The width, in digits, of the zero-padded unit ordinal embedded in a
    /// presence key. Fixed-width so the lexical key order matches the numeric unit
    /// order, and wide enough for the chunker's per-file cap.
    /// </summary>
    internal const int UnitDigits = 4;

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;
    private readonly ILatticeReplicationContext _replication;
    private readonly RepoContextVectorCache _cache;

    /// <summary>Creates the vector writer.</summary>
    /// <param name="grainFactory">The grain factory used to reach the reserved vector trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode and re-encode vector records. Must not be <see langword="null"/>.</param>
    /// <param name="replication">The replication context that reports whether, and in what merge mode, the membership tree is replicated. Must not be <see langword="null"/>.</param>
    /// <param name="cache">The warm decoded-candidate cache invalidated after every local mutation. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextVectorWriter(
        IGrainFactory grainFactory,
        Serializer serializer,
        ILatticeReplicationContext replication,
        RepoContextVectorCache cache)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(replication);
        ArgumentNullException.ThrowIfNull(cache);
        _grainFactory = grainFactory;
        _serializer = serializer;
        _replication = replication;
        _cache = cache;
    }

    /// <summary>
    /// Stores <paramref name="vectors"/> as the current embedding of
    /// <paramref name="sourceKey"/> - one vector per passage (a file's overlapping
    /// windows, or a symbol's single passage) - retiring any prior embedding of the
    /// same source. Each payload is content-addressed and written at most once; a
    /// metadata presence key is created per unit; every stale presence key for the
    /// source (a superseded content address or a unit the source no longer has) is
    /// deleted; and the caller records the source identifier in the bounded
    /// membership set once its vectors have landed.
    /// </summary>
    /// <param name="repoId">The repository the vectors belong to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKey">The canonical record key the vectors were derived from. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space the vectors were produced in. Must not be <see langword="null"/>.</param>
    /// <param name="vectors">The ordered passage vectors to store. An empty list retires the source's entire live embedding.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/>,
    /// <paramref name="sourceKey"/>, <paramref name="space"/>, or
    /// <paramref name="vectors"/> is null.</exception>
    public async Task StoreAsync(
        string repoId,
        string sourceKey,
        EmbeddingSpace space,
        IReadOnlyList<ReadOnlyMemory<float>> vectors,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(sourceKey);
        ArgumentNullException.ThrowIfNull(space);
        ArgumentNullException.ThrowIfNull(vectors);

        var tag = EmbeddingSpaceTag.FromSpace(space);
        var sourceId = VectorCodec.SourceId(sourceKey);

        var keep = new HashSet<string>(StringComparer.Ordinal);
        for (var unit = 0; unit < vectors.Count; unit++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var payload = VectorCodec.Encode(vectors[unit]);
            var contentAddress = VectorCodec.ContentAddress(payload);
            var vectorId = FormatVectorId(sourceId, unit, contentAddress);
            keep.Add(vectorId);

            await WritePayloadAsync(repoId, contentAddress, tag, payload, cancellationToken).ConfigureAwait(false);
            await WriteMetadataAsync(repoId, vectorId, sourceKey, contentAddress, tag, cancellationToken)
                .ConfigureAwait(false);
        }

        await RetireStaleAsync(repoId, sourceId, keep, cancellationToken).ConfigureAwait(false);

        // The metadata/payload trees the exact-kNN gather scans just changed for this
        // repository, so drop any warm cached candidate set precisely and immediately.
        _cache.Invalidate(repoId);

        // Membership is recorded by the caller once per embed batch (see
        // AddMembersAsync), not per store: a batch's presence keys land in a single
        // bulk write after its vectors, so an interrupted run leaves at most one
        // batch of vectors unrecorded (re-embedded, idempotently, on the next pass).
    }

    /// <summary>
    /// Formats the per-repository vector identifier for a passage: the source
    /// identifier, the zero-padded unit ordinal, and the content address, joined so
    /// every unit of a source shares the <c>{sourceId}.</c> prefix a range delete
    /// retires in one call.
    /// </summary>
    /// <param name="sourceId">The stable source identifier. Must not be <see langword="null"/>.</param>
    /// <param name="unit">The zero-based passage ordinal within the source. Must be non-negative.</param>
    /// <param name="contentAddress">The payload content address. Must not be <see langword="null"/>.</param>
    /// <returns>The vector identifier <c>{sourceId}.{unit}.{contentAddress}</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sourceId"/> or <paramref name="contentAddress"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="unit"/> is negative.</exception>
    internal static string FormatVectorId(string sourceId, int unit, string contentAddress)
    {
        ArgumentNullException.ThrowIfNull(sourceId);
        ArgumentNullException.ThrowIfNull(contentAddress);
        ArgumentOutOfRangeException.ThrowIfNegative(unit);
        return $"{sourceId}.{unit.ToString($"D{UnitDigits}", System.Globalization.CultureInfo.InvariantCulture)}.{contentAddress}";
    }

    /// <summary>
    /// Retires the entire live embedding of <paramref name="sourceKey"/>: every
    /// metadata presence key for the source is deleted and its membership presence
    /// key is removed (deleted in last-writer-wins mode, disabled in flag mode). Used
    /// when the source itself is removed (its file was pruned), so a deleted file
    /// naturally drops its vector and the membership tree stays an honest tally of
    /// live embeddings. The immutable, content-addressed payload is left for the
    /// per-tree compactor to reclaim, since it may be shared by another source with
    /// identical content. Idempotent: retiring a source with no live vector is a no-op.
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

        // The metadata tree the exact-kNN gather scans just lost this source's
        // vectors, so drop any warm cached candidate set precisely and immediately.
        _cache.Invalidate(repoId);
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
        string repoId, string sourceId, IReadOnlySet<string> keep, CancellationToken cancellationToken)
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
                    && parsed.VectorId is not null
                    && !keep.Contains(parsed.VectorId))
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
    /// Records embedded presence for a whole batch of sources, one enable-wins flag
    /// per source. Each flag lives at key <c>{sourceId}</c> under the repository's
    /// membership range and carries only the 16-character source identifier derived
    /// from <paramref name="sourceKeys"/>, never the embedding itself. Presence is
    /// always an <see cref="OrFlag"/> so it converges add-wins under concurrent
    /// active-active enable/disable across clusters - a source embedded on one cluster
    /// and pruned on another survives by CRDT merge, never by a re-embed - and so the
    /// on-disk format never depends on whether replication happens to be enabled.
    /// Re-adding a source that is already present is idempotent.
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
        var replicaId = ReplicaId();
        foreach (var sourceKey in sourceKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = RepoContextKeys.VectorMembership(repoId, VectorCodec.SourceId(sourceKey));
            await tree.OrFlag(key).EnableAsync(replicaId, cancellationToken).ConfigureAwait(false);
        }

        // Membership does not feed the gather, but a batch's membership write always
        // trails its StoreAsync vectors, so invalidate defensively to keep the cache
        // consistent with any future gather that consults membership.
        _cache.Invalidate(repoId);
    }

    private async Task RemoveMemberAsync(string repoId, string sourceId, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var key = RepoContextKeys.VectorMembership(repoId, sourceId);

        // Disable rather than delete so the removal carries causal history and
        // converges add-wins against a concurrent enable on another cluster.
        await tree.OrFlag(key).DisableAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads the set of embedded source identifiers for <paramref name="repoId"/> so
    /// a caller can probe presence with <see cref="IReadOnlySet{T}.Contains(string)"/>.
    /// Presence is an enable-wins flag, so the read decodes each row and keeps only the
    /// enabled ones (a disabled flag still occupies a key until the compactor reclaims
    /// it). The membership tree carries only 16-character source identifiers, never the
    /// embeddings themselves. Returns an empty set when the repository has embedded
    /// nothing yet.
    /// </summary>
    /// <param name="repoId">The repository whose embedded source identifiers to load. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The set of live embedded source identifiers.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public async Task<IReadOnlySet<string>> LoadEmbeddedMembersAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var prefix = RepoContextKeys.VectorMembershipsPrefix(repoId);
        var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);
        var members = new HashSet<string>(StringComparer.Ordinal);
        if (endExclusive is null)
        {
            return members;
        }

        await foreach (var entry in tree
            .ScanEntriesAsync(prefix, endExclusive, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            if (JsonLatticeSerializer<OrFlag>.Default.Deserialize(entry.Value).IsEnabled
                && TryReadSourceId(entry.Key, out var sourceId))
            {
                members.Add(sourceId);
            }
        }

        return members;
    }

    /// <summary>
    /// Counts the live embedded sources for <paramref name="repoId"/> - the number of
    /// enabled membership flags. A disabled flag still occupies a key until the
    /// compactor reclaims it, so the count decodes each row rather than counting keys.
    /// Returns zero when nothing is embedded.
    /// </summary>
    /// <param name="repoId">The repository whose embedded source count to read. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The number of live embedded sources.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public async Task<long> CountEmbeddedAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var prefix = RepoContextKeys.VectorMembershipsPrefix(repoId);
        var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);
        if (endExclusive is null)
        {
            return 0L;
        }

        var enabled = 0L;
        await foreach (var entry in tree
            .ScanEntriesAsync(prefix, endExclusive, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            if (JsonLatticeSerializer<OrFlag>.Default.Deserialize(entry.Value).IsEnabled)
            {
                enabled++;
            }
        }

        return enabled;
    }

    /// <summary>
    /// The dot-authoring replica identity for a membership flag enable. When the host
    /// runs a replicated cluster this is the seam's <see cref="ILatticeReplicationContext.LocalReplicaId"/>
    /// (a distinct id per cluster, so concurrent enables carry distinct dots and merge
    /// add-wins); on a single-cluster host, where the seam reports no id, it falls back
    /// to a fixed local author. Because merge is a union of dots, the author can change
    /// over a repository's lifetime (single-cluster local id, then a real cluster id
    /// once replication is enabled) with no migration and no format change.
    /// </summary>
    private string ReplicaId()
    {
        var replicaId = _replication.LocalReplicaId;
        return string.IsNullOrEmpty(replicaId) ? LocalReplicaFallback : replicaId;
    }

    private static bool TryReadSourceId(string membershipKey, out string sourceId)
    {
        if (RepoContextKeys.TryParse(membershipKey, out var parsed)
            && parsed.Kind == RepoContextRecordKind.VectorMembership
            && parsed.Collection is { Length: > 0 } collection)
        {
            sourceId = collection;
            return true;
        }

        sourceId = string.Empty;
        return false;
    }
}
