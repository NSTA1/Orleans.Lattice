using System.Runtime.CompilerServices;
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
    /// The reserved prefix that distinguishes a "considered, no passages" marker
    /// from a real embedded-source flag inside the shared membership tree. A marker
    /// records that a structural file was read and found to carry no embeddable
    /// content (an empty or whitespace-only file, or one that chunks to zero
    /// passages), so the always-on gap sweep and the unchanged-file selection stop
    /// classifying it as a missing embedding and re-driving the index forever.
    /// Because a source identifier is always 16 lower-case hex characters (see
    /// <see cref="VectorCodec.SourceId(string)"/>), a non-hex prefix can never
    /// collide with one, and the distinction is read from the decoded collection so
    /// the on-key encoding is irrelevant. A marker carries no vector and is excluded
    /// from <see cref="CountEmbeddedAsync"/>, so <c>embeddedVectorCount</c> stays an
    /// honest count of sources that actually have a landed embedding.
    /// </summary>
    internal const string ContentlessMarkerPrefix = "nil-";

    /// <summary>
    /// The width, in digits, of the zero-padded unit ordinal embedded in a
    /// presence key. Fixed-width so the lexical key order matches the numeric unit
    /// order, and wide enough for the chunker's per-file cap.
    /// </summary>
    internal const int UnitDigits = 4;

    /// <summary>
    /// The maximum number of distinct source identifiers folded into a single
    /// bounded <see cref="ILattice.GetManyAsync(System.Collections.Generic.List{string}, CancellationToken)"/>
    /// point-probe. Chosen to match the paged-scan page size so a probe RPC touches
    /// a comparable, bounded key count and can never approach the response deadline,
    /// no matter how large or churn-bloated the membership tree has grown.
    /// </summary>
    private const int MembershipProbeBatchSize = 256;

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;
    private readonly ILatticeReplicationContext _replication;
    private readonly RepoContextVectorCache _cache;
    private readonly RepoContextVectorPlaneReDeriver _reDeriver;
    private readonly IRepoContextAnnIndex? _annIndex;

    /// <summary>Creates the vector writer.</summary>
    /// <param name="grainFactory">The grain factory used to reach the reserved vector trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode and re-encode vector records. Must not be <see langword="null"/>.</param>
    /// <param name="replication">The replication context that reports whether, and in what merge mode, the membership tree is replicated. Must not be <see langword="null"/>.</param>
    /// <param name="cache">The warm decoded-candidate cache invalidated after every local mutation. Must not be <see langword="null"/>.</param>
    /// <param name="reDeriver">The vector-plane self-healer that detects, meters, and re-derives a rebuildable vector tree that fell terminally off its write-ahead log. Must not be <see langword="null"/>.</param>
    /// <param name="annIndex">The approximate retrieval plane kept in step with every local mutation, or <see langword="null"/> when the host binds the exact scan and no index is maintained.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextVectorWriter(
        IGrainFactory grainFactory,
        Serializer serializer,
        ILatticeReplicationContext replication,
        RepoContextVectorCache cache,
        RepoContextVectorPlaneReDeriver reDeriver,
        IRepoContextAnnIndex? annIndex = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(replication);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(reDeriver);
        _grainFactory = grainFactory;
        _serializer = serializer;
        _replication = replication;
        _cache = cache;
        _reDeriver = reDeriver;
        _annIndex = annIndex;
    }

    // ── Vector-plane self-heal guards ──────────────────────────────────────
    // Every operation against a rebuildable vector tree funnels through one of
    // these so a terminal LeafProjectionStaleException (the tree fell off its
    // write-ahead log) is detected at the narrowest seam where the target tree is
    // a known local constant, surfaced (logged + metered), and triggers a bounded
    // single-flight re-derivation before the fault is re-thrown. The payload tree
    // is not guarded: it is write-once and content-addressed, out of the
    // re-derivation allow-list, so its faults propagate unchanged.

    private Task GuardMetadataAsync(Func<Task> operation, CancellationToken cancellationToken)
        => _reDeriver.GuardAsync(RepoContextTrees.VectorMetadata, operation, cancellationToken);

    private Task<T> GuardMetadataAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
        => _reDeriver.GuardAsync(RepoContextTrees.VectorMetadata, operation, cancellationToken);

    private Task GuardMembershipAsync(Func<Task> operation, CancellationToken cancellationToken)
        => _reDeriver.GuardAsync(RepoContextTrees.VectorMembership, operation, cancellationToken);

    private Task<T> GuardMembershipAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
        => _reDeriver.GuardAsync(RepoContextTrees.VectorMembership, operation, cancellationToken);

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

        // Only materialised when an approximate plane is bound: with the exact scan
        // configured there is no index to keep in step and the write path stays
        // exactly as it was.
        var updates = _annIndex is null ? null : new List<RepoContextAnnVectorUpdate>(vectors.Count);

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
            updates?.Add(new RepoContextAnnVectorUpdate(vectorId, sourceKey, vectors[unit]));
        }

        var retired = await RetireStaleAsync(repoId, sourceId, keep, cancellationToken).ConfigureAwait(false);

        // The metadata/payload trees the exact-kNN gather scans just changed for this
        // repository, so drop any warm cached candidate set precisely and immediately.
        _cache.Invalidate(repoId);

        // The same seam keeps the approximate index in step. It runs after the store
        // of record has landed and after the retirements are known, so the index can
        // never be ahead of the source, and a superseded vector is dropped before its
        // replacement is added.
        if (_annIndex is not null)
        {
            await _annIndex
                .ApplyWriteAsync(repoId, tag, updates!, retired, cancellationToken)
                .ConfigureAwait(false);
        }

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

        // With an approximate plane bound, the identifiers the range delete is about
        // to remove are collected first with a key-only walk of this one source's
        // prefix - no value is transferred - so the index can drop them and can
        // never return a vector the store of record no longer holds.
        var retired = _annIndex is null
            ? Array.Empty<string>()
            : await ListVectorIdsAsync(repoId, sourceId, cancellationToken).ConfigureAwait(false);

        await DeleteVectorsAsync(repoId, sourceId, cancellationToken).ConfigureAwait(false);
        await RemoveMemberAsync(repoId, sourceId, cancellationToken).ConfigureAwait(false);

        // Also clear any "considered, no passages" marker: a pruned file that was
        // contentless carries a marker but no vectors and no real membership flag,
        // so its marker must be retired too or it lingers past the file's deletion.
        await UnmarkContentlessAsync(repoId, sourceId, cancellationToken).ConfigureAwait(false);

        // The metadata tree the exact-kNN gather scans just lost this source's
        // vectors, so drop any warm cached candidate set precisely and immediately.
        _cache.Invalidate(repoId);

        if (_annIndex is not null && retired.Count > 0)
        {
            // A retirement is space-agnostic: a vector identifier is unique within a
            // repository, so it is applied to every embedding space the plane holds
            // an index for, and is a no-op in the ones that never held it.
            await _annIndex.ApplyRetirementAsync(repoId, retired, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Lists the live vector identifiers of one source with a key-only walk of its
    /// presence-key prefix, so no metadata value crosses the grain boundary.
    /// </summary>
    private async Task<IReadOnlyList<string>> ListVectorIdsAsync(
        string repoId, string sourceId, CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var prefix = $"{RepoContextKeys.VectorsPrefix(repoId)}{sourceId}.";
        var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);

        var ids = new List<string>();
        await foreach (var key in tree
            .KeysAsync(prefix, endExclusive, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (RepoContextKeys.TryParse(key, out var parsed)
                && parsed.Kind == RepoContextRecordKind.VectorMetadata
                && parsed.VectorId is not null)
            {
                ids.Add(parsed.VectorId);
            }
        }

        return ids;
    }

    private Task DeleteVectorsAsync(string repoId, string sourceId, CancellationToken cancellationToken)
        => GuardMetadataAsync(async () =>
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
        }, cancellationToken);

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

    private Task WriteMetadataAsync(
        string repoId,
        string vectorId,
        string sourceKey,
        string contentAddress,
        EmbeddingSpaceTag tag,
        CancellationToken cancellationToken)
        => GuardMetadataAsync(async () =>
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
        }, cancellationToken);

    private Task<List<string>> RetireStaleAsync(
        string repoId, string sourceId, IReadOnlySet<string> keep, CancellationToken cancellationToken)
        => GuardMetadataAsync(async () =>
        {
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
            var prefix = $"{RepoContextKeys.VectorsPrefix(repoId)}{sourceId}.";

            string? token = null;
            var stale = new List<string>();
            var retiredIds = new List<string>();
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
                        retiredIds.Add(parsed.VectorId);
                    }
                }

                token = page.HasMore ? page.ContinuationToken : null;
            }
            while (token is not null);

            foreach (var key in stale)
            {
                await tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            }

            return retiredIds;
        }, cancellationToken);

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

        await GuardMembershipAsync(async () =>
        {
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var replicaId = ReplicaId();
            foreach (var sourceKey in sourceKeys)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var key = RepoContextKeys.VectorMembership(repoId, VectorCodec.SourceId(sourceKey));
                await tree.OrFlag(key).EnableAsync(replicaId, cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);

        // Membership does not feed the gather, but a batch's membership write always
        // trails its StoreAsync vectors, so invalidate defensively to keep the cache
        // consistent with any future gather that consults membership.
        _cache.Invalidate(repoId);
    }

    private Task RemoveMemberAsync(string repoId, string sourceId, CancellationToken cancellationToken)
        => GuardMembershipAsync(async () =>
        {
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var key = RepoContextKeys.VectorMembership(repoId, sourceId);

            // Disable rather than delete so the removal carries causal history and
            // converges add-wins against a concurrent enable on another cluster.
            await tree.OrFlag(key).DisableAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken);

    /// <summary>
    /// Records a "considered, no passages" marker for each contentless source in a
    /// batch - one enable-wins flag per source at the reserved
    /// <see cref="ContentlessMarkerPrefix"/> collection, so it shares the membership
    /// tree's convergence and durability without being counted as an embedded
    /// vector. A marked file is treated as covered by the gap sweep and by
    /// unchanged-file selection, which is what stops an empty or whitespace-only file
    /// from being re-scanned and re-read on every reconcile. Re-marking an already
    /// marked source is idempotent.
    /// </summary>
    /// <param name="repoId">The repository the sources belong to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKeys">The canonical record keys of the files found contentless. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="sourceKeys"/> is null.</exception>
    public async Task MarkContentlessAsync(
        string repoId, IReadOnlyList<string> sourceKeys, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(sourceKeys);

        if (sourceKeys.Count == 0)
        {
            return;
        }

        await GuardMembershipAsync(async () =>
        {
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var replicaId = ReplicaId();
            foreach (var sourceKey in sourceKeys)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var key = RepoContextKeys.VectorMembership(
                    repoId, ContentlessMarkerPrefix + VectorCodec.SourceId(sourceKey));
                await tree.OrFlag(key).EnableAsync(replicaId, cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Clears the "considered, no passages" marker for a single source identifier -
    /// used when a previously contentless file gains embeddable content (so it stops
    /// being covered by the marker and its real embedding takes over) and when a
    /// contentless file is pruned. Disabling an absent marker is a harmless no-op.
    /// </summary>
    /// <param name="repoId">The repository the source belongs to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceId">The 16-character source identifier whose marker to clear. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="sourceId"/> is null.</exception>
    public Task UnmarkContentlessAsync(
        string repoId, string sourceId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(sourceId);

        return GuardMembershipAsync(() =>
        {
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var key = RepoContextKeys.VectorMembership(repoId, ContentlessMarkerPrefix + sourceId);
            return tree.OrFlag(key).DisableAsync(cancellationToken);
        }, cancellationToken);
    }

    /// <summary>
    /// Point-probes the membership coverage of a <b>bounded candidate set</b> of
    /// source keys, returning only the coverage of those candidates rather than
    /// scanning the whole membership tree. Each candidate key is reduced to its
    /// <see cref="VectorCodec.SourceId(string)"/> and looked up directly with a
    /// batched <see cref="ILattice.GetManyAsync(System.Collections.Generic.List{string}, CancellationToken)"/>,
    /// so the read never gathers a sorted range and its cost is a function of the
    /// candidate count, not the tree size. Membership is an enable-wins
    /// <see cref="OrFlag"/>, so a point read is strictly at least as current as a
    /// whole-set scan; a candidate absent from the probe simply re-embeds
    /// idempotently. Both the embedded flag and the contentless marker are probed
    /// for each candidate, mirroring <see cref="LoadCoverageAsync"/>.
    /// </summary>
    /// <param name="repoId">The repository whose coverage to probe. Must not be <see langword="null"/>.</param>
    /// <param name="candidateSourceKeys">The bounded set of canonical record keys to probe coverage for. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The embedded and contentless-marker source identifiers, restricted to the probed candidates.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="candidateSourceKeys"/> is null.</exception>
    public Task<RepoContextEmbeddingCoverage> ProbeCoverageAsync(
        string repoId, IReadOnlyList<string> candidateSourceKeys, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(candidateSourceKeys);
        return ProbeMembershipAsync(repoId, candidateSourceKeys, includeContentless: true, cancellationToken);
    }

    /// <summary>
    /// Point-probes the live <b>embedded</b> source identifiers of a bounded
    /// candidate set, so the symbol-ingest path can decide re-embedding per page
    /// without scanning the whole membership tree. Only the plain embedded flag is
    /// probed - contentless markers live under a different key and are naturally
    /// excluded - mirroring <see cref="LoadEmbeddedMembersAsync"/> on the candidate
    /// subset. See <see cref="ProbeCoverageAsync"/> for the bounding rationale.
    /// </summary>
    /// <param name="repoId">The repository whose embedded members to probe. Must not be <see langword="null"/>.</param>
    /// <param name="candidateSourceKeys">The bounded set of canonical record keys to probe. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The live embedded source identifiers restricted to the probed candidates.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="candidateSourceKeys"/> is null.</exception>
    public async Task<IReadOnlySet<string>> ProbeEmbeddedMembersAsync(
        string repoId, IReadOnlyList<string> candidateSourceKeys, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(candidateSourceKeys);
        var coverage = await ProbeMembershipAsync(
            repoId, candidateSourceKeys, includeContentless: false, cancellationToken).ConfigureAwait(false);
        return coverage.Embedded;
    }

    /// <summary>
    /// Point-probes the <b>covered</b> source identifiers of a bounded candidate set
    /// - real embedded sources unioned with contentless markers - so the always-on
    /// gap sweep can classify exactly the files on the page it is walking without
    /// scanning the whole membership tree. See <see cref="ProbeCoverageAsync"/> for
    /// the bounding rationale and <see cref="LoadCoveredSourceIdsAsync"/> for the
    /// whole-set equivalent.
    /// </summary>
    /// <param name="repoId">The repository whose covered set to probe. Must not be <see langword="null"/>.</param>
    /// <param name="candidateSourceKeys">The bounded set of canonical record keys to probe. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The union of embedded and contentless-marker source identifiers restricted to the probed candidates.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="candidateSourceKeys"/> is null.</exception>
    public async Task<IReadOnlySet<string>> ProbeCoveredSourceIdsAsync(
        string repoId, IReadOnlyList<string> candidateSourceKeys, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(candidateSourceKeys);
        var coverage = await ProbeMembershipAsync(
            repoId, candidateSourceKeys, includeContentless: true, cancellationToken).ConfigureAwait(false);
        if (coverage.Contentless.Count == 0)
        {
            return coverage.Embedded;
        }

        var covered = new HashSet<string>(coverage.Embedded, StringComparer.Ordinal);
        covered.UnionWith(coverage.Contentless);
        return covered;
    }

    /// <summary>
    /// Shared bounded point-probe: reduces the candidate keys to distinct source
    /// identifiers, batches them through <see cref="ILattice.GetManyAsync(System.Collections.Generic.List{string}, CancellationToken)"/>
    /// in <see cref="MembershipProbeBatchSize"/>-sized chunks, and decodes each
    /// returned row exactly as a whole-set scan would.
    /// </summary>
    private Task<RepoContextEmbeddingCoverage> ProbeMembershipAsync(
        string repoId,
        IReadOnlyList<string> candidateSourceKeys,
        bool includeContentless,
        CancellationToken cancellationToken)
        => GuardMembershipAsync(async () =>
        {
            var embedded = new HashSet<string>(StringComparer.Ordinal);
            var contentless = new HashSet<string>(StringComparer.Ordinal);
            if (candidateSourceKeys.Count == 0)
            {
                return new RepoContextEmbeddingCoverage(embedded, contentless);
            }

            var sourceIds = new HashSet<string>(StringComparer.Ordinal);
            foreach (var key in candidateSourceKeys)
            {
                sourceIds.Add(VectorCodec.SourceId(key));
            }

            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var batch = new List<string>(MembershipProbeBatchSize);
            foreach (var sourceId in sourceIds)
            {
                cancellationToken.ThrowIfCancellationRequested();
                batch.Add(RepoContextKeys.VectorMembership(repoId, sourceId));
                if (includeContentless)
                {
                    batch.Add(RepoContextKeys.VectorMembership(repoId, ContentlessMarkerPrefix + sourceId));
                }

                if (batch.Count >= MembershipProbeBatchSize)
                {
                    await ProbeBatchAsync(tree, batch, embedded, contentless, cancellationToken).ConfigureAwait(false);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await ProbeBatchAsync(tree, batch, embedded, contentless, cancellationToken).ConfigureAwait(false);
            }

            return new RepoContextEmbeddingCoverage(embedded, contentless);
        }, cancellationToken);

    private static async Task ProbeBatchAsync(
        ILattice tree,
        List<string> keys,
        HashSet<string> embedded,
        HashSet<string> contentless,
        CancellationToken cancellationToken)
    {
        var found = await tree.GetManyAsync(keys, cancellationToken).ConfigureAwait(false);
        foreach (var (key, value) in found)
        {
            if (!JsonLatticeSerializer<OrFlag>.Default.Deserialize(value).IsEnabled
                || !TryReadSourceId(key, out var collection))
            {
                continue;
            }

            if (collection.StartsWith(ContentlessMarkerPrefix, StringComparison.Ordinal))
            {
                contentless.Add(collection[ContentlessMarkerPrefix.Length..]);
            }
            else
            {
                embedded.Add(collection);
            }
        }
    }

    /// <summary>
    /// Enumerates a repository's membership rows in bounded pages, so a whole-set
    /// read never holds a single unbounded sorted-range scan open long enough to
    /// approach the response deadline. Each page reads at most
    /// <see cref="RepoContextPortability.DefaultPageSize"/> live rows and reopens
    /// from a continuation token, yielding only non-null values.
    /// </summary>
    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> EnumerateMembershipPagedAsync(
        ILattice tree,
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        string? token = null;
        do
        {
            var page = await RepoContextPortability
                .EnumerateAsync(tree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);
            foreach (var record in page.Records)
            {
                if (record.Value is not null)
                {
                    yield return new KeyValuePair<string, byte[]>(record.Key, record.Value);
                }
            }

            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);
    }

    /// <summary>
    /// Loads the repository's membership coverage in a single scan, separating the
    /// two kinds of enabled flag the tree holds: real embedded sources (a landed
    /// vector) and contentless markers (a file considered and found to have no
    /// embeddable passage). Both sets carry plain 16-character source identifiers -
    /// the marker's <see cref="ContentlessMarkerPrefix"/> is stripped - so a caller
    /// probes either set with the same identifier produced by
    /// <see cref="VectorCodec.SourceId(string)"/>. A file is "covered" (not a gap)
    /// when it is in either set; see <see cref="RepoContextEmbeddingCoverage.IsCovered"/>.
    /// Returns empty sets when the repository has embedded and considered nothing yet.
    /// </summary>
    /// <param name="repoId">The repository whose coverage to load. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The embedded and contentless-marker source-identifier sets.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public Task<RepoContextEmbeddingCoverage> LoadCoverageAsync(
        string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        return GuardMembershipAsync(async () =>
        {
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var prefix = RepoContextKeys.VectorMembershipsPrefix(repoId);
            var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);
            var embedded = new HashSet<string>(StringComparer.Ordinal);
            var contentless = new HashSet<string>(StringComparer.Ordinal);
            if (endExclusive is null)
            {
                return new RepoContextEmbeddingCoverage(embedded, contentless);
            }

            await foreach (var entry in EnumerateMembershipPagedAsync(tree, prefix, cancellationToken)
                .ConfigureAwait(false))
            {
                if (!JsonLatticeSerializer<OrFlag>.Default.Deserialize(entry.Value).IsEnabled
                    || !TryReadSourceId(entry.Key, out var collection))
                {
                    continue;
                }

                if (collection.StartsWith(ContentlessMarkerPrefix, StringComparison.Ordinal))
                {
                    contentless.Add(collection[ContentlessMarkerPrefix.Length..]);
                }
                else
                {
                    embedded.Add(collection);
                }
            }

            return new RepoContextEmbeddingCoverage(embedded, contentless);
        }, cancellationToken);
    }

    /// <summary>
    /// Loads the repository's covered source identifiers - real embedded sources
    /// unioned with contentless markers - so the always-on gap sweep probes one set
    /// and treats a considered-but-contentless file as covered rather than a missing
    /// embedding. Returns an empty set when nothing is embedded or considered yet.
    /// </summary>
    /// <param name="repoId">The repository whose covered set to load. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The union of embedded and contentless-marker source identifiers.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public async Task<IReadOnlySet<string>> LoadCoveredSourceIdsAsync(
        string repoId, CancellationToken cancellationToken)
    {
        var coverage = await LoadCoverageAsync(repoId, cancellationToken).ConfigureAwait(false);
        if (coverage.Contentless.Count == 0)
        {
            return coverage.Embedded;
        }

        var covered = new HashSet<string>(coverage.Embedded, StringComparer.Ordinal);
        covered.UnionWith(coverage.Contentless);
        return covered;
    }

    /// <summary>
    /// Loads the repository's live embedded source identifiers into a set so
    /// a caller can probe presence with <see cref="System.Collections.Generic.IReadOnlySet{T}.Contains(T)"/>.
    /// Presence is an enable-wins flag, so the read decodes each row and keeps only the
    /// enabled ones (a disabled flag still occupies a key until the compactor reclaims
    /// it). The membership tree carries only 16-character source identifiers, never the
    /// embeddings themselves. Contentless "considered, no passages" markers (see
    /// <see cref="ContentlessMarkerPrefix"/>) share the tree but are excluded here, so
    /// this returns only sources that carry a real landed vector. Returns an empty set
    /// when the repository has embedded nothing yet.
    /// </summary>
    /// <param name="repoId">The repository whose embedded source identifiers to load. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The set of live embedded source identifiers.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public Task<IReadOnlySet<string>> LoadEmbeddedMembersAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        return GuardMembershipAsync<IReadOnlySet<string>>(async () =>
        {
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var prefix = RepoContextKeys.VectorMembershipsPrefix(repoId);
            var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);
            var members = new HashSet<string>(StringComparer.Ordinal);
            if (endExclusive is null)
            {
                return members;
            }

            await foreach (var entry in EnumerateMembershipPagedAsync(tree, prefix, cancellationToken)
                .ConfigureAwait(false))
            {
                if (JsonLatticeSerializer<OrFlag>.Default.Deserialize(entry.Value).IsEnabled
                    && TryReadSourceId(entry.Key, out var sourceId)
                    && !sourceId.StartsWith(ContentlessMarkerPrefix, StringComparison.Ordinal))
                {
                    members.Add(sourceId);
                }
            }

            return members;
        }, cancellationToken);
    }

    /// <summary>
    /// Counts the live embedded sources for <paramref name="repoId"/> - the number of
    /// enabled membership flags that carry a real vector. A disabled flag still occupies
    /// a key until the compactor reclaims it, so the count decodes each row rather than
    /// counting keys, and it excludes contentless "considered, no passages" markers (see
    /// <see cref="ContentlessMarkerPrefix"/>) so <c>embeddedVectorCount</c> stays an
    /// honest count of sources that actually have a landed embedding. Returns zero when
    /// nothing is embedded.
    /// </summary>
    /// <param name="repoId">The repository whose embedded source count to read. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The number of live embedded sources.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public Task<long> CountEmbeddedAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        return GuardMembershipAsync(async () =>
        {
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var prefix = RepoContextKeys.VectorMembershipsPrefix(repoId);
            var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);
            if (endExclusive is null)
            {
                return 0L;
            }

            var enabled = 0L;
            await foreach (var entry in EnumerateMembershipPagedAsync(tree, prefix, cancellationToken)
                .ConfigureAwait(false))
            {
                if (JsonLatticeSerializer<OrFlag>.Default.Deserialize(entry.Value).IsEnabled
                    && TryReadSourceId(entry.Key, out var sourceId)
                    && !sourceId.StartsWith(ContentlessMarkerPrefix, StringComparison.Ordinal))
                {
                    enabled++;
                }
            }

            return enabled;
        }, cancellationToken);
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
