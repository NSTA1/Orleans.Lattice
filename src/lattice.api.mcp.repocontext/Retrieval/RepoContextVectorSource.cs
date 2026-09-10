using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The store-of-record view the approximate index derives itself from: one
/// repository's vectors in one embedding space, read from the reserved
/// vector-metadata and vector-payload trees.
/// <para>
/// <b>Fail-closed on embedding space.</b> The view yields only vectors written in
/// the space it was created for, so an index built from it can never hold two
/// spaces at once and a query can never be scored against a vector from a
/// different model, dimension, or normalization convention. That guard lives here
/// - at the narrowest seam, where the stored space tag is read - rather than
/// being re-applied by every consumer.
/// </para>
/// <para>
/// <b>Resumable by construction.</b> The metadata tree enumerates in ascending
/// ordinal key order and every key is the constant repository prefix followed by
/// the vector identifier, so ascending key order <i>is</i> ascending identifier
/// order and a build resumes by asking for the page after the last identifier it
/// durably consumed. The scan itself is the shared resilient page read, so a
/// transient enumeration abort (silo failover, cold start, idle expiry) resumes
/// without gaps or duplicates rather than failing the build.
/// </para>
/// </summary>
internal sealed class RepoContextVectorSource : IRepoContextVectorSource
{
    /// <summary>
    /// Reconnect budget for the whole-prefix count walk. Deliberately far above
    /// <see cref="LatticeExtensions.DefaultScanReconnectAttempts"/>: see
    /// <see cref="CountAsync"/>.
    /// </summary>
    private const int CountReconnectAttempts = 64;

    /// <summary>
    /// Wall-clock ceiling on the whole-prefix count walk, above which the walk stops
    /// and reports the count as unknown rather than continuing.
    /// <para>
    /// The reconnect budget above bounds RETRIES; it does not bound WORK. A walk that
    /// never aborts is never retried and so was never bounded at all: it ran until it
    /// reached the end of the prefix, however many leaves that took (issue #2447).
    /// This is the missing half.
    /// </para>
    /// <para>
    /// The value is chosen against the call it runs inside, not against the corpus.
    /// The walk happens on the build's turn-holding path, so every caller of that
    /// grain waits behind it, and the Orleans call timeout that governs those waiting
    /// callers is 30 seconds. Ten leaves room for the rest of the phase to complete
    /// inside one call. Losing the walk is cheap and losing it is bounded: the count
    /// sizes a capacity reservation and reports progress, and neither consumer needs
    /// it to be correct - which is exactly why spending unbounded time on it was the
    /// wrong trade.
    /// </para>
    /// </summary>
    internal static readonly TimeSpan DefaultCountBudget = TimeSpan.FromSeconds(10);

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;
    private readonly string _repoId;
    private readonly EmbeddingSpaceTag _space;
    private readonly TimeSpan _countBudget;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates the store-of-record view.</summary>
    /// <param name="grainFactory">The grain factory used to reach the reserved vector trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode vector records. Must not be <see langword="null"/>.</param>
    /// <param name="repoId">The repository whose vectors the view covers. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space the view is filtered to.</param>
    /// <param name="countBudget">
    /// The wall-clock ceiling on the <see cref="CountAsync"/> walk, or
    /// <see langword="null"/> for <see cref="DefaultCountBudget"/>. A non-positive
    /// value disables the bound and restores the unbounded pre-#2447 walk, which is
    /// offered only so a test can assert the difference.
    /// </param>
    /// <param name="timeProvider">
    /// The clock the count budget is measured against, or <see langword="null"/> for
    /// <see cref="TimeProvider.System"/>.
    /// </param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public RepoContextVectorSource(
        IGrainFactory grainFactory,
        Serializer serializer,
        string repoId,
        EmbeddingSpaceTag space,
        TimeSpan? countBudget = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(repoId);
        _grainFactory = grainFactory;
        _serializer = serializer;
        _repoId = repoId;
        _space = space;
        _countBudget = countBudget ?? DefaultCountBudget;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public int Dimensions => _space.Dimension;

    /// <inheritdoc />
    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var metadataTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var payloadTree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorPayload);
        var prefix = RepoContextKeys.VectorsPrefix(_repoId);

        // The continuation token is the last key consumed, and the shared page read
        // resumes strictly after it, so resuming a build is exactly "the page after
        // the identifier I last checkpointed".
        var token = afterIdExclusive is null ? null : RepoContextKeys.Vector(_repoId, afterIdExclusive);

        // A payload is content-addressed, so several vectors can share one payload
        // key. Decode each distinct payload at most once per page.
        var decoded = new Dictionary<string, float[]>(StringComparer.Ordinal);

        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await RepoContextPortability
                .EnumerateAsync(
                    metadataTree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);

            var pending = new List<PendingVector>(page.Records.Count);
            List<string>? toFetch = null;
            foreach (var record in page.Records)
            {
                if (record.Value is null)
                {
                    continue;
                }

                var metadata = _serializer.Deserialize<VectorMetadataRecord>(record.Value);
                if (!VectorSpaceGuard.Matches(metadata.Space, _space))
                {
                    continue;
                }

                var contentAddress = RepoContextValues.ReadString(metadata.ContentAddress);
                if (contentAddress is null)
                {
                    continue;
                }

                var payloadKey = RepoContextKeys.VectorPayload(metadata.RepoId, contentAddress);
                pending.Add(new PendingVector(metadata.VectorId, payloadKey));
                if (!decoded.ContainsKey(payloadKey))
                {
                    (toFetch ??= []).Add(payloadKey);
                }
            }

            if (toFetch is { Count: > 0 })
            {
                var distinct = toFetch.Count == 1 ? toFetch : [.. toFetch.Distinct(StringComparer.Ordinal)];
                var fetched = await payloadTree.GetManyAsync(distinct, cancellationToken).ConfigureAwait(false);
                foreach (var (payloadKey, payloadBytes) in fetched)
                {
                    var vector = RepoContextVectorPayloads.Decode(_serializer, payloadBytes);
                    if (vector is not null && vector.Length == _space.Dimension)
                    {
                        decoded[payloadKey] = vector;
                    }
                }
            }

            foreach (var item in pending)
            {
                // A vector whose payload could not be loaded is dropped, exactly as
                // the exact scan drops it: the index is a projection and may lag in
                // the missing direction, never hold something the store does not.
                if (decoded.TryGetValue(item.PayloadKey, out var vector))
                {
                    yield return new VectorSourceEntry(item.VectorId, vector);
                }
            }

            // The page's decoded payloads are only reusable within the page: keeping
            // them would grow a dictionary with the corpus, which is the shape this
            // whole plane exists to remove.
            decoded.Clear();
            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Counted with a key-only walk, which never transfers a value and so costs a
    /// small fraction of the streaming enumeration. In a mixed-space repository the
    /// figure is an upper bound rather than an exact count, which the seam
    /// explicitly permits: it sizes the index's initial reservation and reports
    /// progress, and nothing depends on it for correctness.
    /// <para>
    /// Walked through <see cref="LatticeExtensions.ScanKeysAsync"/> rather than the
    /// raw <see cref="ILattice.KeysAsync"/> stream. The raw stream surfaces
    /// <c>EnumerationAbortedException</c> when the remote enumerator is reclaimed
    /// mid-scan, and this walk covers the repository's ENTIRE vector prefix, which
    /// activates every leaf of the metadata tree and takes long enough on a real
    /// corpus to outlive the enumerator. Because a build calls this before it
    /// streams, that abort took down the WHOLE index build - the one call in it
    /// whose own contract says nothing depends on it for correctness - and the
    /// build then retried and aborted again on every subsequent query, so no index
    /// was ever persisted. Measured on a restored copy of the live deployment
    /// (#1844); the resilient wrapper resumes deterministically with no duplicates
    /// and no gaps, which is exactly what a count needs.
    /// </para>
    /// <para>
    /// The reconnect budget is raised well above
    /// <see cref="LatticeExtensions.DefaultScanReconnectAttempts"/> because the
    /// default was ALSO measured to be too small here. Every abort on this walk is
    /// a cold leaf activation outrunning the enumerator's idle expiry, and a tree
    /// holding a repository's whole vector corpus has far more than eight of them
    /// to activate on a cold start. The budget bounds retries, not work: each
    /// reopen resumes strictly after the last key seen, so a larger budget cannot
    /// re-walk ground already covered. The caller tolerates exhaustion anyway, so
    /// this only decides how often the cheap path is taken.
    /// </para>
    /// <para>
    /// The reconnect budget bounds RETRIES, not WORK, and those are different
    /// guarantees. A walk that never aborts is never retried, so before #2447 it was
    /// bounded by nothing at all and ran until it reached the end of the prefix -
    /// on the turn-holding build path, with every other caller of the grain waiting
    /// behind it. <see cref="DefaultCountBudget"/> supplies the missing half.
    /// </para>
    /// <para>
    /// Exceeding that budget raises
    /// <see cref="RepoContextCountBudgetExceededException"/> rather than returning
    /// what was walked so far. Returning the partial figure would be an UNDER-count,
    /// and the shortfall probe reads an under-count as "the index is not behind" and
    /// skips a repair it needed - so the cheap fix would have bought a bounded walk
    /// at the price of an index that lags the store of record silently. Both callers
    /// treat the fault as "unknown" and resolve it in their own safe direction, which
    /// is the same conclusion the reconnect-exhaustion path already reached.
    /// </para>
    /// </remarks>
    /// <exception cref="RepoContextCountBudgetExceededException">
    /// The walk did not reach the end of the prefix within its wall-clock budget, so
    /// no count can be reported.
    /// </exception>
    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var prefix = RepoContextKeys.VectorsPrefix(_repoId);
        var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);

        var bounded = _countBudget > TimeSpan.Zero;
        var startedAt = bounded ? _timeProvider.GetTimestamp() : 0L;

        var count = 0;
        var overBudget = false;
        await foreach (var _ in tree
            .ScanKeysAsync(prefix, endExclusive, maxAttempts: CountReconnectAttempts, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            count++;

            // Checked AFTER the key is counted, so a budget smaller than the cost of
            // a single key still makes progress rather than spinning. The walk is
            // abandoned, not resumed: a count has no checkpoint to resume from, and
            // the caller does not need one because it only needs to know that the
            // figure is unavailable.
            if (bounded && _timeProvider.GetElapsedTime(startedAt) >= _countBudget)
            {
                overBudget = true;
                break;
            }
        }

        // Raised outside the enumeration so the scan's enumerator is disposed first,
        // and so the throw cannot be mistaken by the resilient wrapper for a fault of
        // the underlying stream that it should reconnect around.
        return overBudget
            ? throw new RepoContextCountBudgetExceededException(_repoId, count, _countBudget)
            : count;
    }

    /// <inheritdoc />
    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        return tree.ExistsAsync(RepoContextKeys.Vector(_repoId, id), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, string>> ResolveSourceKeysAsync(
        IReadOnlyList<string> vectorIds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(vectorIds);
        if (vectorIds.Count == 0)
        {
            return EmptySourceKeys;
        }

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var keys = new List<string>(vectorIds.Count);
        for (var i = 0; i < vectorIds.Count; i++)
        {
            keys.Add(RepoContextKeys.Vector(_repoId, vectorIds[i]));
        }

        var fetched = await tree.GetManyAsync(keys, cancellationToken).ConfigureAwait(false);
        var resolved = new Dictionary<string, string>(fetched.Count, StringComparer.Ordinal);
        foreach (var (_, value) in fetched)
        {
            var metadata = _serializer.Deserialize<VectorMetadataRecord>(value);

            // The store of record settles every disagreement: a record whose space no
            // longer matches, or that carries no source key, is simply not resolved,
            // so the index can never hydrate a hit the store would not stand behind.
            if (!VectorSpaceGuard.Matches(metadata.Space, _space))
            {
                continue;
            }

            var sourceKey = RepoContextValues.ReadString(metadata.SourceKey);
            if (!string.IsNullOrEmpty(sourceKey))
            {
                resolved[metadata.VectorId] = sourceKey;
            }
        }

        return resolved;
    }

    private static readonly Dictionary<string, string> EmptySourceKeys = new(StringComparer.Ordinal);

    private readonly record struct PendingVector(string VectorId, string PayloadKey);
}
