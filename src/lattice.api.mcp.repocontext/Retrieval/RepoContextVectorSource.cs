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
    /// <para>
    /// WHAT A ZERO <see cref="RepoContextCountBudgetExceededException.Counted"/>
    /// MEANS, AND WHAT IT DOES NOT. Two mechanisms bound this walk and they bound
    /// different things: the deadline bounds the walk AS A WHOLE, including its
    /// first page, and the sampled in-loop check bounds the gap BETWEEN keys. Only
    /// the second is charged after a key is counted, so only the second carries the
    /// one-key minimum-progress property. A first page slower than the whole budget
    /// therefore reports <c>Counted = 0</c>, and that reading means precisely "the
    /// source did not deliver a first page inside the budget" - it is a measured
    /// absence, not a walk that never started, because the exception is raised only
    /// on a path that did start one. Do not read a zero as an empty prefix: an empty
    /// prefix RETURNS <c>0</c> and never throws, and the two are distinguishable for
    /// exactly that reason. This paragraph exists because the in-loop comment below
    /// once claimed the minimum-progress guarantee held for the walk rather than for
    /// the sampled check, which is a guarantee the deadline can and does defeat.
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

        // The budget has to be a DEADLINE and not just a sample taken between keys.
        // Sampling in the loop body bounds the gap between keys and nothing else,
        // so it does not bound a walk that yields NO key - and that is precisely
        // the walk that needs bounding, because it is the one whose first page
        // stalls. With CountReconnectAttempts set to 64, an unbounded such walk
        // spends a 64-deep reconnect storm plus the stall-resume budget derived
        // from it, all on the build's turn-holding path, before anything gives up.
        // That was measured as a phase tick active for over four minutes against a
        // 30-second call timeout, which starved the coordinator's own keep-alive
        // reminder (issues #2536 and #2483).
        //
        // The token is linked rather than substituted so that a caller-cancelled
        // count is still distinguishable from a merely over-budget one below.
        using var deadline = bounded
            ? new CancellationTokenSource(_countBudget, _timeProvider)
            : null;
        using var linked = deadline is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var walkToken = linked?.Token ?? cancellationToken;

        var count = 0;
        var overBudget = false;
        try
        {
            await foreach (var _ in tree
                .ScanKeysAsync(prefix, endExclusive, maxAttempts: CountReconnectAttempts, cancellationToken: walkToken)
                .ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                count++;

                // Checked AFTER the key is counted, so that THIS check - the sampled
                // one, bounding the gap between keys - cannot consume a walk without
                // advancing it. Read the scope precisely: the one-key minimum belongs
                // to this check, NOT to the walk. The deadline above bounds the walk
                // as a whole and is under no such constraint, so a first page slower
                // than the entire budget still reports zero. An earlier revision of
                // this comment said "a budget smaller than the cost of a single key
                // still makes progress", stated of the walk; that is false whenever
                // the deadline binds first, and the fixture that appeared to prove it
                // only passed because its fake source answered synchronously, so the
                // deadline never armed. A guarantee asserted at the wrong scope is
                // worse than none, because it is believed. The walk is
                // abandoned, not resumed: a count has no checkpoint to resume from, and
                // the caller does not need one because it only needs to know that the
                // figure is unavailable.
                //
                // That reasoning is sound, and it is also the reasoning that produced
                // the hole this method's deadline now closes, so read it for what it
                // covers rather than as a statement about the walk as a whole. It
                // considers a walk that yields keys SLOWLY and concludes - correctly -
                // that the check belongs after the first one. It does not consider a
                // walk that yields NO key, which never reaches this line at all, and
                // which is the walk that was actually observed: the budget was
                // therefore never evaluated and the count ran on unbounded. Do not
                // move this check back to the top of the body on the strength of the
                // first case; the two cases need the two different mechanisms that are
                // now both present.
                if (bounded && _timeProvider.GetElapsedTime(startedAt) >= _countBudget)
                {
                    overBudget = true;
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (
            deadline is { IsCancellationRequested: true } && !cancellationToken.IsCancellationRequested)
        {
            // The deadline stopped the walk mid-read. That is the budget being
            // spent, which this method already has a documented answer for, so it
            // takes the same exit as the sampled path rather than propagating a
            // cancellation the caller never asked for.
            overBudget = true;
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
