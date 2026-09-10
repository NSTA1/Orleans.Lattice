using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// Binds <see cref="IVectorIndexStore"/> to a Lattice tree, so a persisted index
/// is ordinary Lattice data: sharded, replicated, backed up, and reclaimed by the
/// same machinery as everything else in the store.
/// <para>
/// This is the only type in the package that touches Orleans or a Lattice tree.
/// The algorithmic core in <c>Orleans.Lattice.Vector</c> and the whole durable
/// engine above this seam are free of both, so they can be exercised - and
/// reused - without a silo.
/// </para>
/// <para>
/// Give the index its own tree, or at least its own key prefix. It is a derived
/// projection and its recovery path is to delete a whole key range and rebuild;
/// pointing it at a tree holding anything else would put a store of record in
/// range of that.
/// </para>
/// </summary>
/// <param name="tree">The Lattice tree the index is persisted on. Must not be <see langword="null"/>.</param>
public sealed class LatticeVectorIndexStore(ILattice tree) : IVectorIndexStore
{
    /// <summary>
    /// How many times <see cref="ScanAsync"/> resumes a walk abandoned by the
    /// Orleans response timeout before giving up.
    /// <para>
    /// Deliberately small, and chosen on the same reasoning as
    /// <see cref="LatticeExtensions.DefaultScanStallResumeAttempts"/>: each
    /// attempt costs a whole response timeout before it fails, so a generous
    /// budget would not rescue a walk that cannot finish - it would only make
    /// each doomed reload slower, against the very tree whose contention caused
    /// the timeout.
    /// </para>
    /// <para>
    /// It does not need to be large to remove the livelock. The failure this
    /// bounds is total: without a resume, a walk that times out on its last page
    /// discards every page before it. Two resumes convert "restart the whole
    /// O(corpus) reload" into "re-issue one page", which is the entire
    /// difference between a build that converges and one that does not.
    /// </para>
    /// </summary>
    public const int DefaultScanTimeoutResumeAttempts = 2;

    // Backoff before resuming a timed-out page walk.
    //
    // The first resume is immediate, on exactly the reasoning
    // ComputeReconnectDelayMs applies to an enumerator reclaim: there is nothing
    // to back off from, because the fault itself already provided the separation.
    // A response timeout is the strongest case for that - the walk has just spent
    // the whole timeout waiting, so a further pause adds nothing that the fault
    // has not already supplied, and delaying would only lengthen a reload that is
    // finally about to make progress. Later attempts ramp, so a tree that is
    // genuinely saturated is not re-entered at the same rate as one that hit a
    // single slow page.
    //
    // Unlike the core stall resume - which derives its backoff from the ceiling
    // the ScanPageStalledException itself reports, so the two cannot drift apart
    // when a deployment retunes MaxScanPageStallDuration - a bare
    // TimeoutException carries no ceiling to derive from, and ILattice exposes
    // no way to ask the tree what its response timeout is. So the ramp is a fixed
    // duration, and saying so plainly is better than deriving it from a
    // hard-coded copy of the 30-second default that would silently stop matching
    // a retuned deployment.
    private static readonly TimeSpan ScanTimeoutResumeBackoff = TimeSpan.FromSeconds(2);

    private readonly ILattice _tree = tree ?? throw new ArgumentNullException(nameof(tree));

    /// <inheritdoc />
    public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _tree.GetAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
        IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count == 0)
        {
            return EmptyRecords;
        }

        return await _tree.GetManyAsync([.. keys], cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task WriteAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        return entries.Count == 0
            ? Task.CompletedTask
            : _tree.SetManyAsync([.. entries], cancellationToken);
    }

    /// <inheritdoc />
    public async Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        for (var i = 0; i < keys.Count; i++)
        {
            await _tree.DeleteAsync(keys[i], cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Walked through <see cref="LatticeExtensions.ScanEntriesAsync"/> rather than
    /// the raw <see cref="ILattice.EntriesAsync"/> stream. This is the walk that
    /// RELOADS a persisted index at open, so its range grows with the index and
    /// therefore with the corpus - precisely the shape the raw stream warns about,
    /// because it surfaces <c>EnumerationAbortedException</c> when the remote
    /// enumerator is reclaimed mid-scan (silo failover, cold start, idle expiry,
    /// scale-down). An abort here does not degrade the reload, it FAILS it, and the
    /// index then rebuilds from scratch or refuses to serve - which is the opposite
    /// of the "reload instead of recompute" property the persisted index exists to
    /// provide. The resilient wrapper resumes deterministically with no duplicates
    /// and no gaps. The same defect was measured firing for real on the repocontext
    /// count path against a restored copy of a live deployment (#1844).
    /// <para>
    /// The wrapper resumes two fault classes, and the loop below adds a third it
    /// does not cover: the bare <see cref="TimeoutException"/> the Orleans runtime
    /// raises when a response does not arrive in time. It was observed on this
    /// path in the field, terminating a reload that had already walked most of the
    /// corpus, and because it is untyped it falls through both of the wrapper's
    /// catches and fails the whole open (#2539).
    /// <para>
    /// Deliberately NOT claimed here: that this fault and the typed
    /// <c>ScanPageStalledException</c> are the same event. Both were measured on
    /// this tree, but they are recorded at different sites and may be raised by
    /// different call paths, so they are handled as what they are - two fault
    /// classes with independent evidence - rather than unified on a resemblance.
    /// </para>
    /// </para>
    /// <para>
    /// Resuming is safe here for the same reason the wrapper's own resume is: the
    /// walk is a read over an ordinal key range, so continuing from the successor
    /// of the last key actually yielded re-reads nothing and skips nothing.
    /// <see cref="DefaultScanTimeoutResumeAttempts"/> bounds CONSECUTIVE resumes
    /// that bank no records, so a genuinely unavailable tree still fails rather
    /// than looping, while a corpus-sized walk that keeps making progress is not
    /// killed by a budget it spent much earlier in the same walk.
    /// </para>
    /// </remarks>
    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);

        // A "starts with keyPrefix" query is exactly the half-open ordinal range
        // [keyPrefix, PrefixUpperBound(keyPrefix)). The shared helper owns that
        // bound - incrementing the last code unit by hand wraps a trailing U+FFFF
        // to U+0000 and silently inverts the range - and returns null when the
        // range has no finite upper bound, which the scan primitives take to mean
        // "run to the end of the keyspace".
        var upperBound = LatticeKeyRange.PrefixUpperBound(keyPrefix);
        string? lastKey = null;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // The resume bound is the successor of the last key yielded, which is
            // exactly how the core wrapper resumes its own two fault classes.
            // Appending U+0000 is the smallest ordinal step past a key, so the
            // resumed range is half-open on the record already delivered: the
            // caller sees each record once and none is skipped.
            var start = lastKey is null ? keyPrefix : lastKey + "\u0000";
            var entries = _tree.ScanEntriesAsync(start, upperBound, cancellationToken: cancellationToken);
            var enumerator = entries.GetAsyncEnumerator(cancellationToken);
            var completed = false;
            var resuming = false;
            try
            {
                while (true)
                {
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (TimeoutException timeout)
                        when (timeout is not ScanPageStalledException &&
                              attempt < DefaultScanTimeoutResumeAttempts &&
                              !cancellationToken.IsCancellationRequested)
                    {
                        // Deliberately not caught when the caller has cancelled:
                        // a timeout observed during shutdown is not something to
                        // spend a resume budget on.
                        //
                        // ScanPageStalledException derives from TimeoutException
                        // but is deliberately excluded, because the core scan
                        // helpers already resume it on their own budget. Catching
                        // it here would restart a walk that core had ALREADY
                        // resumed to exhaustion, multiplying the two budgets and
                        // turning a bounded recovery into a retry storm. The two
                        // loops therefore govern disjoint exception types and are
                        // complementary rather than nested: core owns the typed
                        // stall, this loop owns only the bare TimeoutException the
                        // Orleans runtime raises when a grain call misses its
                        // response deadline outright, which falls through both of
                        // core's typed catches and would otherwise abandon the
                        // whole reload.
                        attempt++;
                        resuming = true;
                        break;
                    }

                    if (!hasNext)
                    {
                        completed = true;
                        break;
                    }

                    lastKey = enumerator.Current.Key;

                    // Progress replenishes the budget, which is the whole point:
                    // the budget exists to stop a walk that is banking NOTHING,
                    // not to cap the total faults a corpus-sized walk may absorb
                    // over its life. A monotonic counter conflates those two, and
                    // on a reload whose range grows with the corpus it is the
                    // second reading that bites - a walk that resumed correctly
                    // dozens of times, banking real progress each time, still dies
                    // on the next fault because a budget spent hours earlier was
                    // never given back.
                    //
                    // This terminates. Every record strictly advances lastKey, and
                    // the resumed range is half-open above it, so each iteration
                    // walks a strictly smaller range; a walk that makes progress
                    // is converging by construction. A walk that makes none spends
                    // its budget on consecutive faults and rethrows.
                    attempt = 0;
                    yield return enumerator.Current;
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            if (completed || !resuming)
            {
                yield break;
            }

            if (attempt > 1)
            {
                await Task.Delay(ScanTimeoutResumeBackoff * (attempt - 1), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);

        var upperBound = LatticeKeyRange.PrefixUpperBound(keyPrefix);
        if (upperBound is not null)
        {
            await _tree.DeleteRangeAsync(keyPrefix, upperBound, cancellationToken).ConfigureAwait(false);
            return;
        }

        // No finite upper bound exists, so the range runs to the end of the
        // keyspace and the range-delete primitive cannot express it. Enumerating
        // first is correct here because the keys are collected before any delete
        // is issued, so the walk is not invalidated underneath itself. The walk
        // uses the abort-resilient wrapper for the same reason ScanAsync does: an
        // aborted enumerator would yield a SHORT key list, and a short list here
        // means a partial delete that silently leaves a superseded generation
        // behind rather than failing.
        var keys = new List<string>();
        var enumerator = _tree.ScanKeysAsync(keyPrefix, null, cancellationToken: cancellationToken);
        await foreach (var key in enumerator.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            keys.Add(key);
        }

        await DeleteAsync(keys, cancellationToken).ConfigureAwait(false);
    }

    private static readonly Dictionary<string, byte[]> EmptyRecords = [];
}
