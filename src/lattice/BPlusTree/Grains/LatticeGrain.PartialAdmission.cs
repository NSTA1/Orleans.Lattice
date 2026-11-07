namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Partial admission of a batch write (issue #5970): a refused
/// <c>SetManyAsync</c> reports a superset of the keys that may not have landed,
/// so a caller can retry those alone rather than the whole batch.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// Re-raises a refused multi-shard batch as a refusal listing every key that
    /// may not have landed, or returns <see langword="null"/> to surface
    /// <paramref name="refusal"/> unchanged.
    /// </summary>
    /// <param name="refusal">The refusal about to surface.</param>
    /// <param name="buckets">The batch's shard buckets, in dispatch order.</param>
    /// <param name="branches">The per-shard writes, one per bucket, in the same order.</param>
    /// <param name="batchCount">The number of entries in the batch.</param>
    /// <param name="boundPhysicalTreeId">The copy an atomic saga's prepared dispatch is bound to, if any.</param>
    /// <remarks>
    /// <para>
    /// <b>Superset.</b> Evaluated when the refusal surfaces, without waiting for
    /// any branch. A branch that completed confirmed its whole bucket and is
    /// left out. A branch refused for saturation contributes the keys it
    /// reported unapplied, or its whole bucket when it reported none. A branch
    /// still in flight contributes its whole bucket. So a key is left out only
    /// when its shard returned success for it.
    /// </para>
    /// <para>
    /// <see langword="null"/> when that cannot be attributed: a branch that
    /// faulted with anything but a saturation refusal, a cancelled branch, or an
    /// atomic saga's prepared dispatch, whose saga retries the batch whole.
    /// </para>
    /// <para>
    /// Allocates only here, on the failure path. The buckets are re-enumerated
    /// rather than recorded at dispatch, so the healthy path is unchanged.
    /// </para>
    /// </remarks>
    private LatticeSaturatedException? AttributeFanOutRefusal(
        LatticeSaturatedException refusal,
        ShardFanoutBuckets<KeyValuePair<string, byte[]>> buckets,
        List<Task> branches,
        int batchCount,
        string? boundPhysicalTreeId)
    {
        if (!IsFanOutAttributable(branches, boundPhysicalTreeId))
        {
            // A shard's own list covers only its bucket. Surfacing it here, where
            // another branch's keys cannot be accounted for, would let a caller
            // that retries only the listed keys drop those keys for good.
            return refusal.UnappliedKeys is null ? null : WithoutUnappliedKeys(refusal);
        }

        var unapplied = new List<string>();
        var i = 0;
        foreach (var (_, bucket) in buckets)
        {
            var branch = branches[i++];
            if (branch.IsCompletedSuccessfully)
                continue;

            if (branch.Exception is { InnerExceptions: [LatticeSaturatedException { UnappliedKeys: { } reported }] })
            {
                unapplied.AddRange(reported);
                continue;
            }

            foreach (var entry in bucket)
            {
                unapplied.Add(entry.Key);
            }
        }

        return new LatticeSaturatedException(
            RootMessage(refusal),
            TreeId,
            refusal.SaturationSource,
            unapplied,
            batchCount,
            refusal);
    }

    /// <summary>
    /// Whether every branch's outcome can be attributed key by key: none was
    /// cancelled or faulted with anything but a saturation refusal, and the
    /// batch is not an atomic saga's prepared dispatch.
    /// </summary>
    private static bool IsFanOutAttributable(List<Task> branches, string? boundPhysicalTreeId)
    {
        if (boundPhysicalTreeId is not null || LatticePreparedContext.Current)
            return false;

        foreach (var branch in branches)
        {
            if (branch.IsCanceled)
                return false;

            if (branch.IsFaulted && branch.Exception is not { InnerExceptions: [LatticeSaturatedException] })
                return false;
        }

        return true;
    }

    /// <summary>
    /// The same refusal without a list: unknown, so the caller retries the whole
    /// batch, as before issue #5970.
    /// </summary>
    private static LatticeSaturatedException WithoutUnappliedKeys(LatticeSaturatedException refusal)
        => new(RootMessage(refusal), refusal.TreeId, refusal.SaturationSource, refusal);

    /// <summary>
    /// Publishes the <c>Set</c> events of the keys a refused batch did land,
    /// before the refusal reaches the caller (issue #5970).
    /// </summary>
    /// <remarks>
    /// A key left out of <see cref="LatticeSaturatedException.UnappliedKeys"/>
    /// is not re-sent by a caller that retries only the listed keys, so its
    /// event is published here or never. It persisted, so publishing it keeps
    /// the rule that a subscriber only sees a <c>Set</c> for a key that
    /// persisted. Publication is best-effort and never faults, exactly as on the
    /// success path.
    /// </remarks>
    /// <param name="entries">The refused batch.</param>
    /// <param name="unapplied">The refusal's unapplied keys.</param>
    private async Task PublishLandedSetEventsAsync(
        List<KeyValuePair<string, byte[]>> entries, IReadOnlyList<string> unapplied)
    {
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, Options))
            return;

        var skip = new HashSet<string>(unapplied, StringComparer.Ordinal);
        var landed = entries.FindAll(e => !skip.Contains(e.Key));
        if (landed.Count == 0)
            return;

        var batch = LatticeEventPublisher.CreateBatch(services, Options, TreeId, logger);
        await LatticeEventPublisher.PublishManyAsync(
            batch, LatticeTreeEventKind.Set, landed, static entry => entry.Key);
    }

    /// <summary>
    /// The refusal's own description, without the unapplied-key count a shard
    /// already appended, so the count is stated once, for the whole batch.
    /// </summary>
    private static string RootMessage(LatticeSaturatedException refusal)
        => refusal.UnappliedKeys is not null && refusal.InnerException is LatticeSaturatedException root
            ? root.Message
            : refusal.Message;
}
