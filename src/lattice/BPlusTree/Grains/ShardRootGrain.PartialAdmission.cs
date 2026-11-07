using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Partial admission of a batch write (issue #5970): the failure path of the
/// per-leaf <c>SetManyAsync</c> fan-out, which reports the refused leaves' keys
/// so a caller can retry only those.
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Attributes a per-leaf batch fan-out that did not complete, after first
    /// linking the splits of the leaves that did.
    /// </summary>
    /// <param name="orderedBuckets">The dispatched leaf buckets, in dispatch order.</param>
    /// <param name="dispatched">The per-leaf dispatches, all completed.</param>
    /// <param name="batchCount">The number of entries in the shard's batch.</param>
    /// <returns>
    /// A refusal listing every key of every refused leaf, or
    /// <see langword="null"/> to rethrow the original fault unchanged.
    /// </returns>
    /// <remarks>
    /// <para>
    /// <b>Superset.</b> A leaf that returned success applied its whole slice; a
    /// leaf that faulted is listed whole. So a key is left out only when its leaf
    /// returned success for it. The list is reported only when every faulted
    /// leaf was refused for saturation. Any other fault, or a cancelled dispatch,
    /// keeps today's rethrow.
    /// </para>
    /// <para>
    /// <b>Post-commit obligations.</b> On success this shard forwards each key to
    /// an adaptive split's target and mirrors it to an online resize's
    /// destination, and an atomic saga's prepare is recorded against its leaves.
    /// None of that runs on this path. So while any of them could apply, the
    /// refusal reports no list, and the caller retries the whole batch as it
    /// always has: that retry is what carries those keys to their forward and
    /// mirror.
    /// </para>
    /// <para>
    /// <b>Split linking (P5).</b> The splits of the leaves that committed are
    /// linked here, by the same propagation the success path runs, rather than
    /// waiting for the durable pending-link recovery. A fault while linking is
    /// logged and left to that recovery, and never replaces the refusal. The
    /// propagation is not timed out here: abandoning it mid-way would leave a
    /// routing mutation running under a later turn. The caller's wait stays
    /// bounded by the batch's envelope and fan-out budgets, which race this whole
    /// shard call.
    /// </para>
    /// </remarks>
    private async Task<LatticeSaturatedException?> AttributeLeafBatchRefusalAsync(
        List<(GrainId LeafId, LeafBucket Bucket)> orderedBuckets,
        Task<SplitResult?>[] dispatched,
        int batchCount)
    {
        for (var i = 0; i < dispatched.Length; i++)
        {
            if (!dispatched[i].IsCompletedSuccessfully || dispatched[i].Result is not { } split)
                continue;

            try
            {
                var residual = await PropagateSplitAsync(split, orderedBuckets[i].Bucket.Parents);
                while (residual is not null)
                {
                    residual = await PromoteRootAsync(residual);
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Shard {ShardIndex} of tree {TreeId}: linking leaf {LeafId}'s split failed while its batch was "
                    + "refused; the durable pending-link recovery links it (#5970).",
                    MyShardIndex,
                    TreeId,
                    orderedBuckets[i].LeafId);
            }
        }

        if (LatticePreparedContext.Current
            || state.State.SplitInProgress is not null
            || state.State.MovedAwaySlots.Count != 0
            || TryGetShadowTarget() is not null)
        {
            return null;
        }

        LatticeSaturatedException? first = null;
        var unapplied = new List<string>();
        for (var i = 0; i < dispatched.Length; i++)
        {
            var task = dispatched[i];
            if (task.IsCompletedSuccessfully)
                continue;

            if (task.Exception is not { InnerExceptions: [LatticeSaturatedException refused] })
                return null;

            first ??= refused;
            foreach (var entry in orderedBuckets[i].Bucket.Slice)
            {
                unapplied.Add(entry.Key);
            }
        }

        if (first is null)
            return null;

        return new LatticeSaturatedException(
            first.Message,
            first.TreeId,
            first.SaturationSource,
            unapplied,
            batchCount,
            first);
    }
}
