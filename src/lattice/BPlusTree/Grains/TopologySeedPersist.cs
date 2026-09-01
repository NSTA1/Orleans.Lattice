using Microsoft.Extensions.Logging;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Shared persistence helper for the B+ tree topology-seed grains -
/// <see cref="BPlusLeafGrain"/>, <see cref="BPlusInternalGrain"/>, and
/// <see cref="ShardRootGrain"/> - that converges a benign cold-start
/// first-create write race instead of aborting the run.
/// </summary>
/// <remarks>
/// <para>
/// On a fresh volume / cold silo the grain-directory warmup can transiently
/// materialise two activations of the same deterministic grain id. Both issue
/// their first <c>WriteStateAsync</c> as an
/// insert with a null/empty expected etag, and the loser of the storage insert
/// compare-and-swap throws <see cref="InconsistentStateException"/> with
/// <em>both</em> etags empty. A per-activation gate cannot serialise that race -
/// it is cross-activation - so the loser would otherwise fail the cold-start
/// apply with a spurious fail-level abort that only self-heals on a retry
/// (#1557 for the leaf; #1566 extends the same convergence to the internal-node
/// and shard-root topology seeds).
/// </para>
/// <para>
/// The adopt is safe because it is scoped, by construction, to a genuine
/// insert-vs-insert race on a brand-new row: the <c>creatingRow</c> flag is
/// captured from <c>RecordExists</c> <em>before</em> the write, so this
/// activation never read an existing row. A stale-state conflict on an existing
/// row therefore still surfaces, preserving the #1560 fall-off-the-log fail-loud
/// contract.
/// </para>
/// <para>
/// <b>The etag test is a belt-and-braces check, not the discriminator - do not
/// rely on it.</b> This helper also requires
/// <see cref="InconsistentStateException.StoredEtag"/> and
/// <see cref="InconsistentStateException.CurrentEtag"/> to be empty, and the
/// original rationale held that a conflict on an existing row "can never be"
/// empty/empty because it carries non-empty etags. <b>That is not true of every
/// provider.</b> Measured against <c>AdoNetGrainStorage</c> on a real
/// deployment, <b>0 of 134</b> version conflicts carried a non-empty etag -
/// including conflicts on rows that plainly existed (<c>ETag=245</c>,
/// <c>ETag=164</c>, <c>ETag=7718</c>) - because that provider raises the
/// conflict with the message-only constructor and never populates either
/// property. So with AdoNet the etag clause is vacuously true and
/// <c>creatingRow</c> alone does the discriminating.
/// </para>
/// <para>
/// The guard is still correct, because <c>creatingRow</c> is the load-bearing
/// condition and it is evaluated from this activation's own read. But the etag
/// clause must not be treated as a second, independent safety net, and it must
/// not be relaxed on the assumption that the etags carry information: on a
/// provider that leaves them empty, removing <c>creatingRow</c> would silently
/// widen this catch to every conflict, including the stale-state ones #1560
/// requires to fail loudly.
/// </para>
/// <para>
/// The only writers of a topology grain's deterministic id are the shard root
/// seeding the same tree (and the split/bulk-load paths that create nodes with a
/// single logical identity), and data mutations are gated behind the topology
/// seed, so the very first state-row write is always the idempotent topology
/// seed. Two first-writers to the same id therefore carry identical seed
/// content, and the winner's durably-committed row already satisfies the seed
/// this activation intended: re-read to adopt it and converge.
/// </para>
/// <para>
/// The benign catch is deliberately the <em>innermost</em> guard around the
/// write: a caller that wraps this helper in its own snapshot/revert
/// <c>catch { ...; throw; }</c> still reverts on every non-benign failure
/// (those rethrow through this helper unchanged), while the benign empty/empty
/// first-create is swallowed and adopted before the caller's revert can undo the
/// seed it was trying to persist.
/// </para>
/// </remarks>
internal static class TopologySeedPersist
{
    /// <summary>
    /// Writes <paramref name="state"/>, converging a benign cold-start
    /// first-create write race by re-reading and adopting the
    /// concurrently-committed row. Every other failure - including a stale-state
    /// conflict on an existing row - is rethrown unchanged.
    /// <para>
    /// <c>creatingRow</c> is the load-bearing condition; see the type remarks for
    /// why the etag clause beside it is vacuous on <c>AdoNetGrainStorage</c> and
    /// must not be relied on as an independent check.
    /// </para>
    /// </summary>
    /// <typeparam name="TState">The persisted grain-state POCO.</typeparam>
    /// <param name="state">The grain's persistent state.</param>
    /// <param name="logger">
    /// Optional logger for the best-effort convergence debug line; may be
    /// <see langword="null"/>.
    /// </param>
    /// <param name="grainId">The grain id, for the convergence debug line.</param>
    public static async Task WriteAdoptingBenignFirstCreateRaceAsync<TState>(
        IPersistentState<TState> state,
        ILogger? logger,
        GrainId grainId)
    {
        // #1557 / #1566: capture whether this write is an initial create BEFORE
        // the attempt. This flag - NOT the etag test below - is what makes
        // adopting the winner's row safe: see the type remarks for the measured
        // evidence that AdoNetGrainStorage never populates either etag, so that
        // clause is vacuously true and cannot discriminate a create race from a
        // stale-state conflict. Never relax this condition.
        var creatingRow = !state.RecordExists;
        try
        {
            await state.WriteStateAsync();
        }
        catch (InconsistentStateException ex)
            when (creatingRow
                && string.IsNullOrEmpty(ex.StoredEtag)
                && string.IsNullOrEmpty(ex.CurrentEtag))
        {
            await state.ReadStateAsync();
            logger?.LogDebug(
                "Topology grain {GrainId} converged a benign first-create write race (#1557/#1566) by adopting the concurrently-committed row.",
                grainId);
        }
    }
}
