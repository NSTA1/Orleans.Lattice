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
/// insert-vs-insert race on a brand-new row:
/// <list type="bullet">
///   <item><description>
///     the <c>creatingRow</c> flag is captured from
///     <c>RecordExists</c> <em>before</em> the
///     write, so this activation never read an existing row; and
///   </description></item>
///   <item><description>
///     both <see cref="InconsistentStateException.StoredEtag"/> and
///     <see cref="InconsistentStateException.CurrentEtag"/> are empty, which a
///     stale-state conflict on an <em>existing</em> row can never be (it carries
///     non-empty etags and must still surface, preserving the #1560
///     fall-off-the-log fail-loud contract).
///   </description></item>
/// </list>
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
    /// first-create write race (both etags empty on a brand-new row) by
    /// re-reading and adopting the concurrently-committed row. Every other
    /// failure - including a stale-state conflict on an existing row - is
    /// rethrown unchanged.
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
        // the attempt. See the type remarks for why the empty/empty guard makes
        // adopting the winner's row provably safe.
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
