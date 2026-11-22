using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Core;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;
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
/// <para>
/// <b>A write that landed but was reported as a conflict is adopted too
/// (#5095).</b> A storage client that transparently retries a request whose
/// first attempt committed sees its own new etag on the retry and reports an
/// etag conflict, although the row already holds exactly what this activation
/// wrote. Surfacing that conflict deactivated the grain in the middle of a split
/// and stranded the stale etag on the next checkpoint barrier, which is how a
/// large ascending <c>SetManyAsync</c> wedged behind a long WAL replay. Every
/// write through this helper therefore re-reads the stored row on a conflict and
/// adopts its etag when, and only when, the row serialises byte-for-byte equal to
/// the state this activation wrote. Any difference - a genuine competing writer
/// or a stale activation - rethrows the original conflict, so the #1560
/// fail-loud contract is unchanged; the check can only err towards surfacing.
/// </para>
/// </remarks>
internal static class TopologySeedPersist
{
    /// <summary>The persistent-state name of <see cref="BPlusLeafGrain"/>.</summary>
    public const string LeafStateName = "leaf";

    /// <summary>The persistent-state name of <see cref="BPlusInternalGrain"/>.</summary>
    public const string InternalStateName = "internal";

    /// <summary>The persistent-state name of <see cref="ShardRootGrain"/>.</summary>
    public const string ShardRootStateName = "shardroot";

    private static string? StateNameOf<TState>() =>
        typeof(TState) == typeof(LeafNodeState) ? LeafStateName
        : typeof(TState) == typeof(InternalNodeState) ? InternalStateName
        : typeof(TState) == typeof(ShardRootState) ? ShardRootStateName
        : null;

    /// <summary>
    /// Writes <paramref name="state"/>, converging a benign cold-start
    /// first-create write race by re-reading and adopting the
    /// concurrently-committed row, and adopting a write that landed but was
    /// reported as an etag conflict (#5095). Every other failure - including a
    /// stale-state conflict on an existing row - is rethrown unchanged.
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
    /// <param name="context">
    /// The grain's context, which supplies the grain id for the convergence debug
    /// line and the services the landed-write check reads the committed row with.
    /// </param>
    public static Task WriteAdoptingBenignFirstCreateRaceAsync<TState>(
        IPersistentState<TState> state,
        ILogger? logger,
        IGrainContext context)
        => WriteCoreAsync(state, logger, context, adoptBenignFirstCreate: true);

    /// <summary>
    /// Writes <paramref name="state"/>, recovering only a write that landed but
    /// was reported as an etag conflict (issue #5095). Every other failure -
    /// including a benign first-create race and a genuine stale-state conflict -
    /// is rethrown unchanged.
    /// </summary>
    /// <typeparam name="TState">The persisted grain-state POCO.</typeparam>
    /// <param name="state">The grain's persistent state.</param>
    /// <param name="logger">Optional logger; may be <see langword="null"/>.</param>
    /// <param name="context">The grain's context.</param>
    public static Task WriteRecoveringLandedConflictAsync<TState>(
        IPersistentState<TState> state,
        ILogger? logger,
        IGrainContext context)
        => WriteCoreAsync(state, logger, context, adoptBenignFirstCreate: false);

    private static async Task WriteCoreAsync<TState>(
        IPersistentState<TState> state,
        ILogger? logger,
        IGrainContext context,
        bool adoptBenignFirstCreate)
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
            when (adoptBenignFirstCreate
                && creatingRow
                && string.IsNullOrEmpty(ex.StoredEtag)
                && string.IsNullOrEmpty(ex.CurrentEtag))
        {
            await state.ReadStateAsync();
            logger?.LogDebug(
                "Topology grain {GrainId} converged a benign first-create write race (#1557/#1566) by adopting the concurrently-committed row.",
                context.GrainId);
        }
        catch (InconsistentStateException ex)
        {
            if (!await TryAdoptLandedWriteAsync(state, context, creatingRow))
            {
                ExceptionDispatchInfo.Capture(ex).Throw();
            }

            LatticeMetrics.LandedWriteConflictsAdopted.Add(1, LatticeTenantLabel.ForTree(TreeIdOf(state.State)));
            logger?.LogWarning(
                "Topology grain {GrainId} adopted a write that landed but was reported as an etag conflict (#5095), for example by a storage-client retry of a committed request.",
                context.GrainId);
        }
    }

    private static string? TreeIdOf<TState>(TState state) => state switch
    {
        LeafNodeState leaf => leaf.TreeId,
        InternalNodeState node => node.TreeId,
        _ => null,
    };

    /// <summary>
    /// Issue #5095: a storage client that retries a request which already
    /// committed (the Azure SDK does so on a timeout) reports an etag conflict
    /// for a write that landed. Rethrowing it deactivates the grain mid-call and
    /// leaves every interleaved turn holding a stale etag, so a single transient
    /// fault wedges the tree. This re-reads the committed row without touching
    /// <see cref="IPersistentState{TState}.State"/> and, only when it is
    /// byte-identical to the state just written, adopts its etag.
    /// </summary>
    /// <remarks>
    /// The check is conservative: any doubt - no storage service, a row that
    /// differs (a genuine stale-state conflict, or in-memory state that moved on
    /// since the write began), or a failure reading it - answers
    /// <see langword="false"/>, so the conflict surfaces exactly as before and the
    /// #1560 fail-loud contract holds. Equal bytes mean the durable row already
    /// holds what this write intended, so adopting it loses nothing.
    /// </remarks>
    private static async Task<bool> TryAdoptLandedWriteAsync<TState>(
        IPersistentState<TState> state,
        IGrainContext context,
        bool creatingRow)
    {
        try
        {
            if (state is not StateStorageBridge<TState> bridge)
            {
                return false;
            }

            var services = context.ActivationServices;
            var serializer = services.GetService<Serializer>();
            var storage = services.GetKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName)
                ?? services.GetService<IGrainStorage>();
            var stateName = StateNameOf<TState>();
            if (serializer is null || storage is null || stateName is null)
            {
                return false;
            }

            var committed = new GrainState<TState>();
            await storage.ReadStateAsync(stateName, context.GrainId, committed);
            if (!committed.RecordExists || string.IsNullOrEmpty(committed.ETag))
            {
                return false;
            }

            var intended = serializer.SerializeToArray(state.State);
            if (!intended.AsSpan().SequenceEqual(serializer.SerializeToArray(committed.State)))
            {
                return false;
            }

            if (creatingRow)
            {
                // RecordExists has no public setter on the bridge, so a create
                // adopts the identical row the way the #1557 path does.
                await state.ReadStateAsync();
            }
            else
            {
                bridge.Etag = committed.ETag;
            }

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
