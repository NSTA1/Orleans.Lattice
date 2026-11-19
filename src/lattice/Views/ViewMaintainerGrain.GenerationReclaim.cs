using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Views;

/// <summary>
/// Reclamation of superseded view-tree generations (issue #6097).
/// <para>
/// A shadow-swap rebuild leaves the generation it replaced registered. Every
/// generation below the active one is retired through the ordinary tree-deletion
/// path - <see cref="ILattice.DeleteTreeAsync"/> - which stops the tree's
/// shard-healing orchestrator and hot-shard monitor, unregisters its tombstone
/// compaction, and schedules the purge that reclaims its shards and write-ahead
/// log once <see cref="LatticeOptions.SoftDeleteDuration"/> has elapsed. An
/// earlier release only deleted a superseded generation's keys, so every rebuild
/// left an empty tree behind that kept its full per-tree background workload.
/// </para>
/// <para>
/// <b>One sweep covers both.</b> The durable
/// <see cref="ViewCheckpointState.ReclaimedGenerationFloor"/> walks upward from
/// generation 0 to the active generation, so the generation a swap has just
/// replaced and every generation an earlier release left behind are retired by
/// the same code, oldest first. The most recently replaced generation is held
/// back until its post-swap reader grace has elapsed. The shadow generation
/// above the active one is never touched.
/// </para>
/// <para>
/// <b>Gradual.</b> A pass starts only when the silo-wide
/// <see cref="ViewGenerationReclaimPacer"/> admits it, deletes at most one tree,
/// and probes at most <see cref="MaxReclaimProbesPerPass"/> generations; nothing
/// is buffered, so its memory does not grow with the size or number of the
/// generations being retired. A deletion that fails is retried on a bounded
/// exponential back-off and never fails the drain that attempted it.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    /// <summary>The most generations one reclamation pass inspects.</summary>
    internal const int MaxReclaimProbesPerPass = 16;

    private static readonly TimeSpan ReclaimRetryInitialDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ReclaimRetryMaxDelay = TimeSpan.FromMinutes(30);

    private ViewGenerationReclaimPacer? _reclaimPacer;
    private int _reclaimFailures;
    private long _reclaimRetryAfterTicks;

    /// <summary>
    /// The silo-wide pacer this maintainer draws reclamation passes from. Falls
    /// back to a pacer private to this activation when the silo registered none,
    /// which still spaces this view's own passes. Settable for tests.
    /// </summary>
    internal ViewGenerationReclaimPacer ReclaimPacer
    {
        get => _reclaimPacer ??= context.ActivationServices.GetService<ViewGenerationReclaimPacer>()
            ?? new ViewGenerationReclaimPacer();
        set => _reclaimPacer = value;
    }

    /// <summary>
    /// Retires superseded generations below the active one, oldest first, under
    /// the pacing described on this partial. Runs on the drain and reconcile
    /// cadence; a call with nothing to do returns without any I/O.
    /// </summary>
    internal async Task TryReclaimSupersededGenerationsAsync(CancellationToken cancellationToken)
    {
        var nowTicks = DateTime.UtcNow.Ticks;
        var floor = state.State.ReclaimedGenerationFloor ?? 0L;
        var ceiling = ReclaimCeiling(nowTicks);
        if (floor >= ceiling)
        {
            await SettleUnreclaimablePendingAsync(floor);
            return;
        }

        // A submitted joint repair owns its view generation; nothing it might
        // still address is retired until its checkpoint does.
        if (HasJointOwnership || nowTicks < _reclaimRetryAfterTicks || !ReclaimPacer.TryAdmit())
        {
            return;
        }

        var next = floor;
        try
        {
            for (var probes = 0; next < ceiling && probes < MaxReclaimProbesPerPass; probes++)
            {
                var deleted = await ReclaimGenerationAsync(next, cancellationToken);
                next++;
                if (deleted)
                {
                    break;
                }
            }

            _reclaimFailures = 0;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var delay = ReclaimRetryDelay(++_reclaimFailures);
            _reclaimRetryAfterTicks = nowTicks + delay.Ticks;
            logger.LogWarning(
                ex,
                "View '{ViewName}' could not reclaim superseded generation {Generation}; retrying in {Delay}.",
                ViewName,
                next,
                delay);
        }

        if (next != floor)
        {
            await AdvanceReclaimedGenerationFloorAsync(next);
        }
    }

    /// <summary>
    /// The exclusive upper bound of the generations reclaimable now: the active
    /// generation, or the most recently replaced one while its reader grace has
    /// not yet elapsed.
    /// </summary>
    private long ReclaimCeiling(long nowTicks)
    {
        var active = state.State.ActiveGeneration;
        if (state.State.HasPendingReclaim
            && nowTicks < state.State.ReclaimEligibleAtTicks
            && state.State.PendingReclaimGeneration < active)
        {
            return state.State.PendingReclaimGeneration;
        }

        return active;
    }

    /// <summary>
    /// Retires one generation's tree if it is still live, returning whether a
    /// deletion was issued. A generation that was never registered, or whose
    /// deletion is already recorded, needs nothing more: its purge is driven by
    /// its own deletion grain.
    /// </summary>
    private async Task<bool> ReclaimGenerationAsync(long generation, CancellationToken cancellationToken)
    {
        var treeId = GenerationTreeId(generation);

        // A point read through the silo's bounded registry path. Deliberately not
        // a call through ILattice: activating the router of an id that has
        // already been purged would arm its autonomic loops again.
        if (await optionsResolver.RegistryReads.GetEntryAsync(treeId) is null)
        {
            return false;
        }

        if (await grainFactory.GetGrain<ITreeDeletionGrain>(treeId).HoldsDeletionAsync())
        {
            return false;
        }

        // The ordinary tree-deletion path, authorised by the view-write scope
        // every caller of this method holds.
        await grainFactory.GetGrain<ILattice>(treeId).DeleteTreeAsync(cancellationToken);
        logger.LogInformation(
            "View '{ViewName}' deleted superseded generation {Generation} ('{TreeId}'); its shards and log are purged after the soft-delete window.",
            ViewName,
            generation,
            treeId);
        return true;
    }

    private async Task AdvanceReclaimedGenerationFloorAsync(long floor)
    {
        var previousFloor = state.State.ReclaimedGenerationFloor;
        var hadPending = state.State.HasPendingReclaim;
        var pendingGeneration = state.State.PendingReclaimGeneration;
        var eligibleAt = state.State.ReclaimEligibleAtTicks;

        state.State.ReclaimedGenerationFloor = floor;
        if (hadPending && pendingGeneration < floor)
        {
            ClearPendingReclaim();
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.ReclaimedGenerationFloor = previousFloor;
            state.State.HasPendingReclaim = hadPending;
            state.State.PendingReclaimGeneration = pendingGeneration;
            state.State.ReclaimEligibleAtTicks = eligibleAt;
            throw;
        }
    }

    /// <summary>
    /// Drops a pending-reclaim record the sweep can never act on: one the floor
    /// has already passed, or one naming the active or a later generation.
    /// </summary>
    private async Task SettleUnreclaimablePendingAsync(long floor)
    {
        if (!state.State.HasPendingReclaim
            || (state.State.PendingReclaimGeneration >= floor
                && state.State.PendingReclaimGeneration < state.State.ActiveGeneration))
        {
            return;
        }

        var pendingGeneration = state.State.PendingReclaimGeneration;
        var eligibleAt = state.State.ReclaimEligibleAtTicks;
        ClearPendingReclaim();
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.HasPendingReclaim = true;
            state.State.PendingReclaimGeneration = pendingGeneration;
            state.State.ReclaimEligibleAtTicks = eligibleAt;
            throw;
        }
    }

    private void ClearPendingReclaim()
    {
        state.State.HasPendingReclaim = false;
        state.State.PendingReclaimGeneration = 0;
        state.State.ReclaimEligibleAtTicks = 0;
    }

    /// <summary>
    /// The back-off after <paramref name="failures"/> consecutive failed passes:
    /// doubling from <see cref="ReclaimRetryInitialDelay"/> and saturating at
    /// <see cref="ReclaimRetryMaxDelay"/>.
    /// </summary>
    internal static TimeSpan ReclaimRetryDelay(int failures)
    {
        var shift = Math.Clamp(failures - 1, 0, 16);
        var ticks = ReclaimRetryInitialDelay.Ticks << shift;
        return ticks >= ReclaimRetryMaxDelay.Ticks ? ReclaimRetryMaxDelay : TimeSpan.FromTicks(ticks);
    }
}
