using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class LatticeRegistryGrain
{
    private const string AliasRecoveryReminderName = "alias-routing-recovery";
    private readonly Dictionary<string, AliasRoutingMoveState> _aliasMoves = [];
    private Dictionary<string, AliasRoutingMoveState> AliasMoves => aliasRoutingState?.State ?? _aliasMoves;
    private IGrainTimer? _aliasRecoveryTimer;
    private bool _aliasReminderRegistered;
    IGrainContext IGrainBase.GrainContext => context
        ?? throw new InvalidOperationException("Registry lifecycle requires an Orleans grain context.");

    Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        if (AliasMoves.Count > 0) StartAliasRecoveryTimer();
        return Task.CompletedTask;
    }

    Task IGrainBase.OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _aliasRecoveryTimer?.Dispose();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == AliasRecoveryReminderName)
            await RecoverAliasMovesAsync(CancellationToken.None);
    }

    private void StartAliasRecoveryTimer()
    {
        if (context is null || _aliasRecoveryTimer is not null) return;
        _aliasRecoveryTimer = this.RegisterGrainTimer(
            RecoverAliasMovesAsync, new GrainTimerCreationOptions(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1))
            {
                Interleave = false,
            });
    }

    private async Task PersistAliasMovesAsync()
    {
        if (aliasRoutingState is not null)
        {
            try
            {
                await aliasRoutingState.WriteStateAsync();
            }
            catch
            {
                // A lost storage acknowledgement can also leave a stale etag.
                // The reminder reloads intent in a fresh activation.
                context?.Deactivate(new DeactivationReason(DeactivationReasonCode.ApplicationRequested,
                    "Alias routing persistence failed; reload durable recovery state."));
                throw;
            }
        }
    }

    private async Task MoveAliasRoutingAsync(
        string treeId, string current, string destination, ShardMap sourceMap, ShardMap destinationMap,
        TreeRegistryEntry? before, TreeRegistryEntry after)
    {
        if (context is not null && aliasRoutingState is null)
            throw new InvalidOperationException("Alias routing requires persistent recovery state.");
        // Register the durable wake-up before writing intent or touching shards.
        // A process death at any later point is re-driven without caller retry.
        if (context is not null && !_aliasReminderRegistered)
        {
            await (reminderRegistry ?? throw new InvalidOperationException("Alias routing requires a reminder registry."))
                .RegisterOrUpdateReminder(context.GrainId, AliasRecoveryReminderName, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            _aliasReminderRegistered = true;
        }
        var operationId = $"alias:{treeId}->{destination}:{Guid.NewGuid():N}";
        var move = new AliasRoutingMoveState
        {
            TreeId = treeId, Source = current, Destination = destination,
            SourceMap = sourceMap, DestinationMap = destinationMap, Before = before,
            After = after with { AliasRoutingOperationId = operationId }, OperationId = operationId,
        };
        AliasMoves.Add(treeId, move);
        StartAliasRecoveryTimer();
        await PersistAliasMovesAsync();
        await ExecuteAliasMoveAsync(move);
    }

    private async Task RecoverAliasMoveAsync(string treeId)
    {
        if (AliasMoves.TryGetValue(treeId, out var move))
        {
            using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
            await PersistAliasMovesAsync();
            await ExecuteAliasMoveAsync(move, recheckAdmission: true);
        }
    }

    private async Task RecoverAliasMovesAsync(CancellationToken cancellationToken)
    {
        foreach (var treeId in AliasMoves.Keys.ToArray())
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await RecoverAliasMoveAsync(treeId);
            }
            catch (Exception failure)
            {
                logger?.LogError(failure, "Alias routing recovery for {TreeId} failed; durable intent will be retried.", treeId);
            }
        }
        if (AliasMoves.Count == 0)
        {
            _aliasRecoveryTimer?.Dispose();
            _aliasRecoveryTimer = null;
        }
    }

    private async Task ExecuteAliasMoveAsync(AliasRoutingMoveState move, bool recheckAdmission = false)
    {
        var row = await GetEntryCoreAsync(move.TreeId);
        var effective = row?.PhysicalTreeId ?? move.TreeId;
        if (row?.AliasRoutingOperationId == move.OperationId && effective == move.Destination)
        {
            await CompletePublishedAliasMoveAsync(move);
            return;
        }
        if (effective != move.Source || row?.AliasRoutingOperationId != move.Before?.AliasRoutingOperationId
            || (row?.ShardMap?.Version ?? 0L) != (move.Before?.ShardMap?.Version ?? 0L))
        {
            // Another publication owns the row: never restore or overwrite it.
            logger?.LogWarning("Alias routing move {OperationId} was superseded; preserving the newer row.", move.OperationId);
            if (effective == move.Source)
                await RollBackSourceFencesAsync(move);
            await ForgetAliasMoveAsync(move);
            return;
        }
        try
        {
            if (recheckAdmission)
            {
                if (move.Destination == move.TreeId)
                    await grainFactory.GetGrain<ITreeDeletionGrain>(move.TreeId).EnsureAliasWritableAsync();
                else
                    await EnsureAliasTargetAdmissibleAsync(move.TreeId, move.Destination);
            }
            await AliasCutoverShardMaps.ArmRedirectsAsync(
                grainFactory, move.Source, move.SourceMap, move.Destination, move.TreeId, move.OperationId, CancellationToken.None);
            var published = (row ?? move.After) with
            {
                PhysicalTreeId = move.After.PhysicalTreeId,
                ShardMap = move.After.ShardMap,
                NextShardIndex = move.After.NextShardIndex,
                UnaliasedShardMap = move.After.UnaliasedShardMap,
                UnaliasedNextShardIndex = move.After.UnaliasedNextShardIndex,
                AliasCutoverTarget = move.After.AliasCutoverTarget,
                Lineage = move.After.Lineage,
                AliasRoutingOperationId = move.OperationId,
            };
            await UpdateAsync(move.TreeId, published);
        }
        catch (Exception failure)
        {
            var observed = await GetEntryCoreAsync(move.TreeId);
            if (observed?.AliasRoutingOperationId == move.OperationId
                && (observed.PhysicalTreeId ?? move.TreeId) == move.Destination)
            {
                // Publication committed. Fresh routers may already have accepted
                // writes there, so only forward completion is safe.
                logger?.LogWarning(failure, "Alias publication {OperationId} committed despite an error; completing forward.", move.OperationId);
                await CompletePublishedAliasMoveAsync(move);
                return;
            }
            if ((observed?.PhysicalTreeId ?? move.TreeId) == move.Source
                && observed?.AliasRoutingOperationId == move.Before?.AliasRoutingOperationId)
            {
                try
                {
                    await RollBackSourceFencesAsync(move);
                    await ForgetAliasMoveAsync(move);
                }
                catch (Exception rollbackFailure)
                {
                    throw new AggregateException("Alias routing failed and rollback requires durable recovery.", failure, rollbackFailure);
                }
            }
            throw;
        }
        await CompletePublishedAliasMoveAsync(move);
    }

    private Task RollBackSourceFencesAsync(AliasRoutingMoveState move) =>
        Task.WhenAll(move.SourceMap.GetPhysicalShardIndices().Select(index => grainFactory
            .GetGrain<IShardRootGrain>($"{move.Source}/{index}")
            .ClearRetainedRedirectIfOwnedAsync(move.OperationId)));

    private async Task CompletePublishedAliasMoveAsync(AliasRoutingMoveState move)
    {
        await AliasCutoverShardMaps.ReleaseRedirectsAsync(
            grainFactory, move.Destination, move.DestinationMap, move.TreeId, CancellationToken.None);
        await PublishAliasChangeAsync(move.TreeId, move.Source, move.Destination);
        await ForgetAliasMoveAsync(move);
    }

    private async Task ForgetAliasMoveAsync(AliasRoutingMoveState move)
    {
        AliasMoves.Remove(move.TreeId);
        try
        {
            await PersistAliasMovesAsync();
        }
        catch
        {
            AliasMoves[move.TreeId] = move;
            throw;
        }
    }
}
