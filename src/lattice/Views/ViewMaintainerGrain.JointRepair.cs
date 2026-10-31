using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Views;

internal sealed partial class ViewMaintainerGrain
{
    private IReadOnlyDictionary<int, long> _jointConsumed = new Dictionary<int, long>();
    private HashSet<Guid> _jointEligible = [];
    private bool _jointRepairRestartRead;
    private bool _jointStateWritePending;
    private bool _jointAdmissionWaiting;
    private IViewJointRepairAdmissionGate JointRepairAdmissionGate =>
        context.ActivationServices.GetRequiredService<IViewJointRepairAdmissionGate>();
    private readonly HashSet<string> _jointRepairsWaitingOnPeers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (ViewCrossTreeReadiness Slice, long[] Heads, HashSet<string> Keys, Dictionary<string, HybridLogicalClock> Versions)> _repairCaptures = new(StringComparer.Ordinal);

    private bool HasJointOwnership => state.State.CrossTreeRepairHolds.Count != 0;
    private bool HasCommittedJointOwnership => state.State.CrossTreeRepairHolds.Values.Any(static hold => hold.OriginalApplied);
    private bool HasPendingJointRepair => state.State.CrossTreeRepairHolds.Values.Any(static hold => !hold.RepairApplied);

    private async Task<bool> PauseJointProtocolForUpgradeAsync(CancellationToken cancellationToken)
    {
        if ((_jointAdmissionWaiting || HasJointOwnership
            || state.State.PendingCrossTreeRepairAcknowledgements.Count != 0)
            && !await JointRepairAdmissionGate.IsOpenAsync(cancellationToken))
        {
            return true;
        }

        _jointAdmissionWaiting = false;
        return false;
    }

    private async Task GuardJointMaintenanceAdmissionAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (await PauseJointProtocolForUpgradeAsync(cancellationToken))
        {
            throw new InvalidOperationException(
                $"View '{ViewName}' cannot perform maintenance while joint protocol admission is closed; its pending checkpoint and generation remain retained.");
        }
    }

    private async Task<bool> ResumeCapturedJointRepairsAsync(CancellationToken cancellationToken)
    {
        _jointRepairsWaitingOnPeers.Clear();
        foreach (var (operationId, hold) in state.State.CrossTreeRepairHolds)
        {
            if (!hold.OriginalApplied || hold.RepairApplied)
            {
                continue;
            }

            var coordinator = grainFactory.GetGrain<IViewCrossTreeCoordinatorGrain>(operationId);
            await CompleteJointRepairAsync(coordinator, hold, _staging.GetValueOrDefault(hold.TransactionId), cancellationToken);
        }

        return HasPendingJointRepair && state.State.CrossTreeRepairHolds.All(pair =>
            pair.Value.RepairApplied || _jointRepairsWaitingOnPeers.Contains(pair.Key));
    }

    private void GuardJointStagingBudget(LatticeViewOptions options)
    {
        if (!HasCommittedJointOwnership)
        {
            return;
        }

        var maxTx = options.MaxStagedTransactions > 0 ? options.MaxStagedTransactions : LatticeViewOptions.DefaultMaxStagedTransactions;
        var maxBytes = options.MaxStagedBytes > 0 ? options.MaxStagedBytes : LatticeViewOptions.DefaultMaxStagedBytes;
        if (_staging.Count > maxTx || _staging.Values.Sum(static tx => tx.StagedBytes) > maxBytes)
        {
            throw new InvalidOperationException(
                $"View '{ViewName}' cannot read more WAL within its joint-repair staging budget. The submitted joint identity remains held; recover its peers or increase the staging budget.");
        }
    }

    private void GuardJointProjectionVersion(ViewRegistration registration)
    {
        if (state.State.CrossTreeRepairHolds.Values.Any(hold =>
            !string.Equals(hold.ProjectionVersion, registration.ProjectionVersion, StringComparison.Ordinal)
            || !string.Equals(hold.SourceTreeId, registration.SourceTreeId, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"View '{ViewName}' cannot change its source or projection while a submitted joint repair owns its generation.");
        }
    }

    private void SetJointReadHorizon(
        IReadOnlyDictionary<int, long> readFrom,
        IReadOnlyDictionary<int, long> advanced,
        List<Guid> completed)
    {
        _jointConsumed = ConsumedThrough(readFrom, advanced);
        _jointEligible = [.. completed];
    }

    private void RestoreRepairedTransactions(string walTreeId)
    {
        // Initialise the log-scoped dedup before restoring durable repaired ids.
        _ = DrainReadCheckpoints(walTreeId, state.State.AppliedOffsets);
        foreach (var hold in state.State.CrossTreeRepairHolds.Values)
        {
            if (hold.RepairApplied)
            {
                MarkResolved(hold.TransactionId);
            }
        }
    }

    private bool DeferPublicationForJointRepair(
        string walTreeId,
        IReadOnlyDictionary<int, long> readFrom,
        IReadOnlyDictionary<int, long> advanced)
    {
        if (_jointRepairRestartRead)
        {
            // Earlier passes scanned ordinary work without publishing it. Replay
            // that work from the durable checkpoint, not from the scan frontier.
            _jointRepairRestartRead = false;
            _stagingReadOffsets.Clear();
            _stagingReadWalTreeId = null;
            return true;
        }

        if (!HasPendingJointRepair && !_jointAdmissionWaiting)
        {
            return false;
        }

        RecordDrainReadOffsets(walTreeId, readFrom, advanced);
        return true;
    }

    private async Task<bool> CompleteJointRepairAsync(
        IViewCrossTreeCoordinatorGrain coordinator,
        ViewCrossTreeRepairHold hold,
        StagedTransaction? tx,
        CancellationToken cancellationToken)
    {
        if (hold.RepairApplied)
        {
            return true;
        }

        if (!hold.OriginalApplied)
        {
            hold.OriginalApplied = true;
            await PersistJointStateAsync();
        }

        if (!await JointRepairAdmissionGate.IsOpenAsync(cancellationToken))
        {
            _jointAdmissionWaiting = true;
            return false;
        }

        var stored = await coordinator.GetRepairReadinessAsync(ViewName);
        var readiness = stored ?? (tx is null ? null : await CaptureJointRepairAsync(hold, tx, cancellationToken));
        if (readiness is null)
        {
            return false;
        }

        if (stored is null)
        {
            hold.RepairSourceVersions = new Dictionary<string, HybridLogicalClock>(_repairCaptures[hold.Original.OperationId].Versions, StringComparer.Ordinal);
            foreach (var (partition, offset) in _jointConsumed)
            {
                hold.RetireThrough[partition] = Math.Max(hold.RetireThrough.GetValueOrDefault(partition, -1), offset);
            }

            await PersistJointStateAsync();
        }

        var decision = await coordinator.RegisterRepairReadyAsync(readiness);
        if (!decision.Applied)
        {
            _jointRepairsWaitingOnPeers.Add(hold.Original.OperationId);
            return false;
        }

        hold.RepairApplied = true;
        _jointRepairRestartRead = true;
        await PersistJointStateAsync();
        if (tx is not null)
        {
            ScheduleReconcileIfJointFlipMayBeSuperseded(tx);
        }
        _repairCaptures.Remove(hold.Original.OperationId);
        return true;
    }

    private async Task<ViewCrossTreeReadiness?> CaptureJointRepairAsync(
        ViewCrossTreeRepairHold hold,
        StagedTransaction tx,
        CancellationToken cancellationToken)
    {
        var registration = catalog.TryGet(ViewName)!;
        var operationId = hold.Original.OperationId;
        if (registration.Accumulative)
        {
            // The original already contains retention-shaped history effects;
            // a replacement observation must not resurrect pruned revisions.
            var historyReadiness = new ViewCrossTreeReadiness
            {
                OperationId = operationId,
                ViewName = ViewName,
                WaitSet = hold.Original.WaitSet,
                ViewTreeId = hold.Original.ViewTreeId,
                Upserts = hold.Original.Upserts,
                Deletes = hold.Original.Deletes,
                RepairProtocolVersion = 1,
            };
            _repairCaptures[operationId] = (historyReadiness, [], new(StringComparer.Ordinal), new(StringComparer.Ordinal));
            return historyReadiness;
        }

        var keys = new HashSet<string>(tx.PreparesByIndex.Values.Select(static prepared => prepared.Key), StringComparer.Ordinal);
        // A replacement atomic batch can overlap the original at only one key.
        // Include its complete source-key closure, never just that intersection.
        var expanded = true;
        while (expanded)
        {
            expanded = false;
            foreach (var (otherId, other) in _staging)
            {
                if (otherId == hold.TransactionId || !other.StagedSourceKeys.Overlaps(keys))
                {
                    continue;
                }

                if (!_jointEligible.Contains(otherId))
                {
                    return null;
                }

                foreach (var key in other.StagedSourceKeys)
                {
                    expanded |= keys.Add(key);
                }
            }
        }

        if (_repairCaptures.TryGetValue(operationId, out var captured)
            && !keys.IsSubsetOf(captured.Keys))
        {
            _repairCaptures.Remove(operationId);
        }

        if (!_repairCaptures.TryGetValue(operationId, out captured))
        {
            var source = await ReadSourceAsync(hold.WalTreeId, [.. keys],
                static (root, page) => root.GetVisibleRawEntriesAsync(page), cancellationToken);
            var mutations = new List<LatticeMutation>(keys.Count);
            foreach (var key in keys)
            {
                if (source.GetValueOrDefault(key) is not { } row)
                {
                    // Absence is not a coherent tombstone observation.
                    return null;
                }

                foreach (var staged in _staging.Values)
                {
                    foreach (var prepared in staged.PreparesByIndex.Values)
                    {
                        if (!string.Equals(prepared.Key, key, StringComparison.Ordinal)
                            || prepared.Delta is not null || prepared.IsMerge)
                        {
                            continue;
                        }

                        var order = row.Timestamp.CompareTo(prepared.Timestamp);
                        if (order < 0 || (order == 0
                            && (row.IsTombstone != IsDeletion(prepared)
                                || (!row.IsTombstone && !row.Value.AsSpan().SequenceEqual(prepared.Value)))))
                        {
                            return null;
                        }
                    }
                }

                var deleted = row.IsTombstone || row.ToLwwValue().IsExpired(DateTimeOffset.UtcNow.Ticks);
                mutations.Add(new LatticeMutation
                {
                    TreeId = hold.WalTreeId,
                    Key = key,
                    Kind = deleted ? MutationKind.Delete : MutationKind.Set,
                    IsTombstone = deleted,
                    Value = deleted ? null : row.Value,
                    Timestamp = row.Timestamp,
                    ExpiresAtTicks = row.ExpiresAtTicks,
                    Mode = row.MergeMode ?? LatticeMergeMode.LwwRegister,
                });
            }

            var (upserts, deletes) = await ProjectJointRepairAsync(registration, hold, mutations, cancellationToken);
            var partitions = await optionsResolver.GetWalPartitionsAsync(hold.WalTreeId);
            var heads = await SampleHeadOffsetsAsync(hold.WalTreeId, partitions, cancellationToken);
            captured = (new ViewCrossTreeReadiness
            {
                OperationId = operationId,
                ViewName = ViewName,
                WaitSet = hold.Original.WaitSet,
                ViewTreeId = hold.Original.ViewTreeId,
                Upserts = upserts,
                Deletes = deletes,
                RepairProtocolVersion = 1,
            }, heads, keys, mutations.Where(static mutation => IsLastWriterSuppressible(mutation))
                .ToDictionary(static mutation => mutation.Key, static mutation => mutation.Timestamp, StringComparer.Ordinal));
            _repairCaptures[operationId] = captured;
        }

        // The observation may include a committed replacement whose WAL has not
        // yet been consumed. Do not publish a partial replacement ahead of it.
        return Covers(_jointConsumed, captured.Heads) ? captured.Slice : null;
    }

    private async Task<(List<KeyValuePair<string, byte[]>> Upserts, List<string> Deletes)> ProjectJointRepairAsync(
        ViewRegistration registration,
        ViewCrossTreeRepairHold hold,
        List<LatticeMutation> mutations,
        CancellationToken cancellationToken)
    {
        if (registration.IsAggregation)
        {
            var buffering = new BufferingAggregationViewStore(
                new LatticeViewStore(grainFactory.GetGrain<ILattice>(hold.Original.ViewTreeId)));
            var capturing = CreateAggregationApplierOver(buffering);
            foreach (var mutation in mutations.OrderBy(static mutation => mutation.Timestamp))
            {
                var projected = false;
                foreach (var contribution in registration.AggregationProjection!.Project(ProjectionInput(registration, mutation)))
                {
                    projected = true;
                    await capturing.ApplyAsync(contribution, cancellationToken);
                }

                if (!projected)
                {
                    // A no-output successor still retracts the original membership.
                    await capturing.ApplyAsync(AggregationContribution.Retract(mutation.Key, mutation.Timestamp), cancellationToken);
                }
            }

            return buffering.Capture();
        }

        var writes = new List<ViewWrite>();
        foreach (var mutation in mutations)
        {
            writes.AddRange(registration.Projection!.Project(ProjectionInput(registration, mutation)));
        }

        var upserts = new List<KeyValuePair<string, byte[]>>();
        var deletes = new HashSet<string>(hold.Original.Deletes, StringComparer.Ordinal);
        foreach (var upsert in hold.Original.Upserts)
        {
            deletes.Add(upsert.Key);
        }

        foreach (var write in ViewWriteCoalescer.Coalesce(writes))
        {
            if (write.Kind == ViewWriteKind.Upsert)
            {
                upserts.Add(new(write.Key, write.Value!));
                deletes.Remove(write.Key);
            }
            else if (write.Kind == ViewWriteKind.Delete)
            {
                deletes.Add(write.Key);
            }
            else
            {
                throw new InvalidOperationException("Joint repair requires exact point projection effects.");
            }
        }

        return (upserts, [.. deletes]);
    }

    private bool RetireJointRepairHolds(IReadOnlyDictionary<int, long> durableOffsets)
    {
        var retired = false;
        foreach (var (operationId, hold) in state.State.CrossTreeRepairHolds.ToArray())
        {
            if (!hold.RepairApplied
                || hold.RetireThrough.Any(pair => durableOffsets.GetValueOrDefault(pair.Key, -1) < pair.Value))
            {
                continue;
            }

            state.State.PendingCrossTreeRepairAcknowledgements.Add(operationId);
            state.State.CrossTreeRepairHolds.Remove(operationId);
            retired = true;
        }

        return retired;
    }

    private async Task AcknowledgeRetiredJointRepairsAsync()
    {
        if (_jointStateWritePending)
        {
            // A failed checkpoint write leaves mutable state ahead of disk.
            // Never send its newly added acknowledgements before retrying it.
            await PersistJointStateAsync();
        }

        foreach (var operationId in state.State.PendingCrossTreeRepairAcknowledgements.ToArray())
        {
            await grainFactory.GetGrain<IViewCrossTreeCoordinatorGrain>(operationId).AcknowledgeRepairAsync(ViewName);
            state.State.PendingCrossTreeRepairAcknowledgements.Remove(operationId);
            await PersistJointStateAsync();
        }
    }

    private async Task ReleaseUncommittedJointOwnershipAsync()
    {
        foreach (var (operationId, hold) in state.State.CrossTreeRepairHolds.ToArray())
        {
            if (hold.OriginalApplied)
            {
                continue;
            }

            var decision = await grainFactory.GetGrain<IViewCrossTreeCoordinatorGrain>(operationId)
                .RegisterDegradedAsync(ViewName);
            if (decision.Applied)
            {
                hold.OriginalApplied = true;
            }
            else
            {
                state.State.CrossTreeRepairHolds.Remove(operationId);
            }

            await PersistJointStateAsync();
        }
    }

    private async Task PersistJointStateAsync()
    {
        _jointStateWritePending = true;
        await state.WriteStateAsync();
        _jointStateWritePending = false;
    }
}
