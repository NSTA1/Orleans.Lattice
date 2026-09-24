using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Row-size admission bound for new atomic-write sagas (issue #3475). The
/// registry persists its whole state as one grain-state row, and group commit
/// lifts saga throughput far enough that the tombstones retained for
/// <see cref="LatticeOptions.TxDecisionRetention"/> could otherwise grow that
/// row past the storage provider's per-row limit, at which point every
/// registry write fails. See
/// <see cref="LatticeOptions.TxRegistryAdmissionBudgetBytes"/> for the
/// ceiling maths.
/// </summary>
internal sealed partial class TxRegistryGrain
{
    // Per-entry weights of the O(1) row-size estimate, in bytes. Each is set at
    // or above the measured JSON size of one entry of that map under the default
    // grain-state serializer (JSON is the largest wire shape the registry is
    // stored in), so the estimate over-counts rather than under-counts.
    // TxRegistryGrainTests.AdmissionBound pins every weight against a real
    // serialisation, so a change to the state shape that outgrows a weight fails
    // a test instead of silently weakening the bound.
    internal const long AdmissionEstimateBaseBytes = 2 * 1024;
    internal const long AdmissionEstimateDecisionBytes = 48;
    internal const long AdmissionEstimateForgottenAtBytes = 80;
    internal const long AdmissionEstimateParticipantsBytes = 256;
    internal const long AdmissionEstimateTerminalArrivalsBytes = 256;
    internal const long AdmissionEstimateExpectedTerminalsBytes = 48;
    internal const long AdmissionEstimateAuthorityBytes = 192;
    internal const long AdmissionEstimateSnapshotPinBytes = 384;
    internal const long AdmissionEstimatePinnedTxidBytes = 48;

    /// <summary>
    /// Count-weighted estimate of the registry's persisted row size. Reads
    /// dictionary counts, never serialises, so it is O(1) in the tombstone
    /// count that dominates the row at high throughput. A participant set is
    /// weighted per entry rather than per shard index, which holds for the
    /// small sets a saga registers; the participants map is also bounded by
    /// the in-flight saga count. Snapshot pins are the one exception: each pin
    /// carries its own txid list, which can be large, so the estimate walks the
    /// pins (one per open snapshot cursor, normally none or a handful) and
    /// weights each pinned txid.
    /// </summary>
    internal long EstimatedRowBytes()
    {
        var s = state.State;
        long pinnedTxids = 0;
        foreach (var pin in s.SnapshotPins.Values)
        {
            pinnedTxids += pin.Txids.Count;
        }

        return AdmissionEstimateBaseBytes
            + (s.Decisions.Count * AdmissionEstimateDecisionBytes)
            + (s.ForgottenAt.Count * AdmissionEstimateForgottenAtBytes)
            + (s.Participants.Count * AdmissionEstimateParticipantsBytes)
            + (s.TerminalArrivals.Count * AdmissionEstimateTerminalArrivalsBytes)
            + (s.ExpectedTerminals.Count * AdmissionEstimateExpectedTerminalsBytes)
            + ((s.ExternalAuthorities.Count + s.ReceiverDecisionAuthorities.Count) * AdmissionEstimateAuthorityBytes)
            + (s.SnapshotPins.Count * AdmissionEstimateSnapshotPinBytes)
            + (pinnedTxids * AdmissionEstimatePinnedTxidBytes);
    }

    /// <inheritdoc />
    public Task EnsureSagaAdmissionAsync()
    {
        if (optionsMonitor.Get(TreeId).TxRegistryAdmissionBudgetBytes is not { } budget
            || EstimatedRowBytes() < budget)
        {
            return Task.CompletedTask;
        }

        return EnsureSagaAdmissionSlowAsync(budget);
    }

    private async Task EnsureSagaAdmissionSlowAsync(long budget)
    {
        // Over budget: reclaim any tombstones that aged out of the retention
        // window before refusing. On a busy tree ForgetAsync prunes inline on
        // every saga completion, but an idle tree has no saga left to call it,
        // and without this reclaim it would refuse forever.
        await PruneExpiredForAdmissionAsync();

        var estimate = EstimatedRowBytes();
        if (estimate < budget)
        {
            return;
        }

        LogTxRegistryAdmissionRefused(logger, TreeId, estimate, budget);
        throw new LatticeSaturatedException(
            $"Atomic write refused on tree '{TreeId}': the transaction registry's estimated persisted row size "
            + $"({estimate} bytes) is at or above {nameof(LatticeOptions.TxRegistryAdmissionBudgetBytes)} ({budget} bytes). "
            + "Capacity returns as completed sagas' tombstones age out of TxDecisionRetention; back off and retry. "
            + "Nothing was written for this operation.",
            TreeId,
            LatticeSaturationSource.TxRegistryCapacity);
    }

    /// <summary>
    /// Runs the same expired-tombstone and expired-pin prune
    /// <see cref="ForgetAsync(Guid)"/> folds into its write, as a group
    /// commit of its own, with the same revision accounting and rollback.
    /// A no-op when nothing has expired.
    /// </summary>
    private async Task PruneExpiredForAdmissionAsync()
    {
        var pruned = PruneExpired(TimeProvider.GetUtcNow(), Retention);
        if (!pruned.Any)
        {
            return;
        }

        var retired = pruned.Tombstones?.Count ?? 0;
        state.State.TombstoneRetirementEpoch += retired;

        var revisionBumped = retired > 0;
        var core = DecisionCore();
        var prevRevision = state.State.DecisionsRevision;
        if (revisionBumped)
        {
            core.AdvanceRevision();
            state.State.DecisionsRevision = core.Revision;
        }

        var group = PendingGroup();
        if (pruned.Tombstones is { } prunedTombstones)
        {
            foreach (var entry in prunedTombstones)
            {
                group.AddTxid(entry.Txid);
            }
        }
        if (pruned.ExpiredPins is not null)
        {
            group.TouchesAllTxids = true;
        }

        await CommitAsync(group, () =>
        {
            if (pruned.Tombstones is { } tombstones)
            {
                foreach (var entry in tombstones)
                {
                    if (entry.HadDecision)
                        state.State.Decisions[entry.Txid] = entry.Decision;
                    state.State.ForgottenAt[entry.Txid] = entry.ForgottenAt;
                }

                state.State.TombstoneRetirementEpoch -= retired;
                InvalidateExpiryMemo();
            }
            if (pruned.ExpiredPins is { } evicted)
            {
                foreach (var (pinId, pin) in evicted)
                {
                    state.State.SnapshotPins[pinId] = pin;
                }
                InvalidatePinMemo();
            }
            if (revisionBumped)
            {
                core.RollbackRevision(prevRevision);
                state.State.DecisionsRevision = core.Revision;
            }
        });
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "TxRegistry on tree {TreeId} refused a new atomic-write saga: estimated registry row size {EstimatedBytes} bytes is at or above the admission budget of {BudgetBytes} bytes.")]
    private static partial void LogTxRegistryAdmissionRefused(ILogger logger, string treeId, long estimatedBytes, long budgetBytes);
}
