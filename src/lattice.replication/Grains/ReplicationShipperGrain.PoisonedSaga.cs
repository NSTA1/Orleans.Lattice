using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Poisoned sagas (issue #4494): a saga whose prepare was parked on the
/// dead-letter queue is never committed on the peer.
/// <para>
/// A batch the shipper cannot encode is parked on the per-tree dead-letter
/// queue and the cursor advances past it. That is a loss for the peer, not a
/// deferral: replaying a parked entry applies it on this cluster, not on the
/// peer. A saga terminal released after one of its prepares was parked would
/// commit the saga on the peer without that write - a torn batch the peer then
/// keeps, because the parked prepare can never arrive. So the saga is
/// poisoned: every later prepare and every terminal of it is parked too
/// (reason <see cref="LatticeReplicationMetrics.ReasonPoisonedSaga"/>), and the
/// peer keeps the saga invisible. That trades the saga's liveness on the peer
/// for its atomicity; a re-bootstrap of the peer ships the saga whole. No abort
/// is sent to settle the prepares the peer already staged: the origin never
/// decided one.
/// </para>
/// <para>
/// The poison list is persisted with the cursors, so a reactivation keeps
/// withholding the saga, and it is bounded. An entry retires only once the saga
/// can append no further record: the origin registry was seen holding its
/// decision and later holding no row for it (every terminal, a split sweep's
/// late one included, needs a recorded decision), and the durable cursor has
/// passed every partition tail sampled after that. A count of the saga's
/// terminals is no bound, because an unstamped or late sweep terminal can
/// follow the stamped ones. When the list is full the shipper fails closed: it
/// does not park the failing batch or advance past it, so the stream to the
/// peer stalls rather than letting a saga through torn.
/// </para>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    /// <summary>Upper bound on the poisoned sagas held at once.</summary>
    internal const int PoisonedSagaCapacity = 16384;

    /// <summary>Upper bound on the registry reads one retirement probe issues.</summary>
    internal const int PoisonRetirementProbeBatch = 64;

    /// <summary>Minimum interval between retirement probes of the poison list.</summary>
    internal static readonly TimeSpan PoisonRetirementProbeInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long the origin registry must keep holding no row for a decided
    /// poisoned saga before the tails are sampled. A split sweep that read the
    /// decision just before the purge appends its late terminal within this
    /// window, so the terminal lands below the sample and is parked.
    /// </summary>
    private TimeSpan _poisonRetirementGrace = TimeSpan.FromMinutes(10);

    /// <summary>Overrides the retirement grace. Test seam.</summary>
    internal void SetPoisonRetirementGraceForTesting(TimeSpan grace) => _poisonRetirementGrace = grace;

    /// <summary>
    /// Source-log positions of poisoned records already parked this activation,
    /// so a batch re-merged after a failed send does not park them twice.
    /// </summary>
    private readonly HashSet<(int Partition, long Sequence)> _poisonParked = new();

    private DateTime _nextPoisonRetirementProbeUtc = DateTime.MinValue;

    private int _poisonRetirementProbeOffset;

    /// <summary>Number of sagas currently poisoned. Test seam.</summary>
    internal int PoisonedSagaCountForTesting => state.State.PoisonedSagas.Count;

    /// <summary>Lets the next pump tick run a retirement probe at once. Test seam.</summary>
    internal void ExpirePoisonRetirementProbeForTesting() => _nextPoisonRetirementProbeUtc = DateTime.MinValue;

    /// <summary>Whether <paramref name="record"/> is a prepare or a terminal of a poisoned saga.</summary>
    private bool IsPoisonedSagaRecord(in WalRecord record) =>
        record.TransactionId != Guid.Empty
        && (record.IsPrepared || record.Op is MutationKind.TxCommit or MutationKind.TxAbort)
        && state.State.PoisonedSagas.Count > 0
        && state.State.PoisonedSagas.ContainsKey(record.TransactionId);

    /// <summary>
    /// Poisons <paramref name="transactionId"/> for this shipper's peer: every
    /// later prepare and terminal of the saga the merge reads is parked instead
    /// of shipped. The caller persists the state before advancing past the
    /// record that caused it, and checks <see cref="HasPoisonedSagaCapacity"/>
    /// first - this method does not refuse. A saga already poisoned is left as
    /// it is. Shared by every path that loses a prepare for the peer.
    /// </summary>
    private void AddPoisonedSaga(Guid transactionId, string cause)
    {
        if (state.State.PoisonedSagas.ContainsKey(transactionId))
        {
            return;
        }

        state.State.PoisonedSagas[transactionId] = new PoisonedSaga { TransactionId = transactionId };
        Logger.LogWarning(
            "Shipper {Context} poisoned saga {TransactionId}: {Cause}, so every later prepare and terminal of the saga is dead-lettered too "
            + "and peer {Peer} serves the saga as never written while this cluster has it decided. Re-bootstrap the peer to repair it.",
            LogContext, transactionId, cause, _peerClusterId);
    }

    /// <summary>Whether the poison list can take <paramref name="count"/> more sagas.</summary>
    private bool HasPoisonedSagaCapacity(int count) =>
        state.State.PoisonedSagas.Count + count <= PoisonedSagaCapacity;

    /// <summary>
    /// Set when the batch being parked poisoned a new saga; the caller then
    /// asks the peer to re-seed (<see cref="MarkReseedRequiredForPoisonAsync"/>).
    /// </summary>
    private bool _reseedForPoisonPending;

    /// <summary>
    /// Asks the peer to re-seed after a saga was poisoned for it (#4620). The
    /// poison keeps the peer from ever committing the saga torn, but it also
    /// means the peer never receives the saga again, so without a re-seed the
    /// peer serves it as never written for good while this cluster has it
    /// decided. Reuses the forced-gap marker (#4577, #4533): the replay hold is
    /// taken first and the tree's export epoch recorded durably, every push
    /// carries it, the peer re-bootstraps from an export after it - which ships
    /// the decided saga as committed rows and its decision row - and the echo
    /// clears the marker. The rewind that follows re-reads the poisoned saga's
    /// records, and they are parked again, because the saga stays poisoned, so
    /// the re-seed does not repeat.
    /// </summary>
    private async Task MarkReseedRequiredForPoisonAsync(CancellationToken cancellationToken)
    {
        _reseedForPoisonPending = false;
        if (ReseedRequired)
        {
            return;
        }

        var epoch = await TakePeerOffLogStateAsync();

        // Park the poisoned saga's held terminals before the holds are dropped,
        // then drop the rest: saga records are withheld from here until the
        // echo, and the rewind re-reads everything a dropped hold would have
        // released. The drain buffer is not purged: it is the batch being parked.
        await ParkPoisonedTerminalHoldsAsync(cancellationToken);
        _terminalHolds.Clear();
        _prepareTallies.Clear();
        _prepareTallyOrder.Clear();

        // Durable with the poison itself, before any record of the batch is parked.
        await state.WriteStateAsync();
        ReportReseedState();

        Logger.LogWarning(
            "{Context}: a saga was poisoned for peer {Peer} after a prepare was dead-lettered. Saga records are withheld from the "
            + "peer until it is re-seeded from a snapshot export after epoch {Epoch}, which delivers the poisoned saga whole.",
            LogContext, _peerClusterId, epoch);
    }

    /// <summary>
    /// Poisons the saga of every prepared record in the drain buffer that is
    /// about to be parked. Returns <see langword="false"/> - poisoning nothing -
    /// when the poison list cannot take the batch's new sagas, in which case the
    /// caller must neither park the batch nor advance past it.
    /// </summary>
    private bool TryPoisonDrainBufferSagas()
    {
        var poisoned = state.State.PoisonedSagas;
        HashSet<Guid>? fresh = null;
        foreach (var entry in _drainBuffer)
        {
            if (entry.IsPrepared && entry.TransactionId != Guid.Empty && !poisoned.ContainsKey(entry.TransactionId))
            {
                (fresh ??= new HashSet<Guid>()).Add(entry.TransactionId);
            }
        }

        if (fresh is not null)
        {
            if (!HasPoisonedSagaCapacity(fresh.Count))
            {
                Logger.LogError(
                    "Shipper {Context} cannot park a {EntryCount}-entry batch carrying a prepare of {SagaCount} saga(s) (first {TransactionId}): "
                    + "the poison list is full ({Capacity}), so parking it would let a saga reach the peer without that write. "
                    + "The shipper does not advance past the batch until the list drains; re-bootstrap the peer.",
                    LogContext, _drainBuffer.Count, fresh.Count, fresh.First(), PoisonedSagaCapacity);
                LatticeReplicationMetrics.ShipperSagaPoisoned.Add(
                    fresh.Count,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.OutcomeSagaPoisonRefused),
                    LatticeTenantLabel.ForTree(_treeName));
                return false;
            }

            foreach (var txid in fresh)
            {
                AddPoisonedSaga(txid, "a prepare of the saga was dead-lettered instead of shipped");
            }

            // A poisoned saga is never shipped to the peer again, so only a
            // re-seed makes it visible there (#4620).
            _reseedForPoisonPending = true;

            LatticeReplicationMetrics.ShipperSagaPoisoned.Add(
                fresh.Count,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.OutcomeSagaPoisoned),
                LatticeTenantLabel.ForTree(_treeName));
        }

        // A terminal riding in the parked batch shows the saga is decided.
        foreach (var entry in _drainBuffer)
        {
            if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort
                && poisoned.TryGetValue(entry.TransactionId, out var saga))
            {
                saga.Decided = true;
            }
        }

        return true;
    }

    /// <summary>
    /// Parks a prepare or terminal of a poisoned saga the merge consumed instead
    /// of shipping it. The caller skips the record; the batch's acknowledgement
    /// then moves the cursor past it.
    /// </summary>
    private async Task ParkPoisonedRecordAsync(WalRecord record, int partition, long sequence, CancellationToken cancellationToken)
    {
        if (record.Op is MutationKind.TxCommit or MutationKind.TxAbort
            && state.State.PoisonedSagas.TryGetValue(record.TransactionId, out var saga))
        {
            saga.Decided = true;
        }

        if (partition >= 0 && !_poisonParked.Add((partition, sequence)))
        {
            return;
        }

        var dlq = _grainFactory.GetGrain<IReplicationDeadLetterGrain>(_treeName);
        try
        {
            await dlq.EnqueueAsync(
                record,
                $"Saga {record.TransactionId} is poisoned for peer '{_peerClusterId}': a prepare of the saga was dead-lettered, "
                + $"so this {(record.IsPrepared ? "prepare" : record.Op.ToString())} is withheld to keep the peer from committing the saga without it.",
                retryCount: 0,
                LatticeReplicationMetrics.ReasonPoisonedSaga,
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The record is withheld from the peer either way; only its parked
            // copy is lost, and the WAL still retains it until the GC trims it.
            Logger.LogWarning(ex,
                "Failed to park a record of poisoned saga {TransactionId} on the DLQ for {Context} (key={Key}, hlc={Hlc}); it is still withheld from the peer",
                record.TransactionId, LogContext, record.Key, record.Timestamp);
        }
    }

    /// <summary>
    /// Parks every terminal held for a poisoned saga that is not riding in an
    /// outstanding batch, and drops its hold. The hold's cursor cap lifts with
    /// it; the cursor still moves past the terminal only once the batch that
    /// consumed it is acknowledged, which is after it was parked here.
    /// </summary>
    private async Task ParkPoisonedTerminalHoldsAsync(CancellationToken cancellationToken)
    {
        if (_terminalHolds.Count == 0 || state.State.PoisonedSagas.Count == 0)
        {
            return;
        }

        List<TerminalHold>? parked = null;
        foreach (var hold in _terminalHolds)
        {
            if (hold.EmittedBatchId == 0 && IsPoisonedSagaRecord(hold.Record))
            {
                (parked ??= new List<TerminalHold>()).Add(hold);
            }
        }

        if (parked is null)
        {
            return;
        }

        foreach (var hold in parked)
        {
            await ParkPoisonedRecordAsync(
                hold.Record, hold.Carried ? -1 : hold.Partition, hold.Sequence, cancellationToken);
            _terminalHolds.Remove(hold);
        }
    }

    /// <summary>
    /// Advances the retirement of the poison list, at most once per
    /// <see cref="PoisonRetirementProbeInterval"/> and for at most
    /// <see cref="PoisonRetirementProbeBatch"/> sagas. For each saga not yet
    /// awaiting its tails it reads the origin registry's stored row: a decision
    /// marks the saga decided, and no row for a decided saga means the row was
    /// purged, so no further record of the saga can be appended. The current
    /// tail of every source partition is then sampled, and the saga retires once
    /// the durable cursor has reached them (<see cref="RetireSettledPoisonedSagas"/>).
    /// A failed read leaves the saga as it is for the next probe.
    /// </summary>
    private async Task ProbePoisonedSagaRetirementAsync(CancellationToken cancellationToken)
    {
        var poisoned = state.State.PoisonedSagas;
        if (poisoned.Count == 0 || DateTime.UtcNow < _nextPoisonRetirementProbeUtc)
        {
            return;
        }

        _nextPoisonRetirementProbeUtc = DateTime.UtcNow + PoisonRetirementProbeInterval;

        var candidates = poisoned.Values.Where(s => s.RetireAfterTails is null).ToList();
        if (candidates.Count == 0)
        {
            return;
        }

        var start = _poisonRetirementProbeOffset % candidates.Count;
        var take = Math.Min(PoisonRetirementProbeBatch, candidates.Count);
        _poisonRetirementProbeOffset = start + take;

        List<PoisonedSaga>? purged = null;
        var changed = false;
        for (var i = 0; i < take; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var saga = candidates[(start + i) % candidates.Count];
            TxStatus recorded;
            try
            {
                recorded = await TxRegistryRouting
                    .GetRegistry(_grainFactory, _treeName, saga.TransactionId)
                    .GetRecordedStatusAsync(saga.TransactionId);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogDebug(ex,
                    "Shipper {Context} could not read the registry row of poisoned saga {TransactionId}; retrying on the next probe",
                    LogContext, saga.TransactionId);
                continue;
            }

            if (recorded != TxStatus.InFlight)
            {
                if (!saga.Decided || saga.AbsentSinceUtcTicks != 0)
                {
                    saga.Decided = true;
                    saga.AbsentSinceUtcTicks = 0;
                    changed = true;
                }
            }
            else if (saga.Decided)
            {
                var now = DateTime.UtcNow.Ticks;
                if (saga.AbsentSinceUtcTicks == 0)
                {
                    saga.AbsentSinceUtcTicks = now;
                    changed = true;
                }

                if (now - saga.AbsentSinceUtcTicks >= _poisonRetirementGrace.Ticks)
                {
                    (purged ??= new List<PoisonedSaga>()).Add(saga);
                }
            }
        }

        if (purged is not null)
        {
            var partitions = _partitionCount;
            var reads = new Task<long>[partitions];
            for (var p = 0; p < partitions; p++)
            {
                var grain = _partitionGrainCache[p] ??=
                    _grainFactory.GetGrain<IWalShardGrain>($"{_walTreeId}/{p}");
                reads[p] = grain.GetNextSequenceAsync(cancellationToken).AsTask();
            }

            var tails = await Task.WhenAll(reads);
            foreach (var saga in purged)
            {
                saga.RetireAfterTails = tails;
            }

            changed = true;
        }

        if (changed)
        {
            _pendingCursorWrites++;
        }
    }

    /// <summary>
    /// Retires every poisoned saga whose sampled tails the durable cursors have
    /// all reached, and forgets parked positions the cursors have passed. Runs
    /// inside the cursor fold, so a retirement is persisted with the cursor move
    /// that justifies it. Returns whether a saga was retired.
    /// </summary>
    private bool RetireSettledPoisonedSagas()
    {
        var cursors = state.State.PartitionCursors;
        if (_poisonParked.Count > 0)
        {
            _poisonParked.RemoveWhere(p => cursors.TryGetValue(p.Partition, out var next) && p.Sequence < next);
        }

        var poisoned = state.State.PoisonedSagas;
        if (poisoned.Count == 0)
        {
            return false;
        }

        List<Guid>? settled = null;
        foreach (var (txid, saga) in poisoned)
        {
            if (saga.RetireAfterTails is not { } tails)
            {
                continue;
            }

            var passed = true;
            for (var p = 0; p < tails.Length; p++)
            {
                var next = cursors.TryGetValue(p, out var saved) ? saved : 0L;
                if (next < tails[p])
                {
                    passed = false;
                    break;
                }
            }

            if (passed)
            {
                (settled ??= new List<Guid>()).Add(txid);
            }
        }

        if (settled is null)
        {
            return false;
        }

        foreach (var txid in settled)
        {
            poisoned.Remove(txid);
            Logger.LogInformation(
                "Shipper {Context} retired poisoned saga {TransactionId}: its registry row is gone and the cursor has passed every record it could have appended.",
                LogContext, txid);
        }

        return true;
    }

    /// <summary>
    /// The source log was replaced under the shipper. After a saga pause both
    /// clusters were restored to the cut, so no poisoned saga survives it.
    /// Otherwise the new copy can mirror a poisoned saga's records, so the poison
    /// stays, and sampled tails - which address the retired log - are dropped:
    /// the saga retires only once tails are sampled from the new log.
    /// </summary>
    private void ResetPoisonedSagasForNewSource(bool followsSagaPause)
    {
        _poisonParked.Clear();
        _nextPoisonRetirementProbeUtc = DateTime.MinValue;
        if (followsSagaPause)
        {
            state.State.PoisonedSagas.Clear();
            return;
        }

        foreach (var saga in state.State.PoisonedSagas.Values)
        {
            saga.RetireAfterTails = null;
        }
    }
}
