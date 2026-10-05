using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Replay filter (issue #4533). A host that ran without replication, or before
/// the decision-purge guard (#4508) existed - or a registry activation on a
/// silo that predates the guard, during a rolling upgrade - purges a saga's
/// decision on retention alone while the write-ahead log keeps its records.
/// Re-shipping such a saga to a peer strands it there: nothing can settle it.
/// Only a non-contiguous stream re-ships it: a re-seed's rewind to the lowest
/// retained entry, or a source-identity rebind's restart on a new log. A
/// contiguous stream delivers every saga whole, prepares before terminal.
/// <para>
/// Such a replay records a horizon, <see cref="ReplicationShipperState.ReplayFilterHorizon"/>:
/// every partition's next sequence when it began. While it is set, the shipper
/// decides once per saga, on the first record it reads, whether the origin
/// proves the saga forgotten and its decision purged: no participant row (a
/// saga registers its participants durably before it appends any prepare, and
/// only <c>ForgetAsync</c>, after the decision, removes them), then no stored
/// decision. The reads go in that order because the decision is recorded before
/// the forget and purged after it. The verdict is cached for the replay and
/// applies to every record of the saga, terminals included, so a saga is
/// shipped whole or withheld whole. A failed read fails the tick, so the
/// partition holds and retries rather than guessing.
/// </para>
/// <para>
/// A saga in flight when the peer was taken off the log can be decided,
/// forgotten and - its records having been trimmed - purged legitimately
/// while the replay runs, which would read as purged and withhold terminals
/// the re-seeded peer needs. So the shipper holds every decision purge on the
/// tree (<see cref="IWalPurgeHoldGrain"/>, under a replay key) from before it
/// takes the peer off the log, or before a rebind replay begins, until the
/// filter clears with no re-seed outstanding. A purged verdict is then only
/// ever a saga purged before the hold, which the re-seed's export cannot
/// carry either.
/// </para>
/// <para>
/// Every record of a purged saga was appended before its purge, so a purged
/// verdict raises the horizon to the log's current next sequences; the filter
/// clears once every partition's cursor has passed the horizon. A withheld
/// saga must still converge, so a purged verdict is acted on only when the
/// replay carries the saga's effects: a re-seed's snapshot carries a committed
/// saga's values (and the receiver clears this origin's pending buckets before
/// it drains the snapshot), and this activation started the replay, so no
/// record of the saga can have shipped before the verdict. Otherwise - a replay
/// a rebind started (no snapshot), or one an earlier activation started (its
/// verdicts died with it) - the verdict takes the peer off the log (#4534), and
/// the re-seed that follows restarts the replay with a carrier.
/// </para>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    // Per transaction, whether the origin proved it forgotten and purged. Lives
    // only while the replay filter is set: cleared when it is set or cleared, and
    // with the activation. Holds at most one entry per saga with a record in the
    // replay region, which the retained log bounds.
    private readonly Dictionary<Guid, bool> _replayVerdicts = new();

    // Whether this activation started the current replay, and whether that
    // replay began from a re-seed (whose snapshot carries a withheld saga).
    private bool _replayStartedHere;
    private bool _replayCarried;

    /// <summary>
    /// Starts a replay filter over the bound log: records the horizon (its
    /// next sequence per partition) in state without writing it, so the
    /// caller's write makes it durable together with the replay's start.
    /// </summary>
    private async Task BeginReplayFilterAsync(int partitions, bool carried)
    {
        await TakeReplayHoldAsync();
        state.State.ReplayFilterHorizon = await ReadNextSequencesAsync(partitions);
        _replayVerdicts.Clear();
        _replayStartedHere = true;
        _replayCarried = carried;
    }

    private async Task<long[]> ReadNextSequencesAsync(int partitions)
    {
        var next = new long[partitions];
        for (var p = 0; p < partitions; p++)
        {
            next[p] = await _grainFactory.GetGrain<IWalShardGrain>($"{_walTreeId}/{p}")
                .GetNextSequenceAsync(CancellationToken.None);
        }

        return next;
    }

    private string ReplayHoldKey => Context.GrainId.ToString() + "#replay";

    private DateTimeOffset _nextPurgeHoldUnsupportedLogUtc = DateTimeOffset.MinValue;

    /// <summary>Test seam: overrides the cluster-manifest check of <see cref="AllSilosHonourPurgeHolds"/>.</summary>
    internal Func<bool>? PurgeHoldSupportForTesting { get; set; }

    /// <summary>
    /// <see langword="true"/> when every active silo hosts
    /// <see cref="IWalPurgeHoldGrain"/>, and so runs a transaction registry
    /// that honours a purge hold (it shipped with that registry, after the
    /// decision-purge guard of #4508). A silo that predates it purges saga
    /// decisions on retention alone, which a replay cannot tolerate. A host
    /// without the Orleans runtime services (a bare unit-test activation) has
    /// no other silo and answers <see langword="true"/>.
    /// </summary>
    private bool AllSilosHonourPurgeHolds()
    {
        return PurgeHoldSupportForTesting is { } overridden
            ? overridden()
            : PurgeHoldSupport.AllSilosHonour(Context.ActivationServices);
    }

    private void LogPurgeHoldUnsupported()
    {
        var now = _cursorFlushClock.GetUtcNow();
        if (now < _nextPurgeHoldUnsupportedLogUtc)
        {
            return;
        }

        _nextPurgeHoldUnsupportedLogUtc = now + TimeSpan.FromMinutes(1);
        Logger.LogWarning(
            "{Context}: a silo in the cluster predates the decision-purge hold, so it may purge a saga decision a replay "
            + "still needs; saga records stay withheld from the peer until every silo is upgraded.",
            LogContext);
    }

    /// <summary>
    /// Durably records this shipper's replay hold on the bound log's
    /// <see cref="IWalPurgeHoldGrain"/>, which suspends every saga decision
    /// purge on the tree while it is outstanding. Records the log in state
    /// without writing it; the caller's write makes it durable. A failure
    /// throws, so nothing that depends on the hold proceeds.
    /// </summary>
    private async Task TakeReplayHoldAsync()
    {
        var log = _walTreeId;
        var previous = state.State.ReplayHoldLog;
        if (string.Equals(previous, log, StringComparison.Ordinal))
        {
            return;
        }

        await _grainFactory.GetGrain<IWalPurgeHoldGrain>(log).AddAsync(ReplayHoldKey, Array.Empty<long>());
        state.State.ReplayHoldLog = log;
        if (previous is not null)
        {
            await RemoveReplayHoldAsync(previous);
        }
    }

    private async Task<bool> RemoveReplayHoldAsync(string log)
    {
        try
        {
            await _grainFactory.GetGrain<IWalPurgeHoldGrain>(log).RemoveAsync(ReplayHoldKey);
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "{Context}: releasing the replay's decision-purge hold on log {Log} failed; the next tick retries.",
                LogContext, log);
            return false;
        }
    }

    /// <summary>
    /// Releases the replay hold once the replay filter has cleared and no
    /// re-seed is outstanding.
    /// </summary>
    private async Task MaybeReleaseReplayHoldAsync()
    {
        if (state.State.ReplayHoldLog is not { } log || state.State.ReplayFilterHorizon is not null || ReseedRequired)
        {
            return;
        }

        if (!await RemoveReplayHoldAsync(log))
        {
            return;
        }

        // A hold added before a crash lost the state write may sit on the
        // current log under the same key; removal is idempotent.
        if (!string.Equals(log, _walTreeId, StringComparison.Ordinal))
        {
            await RemoveReplayHoldAsync(_walTreeId);
        }

        state.State.ReplayHoldLog = null;
        await state.WriteStateAsync();
    }

    /// <summary>Clears the replay filter once every partition's cursor has passed the horizon.</summary>
    private async Task PrepareReplayFilterForTickAsync(int partitions)
    {
        if (state.State.ReplayFilterHorizon is not { } horizon)
        {
            await MaybeReleaseReplayHoldAsync();
            return;
        }

        // A silo that ignores the replay's purge hold joined while the replay
        // runs: its verdicts are no longer exact, so take the peer off the log.
        if (!ReseedRequired && !AllSilosHonourPurgeHolds())
        {
            LogPurgeHoldUnsupported();
            await MarkReseedRequiredAsync(0, 0, 0);
            return;
        }

        for (var p = 0; p < Math.Max(partitions, horizon.Length); p++)
        {
            // A partition never consumed has no cursor and sits at offset 0, so
            // an empty partition (horizon 0) never holds the filter open.
            var bound = p < horizon.Length ? horizon[p] : 0L;
            var cursor = state.State.PartitionCursors.TryGetValue(p, out var consumed) ? consumed : 0L;
            if (cursor < bound)
            {
                return;
            }
        }

        state.State.ReplayFilterHorizon = null;
        await state.WriteStateAsync();
        _replayVerdicts.Clear();
        Logger.LogInformation("{Context}: the replay passed its horizon; saga records ship unfiltered.", LogContext);
        await MaybeReleaseReplayHoldAsync();
    }

    /// <summary>
    /// Withholds <paramref name="record"/> when it belongs to a saga the origin
    /// proves forgotten and purged. Returns <see langword="true"/> when the
    /// record was withheld (or the peer was taken off the log).
    /// </summary>
    private async Task<bool> TryWithholdReplayedSagaAsync(WalRecord record, int partition, long sequence, int partitions)
    {
        if (record.TransactionId == Guid.Empty || !IsSagaRecord(in record))
        {
            return false;
        }

        if (_replayVerdicts.TryGetValue(record.TransactionId, out var purged))
        {
            return purged;
        }

        // Participants first, then the decision: "no participants, then no
        // decision" proves a purge, because the forget follows the decision and
        // the purge follows the forget. A fault propagates and fails the tick.
        var registry = TxRegistryRouting.GetRegistry(_grainFactory, _treeName, record.TransactionId);
        var participants = await registry.GetParticipantsAsync(record.TransactionId);
        if (participants.Count > 0)
        {
            purged = false;
        }
        else
        {
            var recorded = await registry.GetRecordedStatusAsync(record.TransactionId);
            purged = recorded is not (TxStatus.Committed or TxStatus.Aborted);
        }

        if (!purged)
        {
            _replayVerdicts[record.TransactionId] = false;
            return false;
        }

        if (!_replayStartedHere || !_replayCarried)
        {
            // No carrier for a withheld saga, or part of it may have shipped
            // under a verdict that died with an earlier activation: re-seed.
            Logger.LogWarning(
                "{Context}: saga {TransactionId} in a replayed stream was forgotten and its decision purged, and this "
                + "replay cannot withhold it whole with its effects carried; the peer is re-seeded.",
                LogContext, record.TransactionId);
            await MarkReseedRequiredAsync(partition, sequence, sequence);
            return true;
        }

        _replayVerdicts[record.TransactionId] = true;
        ForgetFrontierPrepare(record.TransactionId);
        var horizon = state.State.ReplayFilterHorizon!;
        var next = await ReadNextSequencesAsync(Math.Max(partitions, horizon.Length));
        for (var p = 0; p < next.Length; p++)
        {
            next[p] = Math.Max(next[p], p < horizon.Length ? horizon[p] : 0L);
        }

        state.State.ReplayFilterHorizon = next;
        await state.WriteStateAsync();
        Logger.LogWarning(
            "{Context}: saga {TransactionId} in a replayed stream was forgotten and its decision purged; it is withheld "
            + "whole from the peer, whose re-seed snapshot carried its effects.",
            LogContext, record.TransactionId);
        return true;
    }
}
