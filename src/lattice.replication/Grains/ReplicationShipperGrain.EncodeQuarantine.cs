using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A batch the shipper cannot encode (issue #4614). Its records can never ship
/// in their current form, and parking them on this cluster's dead-letter queue
/// would not help: a replay there applies them locally, never on the peer. So
/// an encode failure is handled like a forced gap (#4577): the peer is taken
/// off the log and re-seeded from a snapshot export, which carries the
/// committed state the batch would have delivered, encoded by a different
/// serializer.
/// <para>
/// The failed batch's per-partition sequence hull is quarantined in state, the
/// marker and the hull are written durably, and only then does the cursor move
/// past the batch. The re-seed rewind consumes quarantined positions without
/// shipping them: every record in the hull was appended before the marker, and
/// the export that clears the marker opens after it. Every further failure
/// raises the marker to the current export epoch, because an export already
/// open past the old epoch cannot carry the newly quarantined records.
/// </para>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    /// <summary>
    /// Takes the peer off the log for a batch that could not be encoded, and
    /// quarantines the batch's per-partition hull: from each consumed partition's
    /// cursor through <paramref name="maxReadSeq"/>. Durable before the caller
    /// advances the cursor past the batch.
    /// </summary>
    private async Task QuarantineUnencodableBatchAsync(
        long[] maxReadSeq,
        bool[] advanced,
        int entryCount,
        Exception failure)
    {
        var wasRequired = ReseedRequired;
        var since = state.State.ReseedRequiredSinceUtcTicks;

        // Re-marked on every failure, not only the first: an export opened past
        // the old epoch predates this batch's records and must not clear it.
        var epoch = await TakePeerOffLogStateAsync();
        if (wasRequired)
        {
            state.State.ReseedRequiredSinceUtcTicks = since;
        }
        else
        {
            // A held terminal may belong to a saga that lost a prepare in the
            // batch; the rewind re-reads everything a dropped hold would release.
            _terminalHolds.Clear();
            _prepareTallies.Clear();
            _prepareTallyOrder.Clear();
        }

        var from = state.State.EncodeQuarantineFrom;
        var through = state.State.EncodeQuarantineThrough;
        for (var p = 0; p < _partitionCount && p < advanced.Length; p++)
        {
            if (!advanced[p])
            {
                continue;
            }

            var first = state.State.PartitionCursors.TryGetValue(p, out var cursor) ? Math.Max(0, cursor) : 0;
            from[p] = from.TryGetValue(p, out var existingFrom) ? Math.Min(existingFrom, first) : first;
            through[p] = through.TryGetValue(p, out var existingThrough) ? Math.Max(existingThrough, maxReadSeq[p]) : maxReadSeq[p];
        }

        await state.WriteStateAsync();
        ReportReseedState();

        Logger.LogWarning(
            failure,
            "{Context}: a {EntryCount}-entry batch could not be encoded for peer {Peer}. It is quarantined and the peer is "
            + "taken off the log: saga records are withheld until it is re-seeded from a snapshot export after epoch {Epoch}, "
            + "which carries the batch's writes.",
            LogContext, entryCount, _peerClusterId, epoch);
    }

    /// <summary>Whether <paramref name="sequence"/> of <paramref name="partition"/> is quarantined.</summary>
    private bool IsEncodeQuarantined(int partition, long sequence) =>
        state.State.EncodeQuarantineThrough.Count > 0
        && state.State.EncodeQuarantineThrough.TryGetValue(partition, out var through)
        && sequence <= through
        && state.State.EncodeQuarantineFrom.TryGetValue(partition, out var from)
        && sequence >= from;

    /// <summary>
    /// Clears the quarantine once no re-seed is outstanding and every
    /// quarantined partition's cursor has passed it. Returns whether it cleared.
    /// </summary>
    private bool TryRetireEncodeQuarantine()
    {
        var through = state.State.EncodeQuarantineThrough;
        if (through.Count == 0 || ReseedRequired)
        {
            return false;
        }

        foreach (var (partition, last) in through)
        {
            if (!state.State.PartitionCursors.TryGetValue(partition, out var cursor) || cursor <= last)
            {
                return false;
            }
        }

        ClearEncodeQuarantine();
        return true;
    }

    private void ClearEncodeQuarantine()
    {
        state.State.EncodeQuarantineFrom.Clear();
        state.State.EncodeQuarantineThrough.Clear();
    }

    /// <summary>
    /// Takes the peer off the log for sagas an earlier build poisoned (#4494),
    /// then forgets them: the re-seed delivers each one whole. Runs once per
    /// activation, before the first merge.
    /// </summary>
    private async Task DrainLegacyPoisonedSagasAsync()
    {
        if (state.State.PoisonedSagas.Count == 0)
        {
            return;
        }

        var count = state.State.PoisonedSagas.Count;
        if (!ReseedRequired)
        {
            await TakePeerOffLogStateAsync();
            _terminalHolds.Clear();
            _prepareTallies.Clear();
            _prepareTallyOrder.Clear();
        }

        state.State.PoisonedSagas.Clear();
        await state.WriteStateAsync();
        ReportReseedState();
        Logger.LogWarning(
            "{Context}: {Count} saga(s) an earlier build poisoned for peer {Peer} are delivered by a re-seed instead; the peer "
            + "is taken off the log until it is re-seeded from a snapshot export after epoch {Epoch}.",
            LogContext, count, _peerClusterId, state.State.ReseedRequiredEpoch);
    }
}
