namespace Orleans.Lattice.Replication;

public partial class ReplicationPeerStats
{
    /// <summary>
    /// Reads one bounded, ordered page of this silo's per-peer telemetry rows for
    /// the peer-status read path. Returns at most
    /// <see cref="ReplicationPeerStatusReadRequest.EffectiveLimit"/> rows that match
    /// the request's filters and order strictly after its cursor, sorted in
    /// <see cref="ReplicationPeerStatusOrder"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Off the shipping hot path.</b> Nothing on the ship or apply path calls
    /// this, and it adds nothing to the recording methods those paths do call. It
    /// walks the state map with the same per-entry monitor the observable gauges
    /// take on every scrape, holds it only to copy five fields, and never allocates
    /// or calls out while holding it, so a concurrent recorder waits at most for a
    /// field copy.
    /// </para>
    /// <para>
    /// <b>Bounded.</b> A row is materialised only when it would enter the current
    /// top <c>limit</c>; a row ordering after the last retained one is rejected
    /// before its entry is touched. Retained memory is bounded by the limit, and
    /// the walk itself is bounded by the number of recorded
    /// <c>(tree, peer, direction)</c> triples.
    /// </para>
    /// </remarks>
    /// <param name="request">The read to perform. Must not be <see langword="null"/>.</param>
    /// <returns>The ordered rows.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
    internal ReplicationPeerStatusRow[] ReadStatusPage(ReplicationPeerStatusReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var limit = request.EffectiveLimit;
        var after = request.After;
        var now = GetTimestamp();
        var selected = new List<ReplicationPeerStatusRow>(Math.Min(limit, state.Count));

        foreach (var kv in state)
        {
            var key = kv.Key;
            if (request.TreeId is not null && !string.Equals(key.Tree, request.TreeId, StringComparison.Ordinal))
            {
                continue;
            }

            if (request.Peer is not null && !string.Equals(key.Peer, request.Peer, StringComparison.Ordinal))
            {
                continue;
            }

            if (after is { } cursor
                && ReplicationPeerStatusOrder.Compare(key.Tree, key.Peer, key.Direction, cursor) <= 0)
            {
                continue;
            }

            if (selected.Count == limit && CompareToRow(key, selected[^1]) >= 0)
            {
                continue;
            }

            long entries, bytes, errors, inFlight;
            DateTimeOffset? lastContact;
            lock (kv.Value)
            {
                entries = kv.Value.EntriesBehind;
                bytes = kv.Value.BytesBehind;
                inFlight = kv.Value.InFlight;
                errors = kv.Value.ConsecutiveErrors;
                lastContact = kv.Value.LastContactTimestamp;
            }

            // Floor at zero - see Snapshot() for the non-monotonic wall-clock rationale.
            var elapsed = lastContact is null
                ? double.NaN
                : Math.Max(0d, (now - lastContact.Value).TotalSeconds);

            var row = new ReplicationPeerStatusRow(
                key.Tree, key.Peer, key.Direction, entries, bytes, errors, elapsed, inFlight);
            selected.Insert(FindInsertIndex(selected, key), row);
            if (selected.Count > limit)
            {
                selected.RemoveAt(selected.Count - 1);
            }
        }

        return selected.ToArray();
    }

    private static int CompareToRow(PeerKey key, in ReplicationPeerStatusRow row) =>
        ReplicationPeerStatusOrder.Compare(key.Tree, key.Peer, key.Direction, row.Tree, row.Peer, row.Direction);

    private static int FindInsertIndex(List<ReplicationPeerStatusRow> selected, PeerKey key)
    {
        var low = 0;
        var high = selected.Count;
        while (low < high)
        {
            var mid = low + ((high - low) / 2);
            if (CompareToRow(key, selected[mid]) > 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        return low;
    }
}
