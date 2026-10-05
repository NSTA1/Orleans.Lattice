using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Decision stamps on shipped cross-tree terminals (issue #4684). A receiver's
/// cross-tree barrier records a participant's arrival from an import that named
/// the operation nowhere only when the import's export opened after the
/// operation's decision, which it judges against the decision stamps. Every
/// terminal of an operation therefore carries the full stamp vector, read from
/// the sub-saga's cross-tree membership in the tree's transaction registry: the
/// coordinator records the stamps there before any participant finalizes, so
/// before the terminal was appended. The stamps ride on the wire copy only; the
/// write-ahead log is never rewritten.
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    private const int MaxCrossTreeStampCacheEntries = 4096;

    // The stamps of a cross-tree sub-saga, by transaction id; null for one
    // whose membership carries none (decided before stamping, or no longer
    // stored). Bounded: cleared wholesale when full.
    private readonly Dictionary<Guid, IReadOnlyDictionary<string, long>?> _crossTreeStamps = new();

    /// <summary>
    /// Stamps every cross-tree terminal in the drained batch with its
    /// operation's decision stamps, re-encoding its wire copy. A failed lookup
    /// fails the tick: shipping a stamped operation's terminal without its
    /// stamps would read, at the receiver, as an operation decided before every
    /// export.
    /// </summary>
    private async Task StampCrossTreeTerminalsAsync()
    {
        List<Guid>? unknown = null;
        var any = false;
        for (var i = 0; i < _drainBuffer.Count; i++)
        {
            var entry = _drainBuffer[i];
            if (!IsCrossTreeTerminal(in entry))
            {
                continue;
            }

            any = true;
            if (!_crossTreeStamps.ContainsKey(entry.TransactionId))
            {
                (unknown ??= []).Add(entry.TransactionId);
            }
        }

        if (!any)
        {
            return;
        }

        if (unknown is not null)
        {
            if (_crossTreeStamps.Count + unknown.Count > MaxCrossTreeStampCacheEntries)
            {
                _crossTreeStamps.Clear();
            }

            var byRegistry = new Dictionary<GrainId, (ITxRegistryGrain Registry, List<Guid> Txids)>();
            foreach (var txid in unknown.Distinct())
            {
                var registry = TxRegistryRouting.GetRegistry(_grainFactory, _treeName, txid);
                var id = registry.GetGrainId();
                if (!byRegistry.TryGetValue(id, out var group))
                {
                    group = (registry, []);
                    byRegistry[id] = group;
                }

                group.Txids.Add(txid);
            }

            foreach (var (registry, txids) in byRegistry.Values)
            {
                var memberships = await registry.GetCrossTreeMembershipsAsync(txids);
                foreach (var txid in txids)
                {
                    _crossTreeStamps[txid] = memberships.TryGetValue(txid, out var membership) ? membership.DecisionStamps : null;
                }
            }
        }

        for (var i = 0; i < _drainBuffer.Count; i++)
        {
            var entry = _drainBuffer[i];
            if (!IsCrossTreeTerminal(in entry)
                || !_crossTreeStamps.TryGetValue(entry.TransactionId, out var stamps)
                || stamps is null)
            {
                continue;
            }

            var stamped = entry with { CrossTreeDecisionStamps = stamps };
            var writer = _coalesceReencodeWriter ??= new System.Buffers.ArrayBufferWriter<byte>();
            writer.Clear();
            _walRecordEncoder.Encode(in stamped, writer);
            var segment = new ArraySegment<byte>(writer.WrittenSpan.ToArray());
            _drainEncodedByteCount += segment.Count - _drainEncodedSegments[i].Count;
            _drainBuffer[i] = stamped;
            _drainEncodedSegments[i] = segment;
        }
    }

    private static bool IsCrossTreeTerminal(in WalRecord entry) =>
        entry.Op is MutationKind.TxCommit or MutationKind.TxAbort
        && !string.IsNullOrEmpty(entry.CrossTreeOperationId)
        && entry.TransactionId != Guid.Empty
        && entry.CrossTreeDecisionStamps is null;
}
