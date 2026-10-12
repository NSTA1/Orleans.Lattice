namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task<List<LwwEntry?>> GetVisibleRawEntriesAsync(List<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        await AwaitReplayBarrierAsync();
        EnsureInternalOrigin(LatticeOperation.Read);

        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        var result = new List<LwwEntry?>(keys.Count);
        foreach (var key in keys)
        {
            if (IsKeyMovedAway(key))
            {
                throw new StaleShardRoutingException(-1, -1, -1);
            }

            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = ResolveReadOutcome(outcomes, pending.txid, pendingKeys.Count);
                // Metadata readers need committed tombstones and expired rows too.
                var visibility = AtomicVisibilityGate.ResolveKey(
                    status, IsRecentlyTerminal(pending.txid), preparedHiddenByTombstoneOrExpiry: false);
                if (visibility == PendingReadOutcome.SurfacePrepared)
                {
                    result.Add(new LwwEntry(key, pending.value));
                    continue;
                }

                if (visibility == PendingReadOutcome.Hidden)
                {
                    result.Add(null);
                    continue;
                }
            }

            if (!Cache.TryGetRow(key, out var row))
            {
                result.Add(null);
                continue;
            }

            if (row.IsMigrated && TryGetShadowedSagas(key, out var shadowSagas)
                && !await IsShadowedReadSafeAsync(key, row.Timestamp, shadowSagas))
            {
                throw new StaleShardRoutingException(-1, -1, -1);
            }

            result.Add(new LwwEntry(key, row, Cache.GetMergeMode(key)));
        }

        return result;
    }
}
