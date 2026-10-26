namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class TxRegistryGrain
{
    /// <inheritdoc />
    public async Task<Dictionary<Guid, bool>> HasUndecidedParticipantsManyAsync(IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(txids);
        if (txids.Count > ITxRegistryGrain.MaxMaintenanceProbeBatchSize)
            throw new ArgumentOutOfRangeException(nameof(txids));

        var result = new Dictionary<Guid, bool>(txids.Count);
        foreach (var txid in txids)
            result[txid] = await HasUndecidedParticipantsAsync(txid);
        return result;
    }

    /// <inheritdoc />
    public async Task<bool> IsPurgedAsync(Guid txid)
    {
        while (true)
        {
            var purged = (!state.State.Participants.TryGetValue(txid, out var participants) || participants.Count == 0)
                && (!state.State.Decisions.TryGetValue(txid, out var status)
                    || status is not (TxStatus.Committed or TxStatus.Aborted));
            if (await WhenDurableAsync(txid))
                return purged;
        }
    }
}
