using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="IReplicationExportEpochGrain"/>.</summary>
internal sealed class ReplicationExportEpochGrain(
    [PersistentState("replication-export-epoch", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationExportEpochState> state) : Grain, IReplicationExportEpochGrain
{
    /// <inheritdoc />
    public Task<long> GetAsync() => Task.FromResult(state.State.Epoch);

    /// <inheritdoc />
    public async Task<long> AdvanceAsync()
    {
        state.State.Epoch++;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Epoch--;
            throw;
        }

        return state.State.Epoch;
    }

    /// <summary>How many export cuts are kept; older ones are dropped.</summary>
    internal const int MaxRecordedCuts = 8;

    /// <inheritdoc />
    public async Task RecordCutAsync(ReplicationExportCut cut)
    {
        ArgumentNullException.ThrowIfNull(cut);
        if (cut.Epoch <= 0 || cut.Epoch > state.State.Epoch)
        {
            return;
        }

        var cuts = state.State.Cuts;
        var index = 0;
        while (index < cuts.Count && cuts[index].Epoch < cut.Epoch)
        {
            index++;
        }

        if (index < cuts.Count && cuts[index].Epoch == cut.Epoch)
        {
            return;
        }

        if (cuts.Count >= MaxRecordedCuts && index == 0)
        {
            return;
        }

        var previous = new List<ReplicationExportCut>(cuts);
        cuts.Insert(index, cut);
        while (cuts.Count > MaxRecordedCuts)
        {
            cuts.RemoveAt(0);
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Cuts = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public Task<long?> GetEarliestCoveringEpochAsync(string walTreeId, long[] retainedHeads)
    {
        ArgumentNullException.ThrowIfNull(walTreeId);
        ArgumentNullException.ThrowIfNull(retainedHeads);
        var cuts = state.State.Cuts;
        long? earliest = null;
        var expected = state.State.Epoch;
        for (var i = cuts.Count - 1; i >= 0 && expected > 0; i--)
        {
            var cut = cuts[i];
            if (cut.Epoch != expected || !Covers(cut, walTreeId, retainedHeads))
            {
                break;
            }

            earliest = expected;
            expected--;
        }

        return Task.FromResult(earliest);
    }

    private static bool Covers(ReplicationExportCut cut, string walTreeId, long[] retainedHeads)
    {
        if (!string.Equals(cut.WalTreeId, walTreeId, StringComparison.Ordinal)
            || cut.OpenHeads.Length != retainedHeads.Length
            || retainedHeads.Length == 0)
        {
            return false;
        }

        for (var p = 0; p < retainedHeads.Length; p++)
        {
            if (retainedHeads[p] > cut.OpenHeads[p])
            {
                return false;
            }
        }

        return true;
    }
}
