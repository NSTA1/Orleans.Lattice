using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafTerminalWitnessGrain"/> implementation: one persisted
/// <see cref="LeafTerminalWitnessState"/> per leaf, via the lattice storage
/// provider. One read, one merge-and-write, one clear; every decision about what
/// to record lives on the leaf.
/// </summary>
internal sealed class LeafTerminalWitnessGrain(
    [PersistentState("leaf-terminal-witness", LatticeOptions.StorageProviderName)]
    IPersistentState<LeafTerminalWitnessState> state) : Grain, ILeafTerminalWitnessGrain
{
    /// <inheritdoc />
    public Task<AppliedTerminalWitness[]> LoadAsync() =>
        Task.FromResult(state.State.Witnesses is { Count: > 0 } witnesses ? witnesses.ToArray() : []);

    /// <inheritdoc />
    public async Task ApplyAsync(AppliedTerminalWitness[]? add, Guid[]? remove)
    {
        var merged = Merge(state.State.Witnesses, add, remove, out var changed);
        if (!changed)
            return;

        var previous = state.State.Witnesses;
        state.State.Witnesses = merged;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Witnesses = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task ClearAsync()
    {
        if (state.RecordExists || state.State.Witnesses is { Count: > 0 })
        {
            await state.ClearStateAsync();
        }

        state.State = new LeafTerminalWitnessState();
    }

    /// <summary>
    /// The record after unioning <paramref name="add"/> into
    /// <paramref name="existing"/> per saga and dropping <paramref name="remove"/>.
    /// Pure; never mutates its inputs.
    /// </summary>
    internal static List<AppliedTerminalWitness>? Merge(
        List<AppliedTerminalWitness>? existing,
        AppliedTerminalWitness[]? add,
        Guid[]? remove,
        out bool changed)
    {
        changed = false;
        var bySaga = new Dictionary<Guid, (HashSet<string> Keys, long RecordedAtTicks)>();
        if (existing is not null)
        {
            foreach (var witness in existing)
            {
                if (witness.Keys is null)
                    continue;
                bySaga[witness.TransactionId] = (new HashSet<string>(witness.Keys, StringComparer.Ordinal), witness.RecordedAtTicks);
            }
        }

        if (add is not null)
        {
            foreach (var witness in add)
            {
                if (witness.TransactionId == Guid.Empty || witness.Keys is not { Length: > 0 })
                    continue;
                if (!bySaga.TryGetValue(witness.TransactionId, out var entry))
                {
                    entry = (new HashSet<string>(StringComparer.Ordinal), witness.RecordedAtTicks);
                    bySaga[witness.TransactionId] = entry;
                    changed = true;
                }

                foreach (var key in witness.Keys)
                {
                    if (!string.IsNullOrEmpty(key) && entry.Keys.Add(key))
                        changed = true;
                }
            }
        }

        if (remove is not null)
        {
            foreach (var txid in remove)
            {
                if (bySaga.Remove(txid))
                    changed = true;
            }
        }

        if (!changed)
            return existing;
        if (bySaga.Count == 0)
            return null;

        var list = new List<AppliedTerminalWitness>(bySaga.Count);
        foreach (var (txid, entry) in bySaga)
        {
            var keys = new string[entry.Keys.Count];
            entry.Keys.CopyTo(keys);
            list.Add(new AppliedTerminalWitness(txid, keys, entry.RecordedAtTicks));
        }

        return list;
    }
}
