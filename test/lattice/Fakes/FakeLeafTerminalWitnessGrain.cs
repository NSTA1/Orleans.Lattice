using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// In-memory <see cref="ILeafTerminalWitnessGrain"/> that merges exactly as the
/// real sidecar does, for leaf unit tests that need its record to survive a new
/// activation. <see cref="FailWrites"/> makes every write throw.
/// </summary>
internal sealed class FakeLeafTerminalWitnessGrain : ILeafTerminalWitnessGrain
{
    /// <summary>The durable record.</summary>
    public List<AppliedTerminalWitness>? Witnesses { get; private set; }

    /// <summary>Number of writes that changed the record.</summary>
    public int Writes { get; private set; }

    /// <summary>When set, every write throws, as an unavailable provider would.</summary>
    public bool FailWrites { get; set; }

    public Task<AppliedTerminalWitness[]> LoadAsync() =>
        Task.FromResult(Witnesses is { Count: > 0 } witnesses ? witnesses.ToArray() : Array.Empty<AppliedTerminalWitness>());

    public Task ApplyAsync(AppliedTerminalWitness[]? add, Guid[]? remove)
    {
        if (FailWrites)
            return Task.FromException(new TimeoutException("witness sidecar unavailable"));
        Witnesses = LeafTerminalWitnessGrain.Merge(Witnesses, add, remove, out var changed);
        if (changed)
            Writes++;
        return Task.CompletedTask;
    }

    public Task ClearAsync()
    {
        Witnesses = null;
        return Task.CompletedTask;
    }

    /// <summary>Whether the durable record names <paramref name="key"/> under <paramref name="txid"/>.</summary>
    public bool Holds(Guid txid, string key) =>
        Witnesses is not null && Witnesses.Any(w => w.TransactionId == txid && w.Keys.Contains(key));
}
