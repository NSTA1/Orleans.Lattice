using System.Collections.Immutable;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="ICrossTreeHoldTrackerGrain"/>.</summary>
internal sealed class CrossTreeHoldTrackerGrain(
    [PersistentState("replication-cross-tree-hold", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreeHoldTrackerState> state) : Grain, ICrossTreeHoldTrackerGrain
{
    /// <inheritdoc />
    public Task<CrossTreeHoldSnapshot> GetAsync() =>
        Task.FromResult(new CrossTreeHoldSnapshot(
            state.State.Completed,
            state.State.Boundaries.ToImmutableDictionary(StringComparer.Ordinal)));

    /// <inheritdoc />
    public async Task RecordBoundaryAsync(string treeId, CrossTreeHoldBoundary boundary)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(boundary);
        if (state.State.Completed
            || (state.State.Boundaries.TryGetValue(treeId, out var recorded)
                && string.Equals(recorded.PhysicalTreeId, boundary.PhysicalTreeId, StringComparison.Ordinal)))
        {
            return;
        }

        var had = state.State.Boundaries.TryGetValue(treeId, out var previous);
        state.State.Boundaries[treeId] = boundary;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (had) state.State.Boundaries[treeId] = previous!;
            else state.State.Boundaries.Remove(treeId);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task MarkReleasedAsync(string treeId, IReadOnlyCollection<string> participants)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(participants);
        if (state.State.Completed || state.State.Released.Contains(treeId))
        {
            return;
        }

        state.State.Released.Add(treeId);
        var completed = participants.All(state.State.Released.Contains);
        var boundaries = state.State.Boundaries;
        if (completed)
        {
            state.State.Completed = true;
            state.State.Boundaries = new(StringComparer.Ordinal);
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Released.Remove(treeId);
            state.State.Completed = false;
            state.State.Boundaries = boundaries;
            throw;
        }
    }
}
