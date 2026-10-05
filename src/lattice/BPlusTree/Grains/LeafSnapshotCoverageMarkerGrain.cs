using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILeafSnapshotCoverageMarkerGrain"/> implementation: one
/// persisted <see cref="LeafSnapshotCoverageMarkerState"/> per leaf, via the
/// lattice storage provider. Monotone: it only ever rises, until cleared.
/// </summary>
internal sealed class LeafSnapshotCoverageMarkerGrain(
    [PersistentState("leaf-snapshot-coverage-marker", LatticeOptions.StorageProviderName)]
    IPersistentState<LeafSnapshotCoverageMarkerState> state) : Grain, ILeafSnapshotCoverageMarkerGrain
{
    /// <inheritdoc />
    public Task<long[]?> GetAsync() =>
        Task.FromResult(state.State.CoveredOffsetsByPartition is { } covered ? (long[]?)covered.ToArray() : null);

    /// <inheritdoc />
    public async Task RaiseAsync(long[] covered)
    {
        ArgumentNullException.ThrowIfNull(covered);
        var raised = Raise(state.State.CoveredOffsetsByPartition, covered);
        if (raised is null)
            return;

        var previous = state.State.CoveredOffsetsByPartition;
        state.State.CoveredOffsetsByPartition = raised;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.CoveredOffsetsByPartition = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task ClearAsync()
    {
        if (state.RecordExists || state.State.CoveredOffsetsByPartition is not null)
        {
            await state.ClearStateAsync();
        }

        state.State = new LeafSnapshotCoverageMarkerState();
    }

    /// <summary>
    /// The per-partition maximum of <paramref name="current"/> and
    /// <paramref name="covered"/>, or <see langword="null"/> when nothing rises.
    /// Pure; never mutates its inputs.
    /// </summary>
    internal static long[]? Raise(long[]? current, long[] covered)
    {
        var length = Math.Max(current?.Length ?? 0, covered.Length);
        var result = new long[length];
        var rose = false;
        for (var p = 0; p < length; p++)
        {
            var existing = current is not null && p < current.Length ? current[p] : -1L;
            var offered = p < covered.Length ? covered[p] : -1L;
            result[p] = Math.Max(existing, offered);
            if (result[p] > existing)
                rose = true;
        }

        return rose ? result : null;
    }
}
