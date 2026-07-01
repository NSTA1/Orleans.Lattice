using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree local vector clock grain. See
/// <see cref="IReplicationHighWaterMarkGrain"/> for the contract.
/// </summary>
internal sealed class ReplicationHighWaterMarkGrain(
    [PersistentState("replication-hwm", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationHighWaterMarkState> state)
    : IReplicationHighWaterMarkGrain
{
    /// <inheritdoc />
    public Task<HybridLogicalClock> GetAsync(string originClusterId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(state.State.Vector.GetClock(originClusterId));
    }

    /// <inheritdoc />
    public Task<HybridLogicalClock> GetPinnedFloorAsync(string originClusterId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(state.State.PinnedFloor.GetClock(originClusterId));
    }

    /// <inheritdoc />
    public Task<VersionVector> GetVectorAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Return a defensive copy so callers cannot mutate grain state.
        return Task.FromResult(state.State.Vector.Clone());
    }

    /// <inheritdoc />
    public async Task<bool> TryAdvanceAsync(string originClusterId, HybridLogicalClock candidate, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var current = state.State.Vector.GetClock(originClusterId);
        if (candidate <= current)
        {
            return false;
        }

        state.State.Vector.Entries[originClusterId] = candidate;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Roll the in-memory advance back so a transient storage
            // failure does not leave a phantom HWM that subsequent
            // dedupe checks would surface as if it were persisted.
            if (current == HybridLogicalClock.Zero)
            {
                state.State.Vector.Entries.Remove(originClusterId);
            }
            else
            {
                state.State.Vector.Entries[originClusterId] = current;
            }
            throw;
        }

        return true;
    }

    /// <inheritdoc />
    public async Task PinSnapshotAsync(HybridLogicalClock asOfHlc, VersionVector frontier, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(frontier);
        cancellationToken.ThrowIfCancellationRequested();
        _ = asOfHlc; // Reserved for future bootstrap-protocol extensions.

        // Build a defensive copy so subsequent caller-side mutations to
        // the supplied frontier do not bleed into grain state.
        var replacement = frontier.Clone();
        if (VectorsEqual(state.State.Vector, replacement)
            && VectorsEqual(state.State.PinnedFloor, replacement))
        {
            return;
        }

        var previous = state.State.Vector;
        var previousFloor = state.State.PinnedFloor;
        state.State.Vector = replacement;
        // The pinned floor records the snapshot's causal cut and is the
        // sole per-origin drop threshold the receiver honours. A second
        // clone keeps the floor and the diagonal independently mutable
        // (TryAdvanceAsync raises the diagonal but must never move the
        // floor).
        state.State.PinnedFloor = replacement.Clone();
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Vector = previous;
            state.State.PinnedFloor = previousFloor;
            throw;
        }
    }

    private static bool VectorsEqual(VersionVector left, VersionVector right)
    {
        if (left.Entries.Count != right.Entries.Count)
        {
            return false;
        }

        foreach (var (id, clock) in left.Entries)
        {
            if (!right.Entries.TryGetValue(id, out var other) || other != clock)
            {
                return false;
            }
        }

        return true;
    }
}
