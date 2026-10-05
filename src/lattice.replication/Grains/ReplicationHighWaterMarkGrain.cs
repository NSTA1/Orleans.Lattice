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
        if (!ReplicationReceiveDedup.AdvancesHighWaterMark(current, candidate))
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
            && state.State.PinnedFloor.Entries.Count == 0)
        {
            return;
        }

        var previous = state.State.Vector;
        var previousFloor = state.State.PinnedFloor;
        state.State.Vector = replacement;
        // A pin installs NO drop floor (#4463): no single HLC per origin
        // is downward-closed over what a snapshot holds, so dropping at or
        // below a pinned coordinate silently lost writes the snapshot never
        // contained. Any floor persisted by an earlier build is cleared
        // here, so a silo still on that build (which reads the floor as a
        // drop threshold) stops dropping once this pin lands.
        state.State.PinnedFloor = new VersionVector();
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

    /// <inheritdoc />
    public async Task<bool> MergeBootstrapFrontierAsync(HybridLogicalClock asOfHlc, VersionVector frontier, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(frontier);
        cancellationToken.ThrowIfCancellationRequested();
        _ = asOfHlc; // Reserved for future bootstrap-protocol extensions.

        var merged = state.State.Vector.Clone();
        var raised = false;
        foreach (var (origin, clock) in frontier.Entries)
        {
            if (clock > merged.GetClock(origin))
            {
                merged.Entries[origin] = clock;
                raised = true;
            }
        }

        if (!raised && state.State.PinnedFloor.Entries.Count == 0)
        {
            return false;
        }

        var previous = state.State.Vector;
        var previousFloor = state.State.PinnedFloor;
        state.State.Vector = merged;
        // No drop floor (#4463); clear any floor an earlier build persisted.
        state.State.PinnedFloor = new VersionVector();
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

        return raised;
    }

    /// <inheritdoc />
    public async Task RecordLostAsync(string originClusterId, HybridLogicalClock timestamp, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var lost = state.State.Lost;
        if (!lost.TryGetValue(originClusterId, out var identities))
        {
            identities = new HashSet<HybridLogicalClock>();
            lost[originClusterId] = identities;
        }

        if (!identities.Add(timestamp))
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            identities.Remove(timestamp);
            if (identities.Count == 0)
            {
                lost.Remove(originClusterId);
            }
            throw;
        }
    }

    /// <inheritdoc />
    public Task<CausalDependencyVerdict[]> CheckDependenciesAsync(IReadOnlyList<VersionVector> dependencies, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        cancellationToken.ThrowIfCancellationRequested();

        var verdicts = new CausalDependencyVerdict[dependencies.Count];
        for (var i = 0; i < dependencies.Count; i++)
        {
            var verdict = CausalDependencyVerdict.Met;
            foreach (var (origin, required) in dependencies[i].Entries)
            {
                if (state.State.Lost.TryGetValue(origin, out var lost) && lost.Contains(required))
                {
                    verdict = CausalDependencyVerdict.Lost;
                    break;
                }

                if (state.State.Vector.GetClock(origin) < required)
                {
                    verdict = CausalDependencyVerdict.Unmet;
                }
            }

            verdicts[i] = verdict;
        }

        return Task.FromResult(verdicts);
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
