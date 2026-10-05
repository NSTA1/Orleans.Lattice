using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree local vector clock grain. See
/// <see cref="IReplicationHighWaterMarkGrain"/> for the contract.
/// </summary>
internal sealed class ReplicationHighWaterMarkGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    [PersistentState("replication-hwm", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationHighWaterMarkState> state)
    : IReplicationHighWaterMarkGrain, IGrainBase
{
    /// <summary>
    /// The applied write identities this tree remembers (issue #4586): the fast
    /// path of the dependency check. In memory only.
    /// </summary>
    private readonly CausalAppliedIdentityRecord _applied = new();

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string TreeId => context.GrainId.Key.ToString() ?? string.Empty;

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

        // A pin re-seeds the tree from its restored contents (issue #4586): a
        // write recorded as applied before the restore may no longer be there.
        _applied.Clear();

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
            if (ReplicationReceiveDedup.AdvancesHighWaterMark(merged.GetClock(origin), clock))
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
    public async Task<bool> AdvanceAppliedAsync(
        string originClusterId,
        HybridLogicalClock highest,
        IReadOnlyList<HybridLogicalClock> applied,
        bool advanceHighWaterMark,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        ArgumentNullException.ThrowIfNull(applied);
        cancellationToken.ThrowIfCancellationRequested();

        var capacity = options.Get(TreeId).CausalAppliedIdentityCapacity;
        foreach (var identity in applied)
        {
            _applied.Record(originClusterId, identity, capacity);
        }

        return advanceHighWaterMark
            && await TryAdvanceAsync(originClusterId, highest, cancellationToken).ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task<bool> HasAppliedAsync(string originClusterId, HybridLogicalClock timestamp)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        return Task.FromResult(_applied.Contains(originClusterId, timestamp));
    }

    /// <inheritdoc />
    public async Task ResetAppliedIdentitiesAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _applied.Clear();

        // The bootstrap drop floor vouches for the tree's contents, so it goes
        // with them (issue #4549); a failure propagates, so the replacement does
        // not happen with the floor still in force.
        if (state.State.BootstrapFloor is not null)
        {
            await ClearBootstrapFloorAsync(cancellationToken).ConfigureAwait(true);
        }
    }

    /// <inheritdoc />
    public Task<ReplicationApplyAdmission> GetAdmissionAsync(string originClusterId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        var hwm = state.State.Vector.GetClock(originClusterId);
        if (state.State.BootstrapFloor is not { } floor)
        {
            return Task.FromResult(new ReplicationApplyAdmission
            {
                HighWaterMark = hwm,
                HeldBelowFloor = Array.Empty<HybridLogicalClock>(),
            });
        }

        var (lowWatermark, held) = floor.For(originClusterId);
        return Task.FromResult(new ReplicationApplyAdmission
        {
            HighWaterMark = hwm,
            BootstrapFloor = lowWatermark,
            HeldBelowFloor = held,
        });
    }

    /// <inheritdoc />
    public async Task SetBootstrapFloorAsync(
        IReadOnlyDictionary<string, HybridLogicalClock> lowWatermarks,
        IReadOnlyDictionary<string, HybridLogicalClock[]> held,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lowWatermarks);
        ArgumentNullException.ThrowIfNull(held);
        cancellationToken.ThrowIfCancellationRequested();

        var previous = state.State.BootstrapFloor;
        state.State.BootstrapFloor = ReplicationBootstrapFloor.From(lowWatermarks, held);
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            state.State.BootstrapFloor = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task ClearBootstrapFloorAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var previous = state.State.BootstrapFloor;
        if (previous is null)
        {
            return;
        }

        state.State.BootstrapFloor = null;
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            state.State.BootstrapFloor = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public Task RecordLostAsync(string originClusterId, HybridLogicalClock timestamp, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        // Issue #4586: a dependency names an origin's write, not a tree, so lost
        // marks live on the origin's frontier where every tree's check reads them.
        return grainFactory.GetGrain<IReplicationOriginFrontierGrain>(originClusterId)
            .RecordLostAsync([timestamp], cancellationToken);
    }

    /// <inheritdoc />
    public async Task<CausalDependencyVerdict[]> CheckDependenciesAsync(IReadOnlyList<VersionVector> dependencies, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        cancellationToken.ThrowIfCancellationRequested();

        // Fast path: a dependency whose exact identity this tree applied is met.
        // The rest are decided per origin by its frontier, one call per origin.
        var verdicts = new CausalDependencyVerdict[dependencies.Count];
        Dictionary<string, List<(int Vector, HybridLogicalClock Required)>>? misses = null;
        for (var i = 0; i < dependencies.Count; i++)
        {
            verdicts[i] = CausalDependencyVerdict.Met;
            foreach (var (origin, required) in dependencies[i].Entries)
            {
                if (_applied.Contains(origin, required))
                {
                    continue;
                }

                misses ??= new Dictionary<string, List<(int, HybridLogicalClock)>>(StringComparer.Ordinal);
                if (!misses.TryGetValue(origin, out var list))
                {
                    list = new List<(int, HybridLogicalClock)>();
                    misses[origin] = list;
                }

                list.Add((i, required));
            }
        }

        if (misses is null)
        {
            return verdicts;
        }

        foreach (var (origin, list) in misses)
        {
            var required = new HybridLogicalClock[list.Count];
            for (var j = 0; j < list.Count; j++)
            {
                required[j] = list[j].Required;
            }

            var decided = await grainFactory.GetGrain<IReplicationOriginFrontierGrain>(origin)
                .CheckAsync(required, cancellationToken).ConfigureAwait(true);
            for (var j = 0; j < list.Count && j < decided.Length; j++)
            {
                var vector = list[j].Vector;
                verdicts[vector] = Worse(verdicts[vector], decided[j]);
            }
        }

        return verdicts;
    }

    /// <summary>Combines two dependency verdicts: Lost over Unmet over Met.</summary>
    private static CausalDependencyVerdict Worse(CausalDependencyVerdict left, CausalDependencyVerdict right) =>
        left == CausalDependencyVerdict.Lost || right == CausalDependencyVerdict.Lost
            ? CausalDependencyVerdict.Lost
            : left == CausalDependencyVerdict.Unmet || right == CausalDependencyVerdict.Unmet
                ? CausalDependencyVerdict.Unmet
                : CausalDependencyVerdict.Met;

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
