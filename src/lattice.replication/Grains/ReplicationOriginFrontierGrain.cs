namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Default <see cref="IReplicationOriginFrontierGrain"/>. See the interface for
/// the contract.
/// </summary>
internal sealed class ReplicationOriginFrontierGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    [PersistentState("replication-origin-frontier", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationOriginFrontierState> state)
    : IReplicationOriginFrontierGrain, IGrainBase
{
    /// <summary>Prefix of a causal-apply buffer source.</summary>
    internal const string BufferSourcePrefix = "b|";

    /// <summary>Prefix of a dead-letter queue source.</summary>
    internal const string DeadLetterSourcePrefix = "d|";

    /// <summary>
    /// Prefix of a bootstrap export source: writes the export's source cluster
    /// held without applying when it opened the export, so the tree that
    /// installed the export lacks them until it applies them itself.
    /// </summary>
    internal const string ExportSourcePrefix = "x|";

    /// <summary>
    /// The longest a raised aggregate stays unpersisted while calls keep
    /// arriving. A lagging stored aggregate only delays dependents after a
    /// restart, so it is not written on every shipment.
    /// </summary>
    internal static readonly TimeSpan AggregatePersistInterval = TimeSpan.FromSeconds(5);

    private bool _aggregateDirty;
    private long _aggregatePersistedAt = Environment.TickCount64;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string Origin => context.GrainId.Key.ToString() ?? string.Empty;

    /// <summary>The source name of <paramref name="treeId"/>'s causal-apply buffer.</summary>
    internal static string BufferSource(string treeId) => BufferSourcePrefix + treeId;

    /// <summary>The source name of <paramref name="treeId"/>'s dead-letter queue.</summary>
    internal static string DeadLetterSource(string treeId) => DeadLetterSourcePrefix + treeId;

    /// <summary>The source name of the writes <paramref name="treeId"/>'s last installed export lacked.</summary>
    internal static string ExportSource(string treeId) => ExportSourcePrefix + treeId;

    /// <inheritdoc />
    public async Task<bool> RecordLowWatermarkAsync(HybridLogicalClock lowWatermark, long generation, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var current = state.State;
        if (generation < current.AggregateGeneration || generation < current.MinGeneration)
        {
            return false;
        }

        if (generation == current.AggregateGeneration && lowWatermark <= current.AggregateLowWatermark)
        {
            return false;
        }

        // A newer generation replaces the value, which may lower it: the older
        // generation's aggregate may count a tree's coverage from before its
        // lineage changed.
        current.AggregateLowWatermark = lowWatermark;
        current.AggregateGeneration = generation;
        _aggregateDirty = true;
        if (Environment.TickCount64 - _aggregatePersistedAt >= (long)AggregatePersistInterval.TotalMilliseconds)
        {
            await PersistAggregateAsync().ConfigureAwait(true);
        }

        return true;
    }

    /// <inheritdoc />
    public Task<HybridLogicalClock> GetLowWatermarkAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(EffectiveLowWatermark);
    }

    /// <inheritdoc />
    public async Task SetTreeCapAsync(string treeId, HybridLogicalClock cap, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var caps = state.State.TreeCaps;
        var had = caps.TryGetValue(treeId, out var previous);
        if (had && previous == cap)
        {
            return;
        }

        caps[treeId] = cap;
        try
        {
            await WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            if (had)
            {
                caps[treeId] = previous;
            }
            else
            {
                caps.Remove(treeId);
            }

            throw;
        }
    }

    /// <inheritdoc />
    public async Task LiftTreeCapAsync(string treeId, long generation, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var current = state.State;
        if (!current.TreeCaps.TryGetValue(treeId, out var previousCap))
        {
            return;
        }

        var previousMin = current.MinGeneration;
        var previousValue = current.AggregateLowWatermark;
        var previousGeneration = current.AggregateGeneration;
        current.TreeCaps.Remove(treeId);
        current.MinGeneration = Math.Max(previousMin, generation);
        if (current.AggregateGeneration < current.MinGeneration)
        {
            current.AggregateLowWatermark = HybridLogicalClock.Zero;
            current.AggregateGeneration = current.MinGeneration;
        }

        try
        {
            await WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            current.TreeCaps[treeId] = previousCap;
            current.MinGeneration = previousMin;
            current.AggregateLowWatermark = previousValue;
            current.AggregateGeneration = previousGeneration;
            throw;
        }
    }

    /// <summary>The recorded aggregate, capped by every tree not yet re-covered.</summary>
    private HybridLogicalClock EffectiveLowWatermark
    {
        get
        {
            var effective = state.State.AggregateLowWatermark;
            foreach (var cap in state.State.TreeCaps.Values)
            {
                if (cap < effective)
                {
                    effective = cap;
                }
            }

            return effective;
        }
    }

    /// <summary>Writes the state, which also persists any raised aggregate.</summary>
    private async Task WriteStateAsync()
    {
        await state.WriteStateAsync().ConfigureAwait(true);
        _aggregateDirty = false;
        _aggregatePersistedAt = Environment.TickCount64;
    }

    private async Task PersistAggregateAsync()
    {
        try
        {
            await WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            // Best effort: the raised aggregate stays live and is written again
            // on the next interval or at deactivation; a lagging stored value
            // only delays dependents after a restart.
        }
    }

    /// <inheritdoc />
    public Task OnDeactivateAsync(DeactivationReason reason, CancellationToken token) =>
        _aggregateDirty ? PersistAggregateAsync() : Task.CompletedTask;

    /// <inheritdoc />
    public async Task SetHeldAsync(string source, IReadOnlyCollection<HybridLogicalClock> held, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(source);
        ArgumentNullException.ThrowIfNull(held);
        cancellationToken.ThrowIfCancellationRequested();

        var bySource = state.State.HeldBySource;
        bySource.TryGetValue(source, out var previous);
        if (held.Count == 0 ? previous is null : previous is not null && previous.SetEquals(held))
        {
            return;
        }

        if (held.Count == 0)
        {
            bySource.Remove(source);
        }
        else
        {
            bySource[source] = new HashSet<HybridLogicalClock>(held);
        }

        try
        {
            await WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            if (previous is null)
            {
                bySource.Remove(source);
            }
            else
            {
                bySource[source] = previous;
            }

            throw;
        }
    }

    /// <inheritdoc />
    public async Task RecordLostAsync(IReadOnlyCollection<HybridLogicalClock> lost, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lost);
        cancellationToken.ThrowIfCancellationRequested();

        List<HybridLogicalClock>? added = null;
        foreach (var identity in lost)
        {
            if (state.State.Lost.Add(identity))
            {
                (added ??= new List<HybridLogicalClock>()).Add(identity);
            }
        }

        if (added is null)
        {
            return;
        }

        try
        {
            await WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            foreach (var identity in added)
            {
                state.State.Lost.Remove(identity);
            }

            throw;
        }
    }

    /// <inheritdoc />
    public Task<HybridLogicalClock[]> GetHeldForTreeAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var held = new HashSet<HybridLogicalClock>(state.State.Lost);
        foreach (var source in new[] { BufferSource(treeId), DeadLetterSource(treeId), ExportSource(treeId) })
        {
            if (state.State.HeldBySource.TryGetValue(source, out var identities))
            {
                held.UnionWith(identities);
            }
        }

        return Task.FromResult(held.ToArray());
    }

    /// <inheritdoc />
    public async Task<int> DropExportHeldBelowAsync(string treeId, HybridLogicalClock below, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var source = ExportSource(treeId);
        if (!state.State.HeldBySource.TryGetValue(source, out var identities))
        {
            return 0;
        }

        var dropped = identities.RemoveWhere(identity => identity < below);
        if (identities.Count == 0)
        {
            state.State.HeldBySource.Remove(source);
        }

        if (dropped > 0)
        {
            await WriteStateAsync().ConfigureAwait(true);
        }

        return identities.Count;
    }

    /// <inheritdoc />
    public async Task<CausalDependencyVerdict[]> CheckAsync(IReadOnlyList<HybridLogicalClock> required, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(required);
        cancellationToken.ThrowIfCancellationRequested();

        var verdicts = new CausalDependencyVerdict[required.Count];
        var lowWatermark = EffectiveLowWatermark;
        List<(string Source, HybridLogicalClock Identity)>? stale = null;
        for (var i = 0; i < required.Count; i++)
        {
            var t = required[i];
            var lost = state.State.Lost.Contains(t);
            var held = false;
            if (!lost && t < lowWatermark)
            {
                foreach (var (source, identities) in state.State.HeldBySource)
                {
                    if (!identities.Contains(t))
                    {
                        continue;
                    }

                    if (await IsStillHeldAsync(source, t, cancellationToken).ConfigureAwait(true))
                    {
                        held = true;
                        break;
                    }

                    (stale ??= new List<(string, HybridLogicalClock)>()).Add((source, t));
                }
            }

            verdicts[i] = CausalFrontierCore.Decide(t, lowWatermark, held, lost);
        }

        if (stale is not null)
        {
            await DropStaleAsync(stale).ConfigureAwait(true);
        }

        return verdicts;
    }

    /// <summary>
    /// Asks <paramref name="source"/> whether it still holds the origin's write at
    /// <paramref name="t"/>. The sources answer from their durable state and
    /// interleave, so this cannot deadlock against a source that is itself
    /// waiting on this grain.
    /// </summary>
    private async Task<bool> IsStillHeldAsync(string source, HybridLogicalClock t, CancellationToken cancellationToken)
    {
        if (source.StartsWith(BufferSourcePrefix, StringComparison.Ordinal))
        {
            var tree = source[BufferSourcePrefix.Length..];
            return await grainFactory.GetGrain<ICausalApplyBufferGrain>(tree)
                .IsHoldingAsync(Origin, t).ConfigureAwait(true);
        }

        if (source.StartsWith(DeadLetterSourcePrefix, StringComparison.Ordinal))
        {
            var tree = source[DeadLetterSourcePrefix.Length..];
            return await grainFactory.GetGrain<IReplicationDeadLetterGrain>(tree)
                .IsHoldingAsync(Origin, t, cancellationToken).ConfigureAwait(true);
        }

        if (source.StartsWith(ExportSourcePrefix, StringComparison.Ordinal))
        {
            // Held until the tree applies the write itself. An identity the tree
            // has since forgotten stays held: that only delays the dependent.
            var tree = source[ExportSourcePrefix.Length..];
            return !await grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(tree)
                .HasAppliedAsync(Origin, t).ConfigureAwait(true);
        }

        // An unknown source cannot be confirmed either way: keep treating it as held.
        return true;
    }

    /// <summary>Removes each identity its source confirmed it no longer holds.</summary>
    private async Task DropStaleAsync(List<(string Source, HybridLogicalClock Identity)> stale)
    {
        var changed = false;
        foreach (var (source, identity) in stale)
        {
            if (!state.State.HeldBySource.TryGetValue(source, out var identities))
            {
                continue;
            }

            changed |= identities.Remove(identity);
            if (identities.Count == 0)
            {
                state.State.HeldBySource.Remove(source);
            }
        }

        if (changed)
        {
            try
            {
                await WriteStateAsync().ConfigureAwait(true);
            }
            catch
            {
                // Best effort: an unpersisted drop is re-confirmed on the next check.
            }
        }
    }
}
