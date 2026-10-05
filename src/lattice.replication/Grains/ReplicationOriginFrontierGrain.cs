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

    private HybridLogicalClock _lowWatermark;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string Origin => context.GrainId.Key.ToString() ?? string.Empty;

    /// <summary>The source name of <paramref name="treeId"/>'s causal-apply buffer.</summary>
    internal static string BufferSource(string treeId) => BufferSourcePrefix + treeId;

    /// <summary>The source name of <paramref name="treeId"/>'s dead-letter queue.</summary>
    internal static string DeadLetterSource(string treeId) => DeadLetterSourcePrefix + treeId;

    /// <inheritdoc />
    public Task<bool> RecordLowWatermarkAsync(HybridLogicalClock lowWatermark, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (lowWatermark <= _lowWatermark)
        {
            return Task.FromResult(false);
        }

        _lowWatermark = lowWatermark;
        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task<HybridLogicalClock> GetLowWatermarkAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_lowWatermark);
    }

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
            await state.WriteStateAsync().ConfigureAwait(true);
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
            await state.WriteStateAsync().ConfigureAwait(true);
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
    public async Task<CausalDependencyVerdict[]> CheckAsync(IReadOnlyList<HybridLogicalClock> required, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(required);
        cancellationToken.ThrowIfCancellationRequested();

        var verdicts = new CausalDependencyVerdict[required.Count];
        List<(string Source, HybridLogicalClock Identity)>? stale = null;
        for (var i = 0; i < required.Count; i++)
        {
            var t = required[i];
            var lost = state.State.Lost.Contains(t);
            var held = false;
            if (!lost && t < _lowWatermark)
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

            verdicts[i] = CausalFrontierCore.Decide(t, _lowWatermark, held, lost);
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
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch
            {
                // Best effort: an unpersisted drop is re-confirmed on the next check.
            }
        }
    }
}
