using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="IWalCopyLogHoldGrain"/> (issue #5471). Non-reentrant for
/// its arm, publish and release verbs; the GC's position read is interleaved and
/// answers from the state those verbs last wrote.
/// </summary>
internal sealed class WalCopyLogHoldGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    [PersistentState("wal-copy-log-hold", LatticeOptions.StorageProviderName)]
    IPersistentState<WalCopyLogHoldState> state) : IWalCopyLogHoldGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>The clock the lease and grace are measured on; tests substitute a controllable one.</summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    /// <summary>The copy's physical tree id: this grain's key.</summary>
    private string CopyTreeId => context.GrainId.Key.ToString()!;

    private long NowTicks => Clock.GetUtcNow().UtcTicks;

    /// <inheritdoc />
    public async Task<bool> ArmAsync(string sourcePhysicalTreeId, TimeSpan lease)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourcePhysicalTreeId);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);

        if (state.State.Published)
        {
            return state.State.Holding;
        }

        if (!state.State.Holding)
        {
            var consumers = await ReadLiveConsumersAsync(sourcePhysicalTreeId);
            if (consumers.Count == 0)
            {
                // Nothing reads the source by offset, so nothing will re-read the copy's
                // log from its start: no hold.
                return false;
            }

            // Registered before the state that answers for it is written, so a GC
            // pass that sees the registration and finds the state not yet holding
            // answers "not reading" only for a copy no builder has written to yet.
            await grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(CopyTreeId).RegisterAsync(context.GrainId);
            state.State.SourcePhysicalTreeId = sourcePhysicalTreeId;
            state.State.Consumers = consumers;
            state.State.Holding = true;
        }
        else if (state.State.ExpiresAtUtcTicks - NowTicks > lease.Ticks / 2)
        {
            // A builder renews on every slice: write only once half the lease is spent.
            return true;
        }

        state.State.ExpiresAtUtcTicks = NowTicks + lease.Ticks;
        await state.WriteStateAsync();
        return true;
    }

    /// <inheritdoc />
    public async Task PublishAsync(TimeSpan grace)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(grace, TimeSpan.Zero);
        if (!state.State.Holding || state.State.Published)
        {
            return;
        }

        state.State.Published = true;
        state.State.ExpiresAtUtcTicks = NowTicks + grace.Ticks;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task ReleaseAsync()
    {
        if (!state.State.Holding)
        {
            return;
        }

        state.State.Holding = false;
        state.State.Consumers = [];
        await state.WriteStateAsync();
        await grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(CopyTreeId).UnregisterAsync(context.GrainId);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Position 0 for every partition (an empty answer, which the GC reads as 0
    /// everywhere) while the hold is in force; <see langword="null"/> otherwise,
    /// and a hold found lapsed or satisfied withdraws itself.
    /// </remarks>
    public async Task<long[]?> GetDurableReadPositionsAsync(string walTreeId)
    {
        if (!state.State.Holding || !string.Equals(walTreeId, CopyTreeId, StringComparison.Ordinal))
        {
            return null;
        }

        if (await HasEndedAsync())
        {
            WithdrawInBackground();
            return null;
        }

        return [];
    }

    /// <inheritdoc />
    public async Task<bool> WithdrawIfEndedAsync()
    {
        if (!state.State.Holding || !await HasEndedAsync())
        {
            return false;
        }

        await ReleaseAsync();
        return true;
    }

    /// <summary>Whether the hold lapsed, or the copy is published and every recorded consumer registered on it.</summary>
    private async Task<bool> HasEndedAsync() =>
        NowTicks >= state.State.ExpiresAtUtcTicks
        || (state.State.Published && await AllConsumersRegisteredOnCopyAsync());

    /// <summary>
    /// The offset consumers of <paramref name="sourcePhysicalTreeId"/> that still
    /// read it. A consumer that answers <see langword="null"/> has moved on; one
    /// that cannot be asked is kept, so the hold errs toward lasting its bound.
    /// Other copies' holds are never waited for: none of them registers on this copy.
    /// </summary>
    private async Task<List<GrainId>> ReadLiveConsumersAsync(string sourcePhysicalTreeId)
    {
        var registered = await grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(sourcePhysicalTreeId)
            .GetConsumersAsync();
        var live = new List<GrainId>(registered.Count);
        foreach (var consumer in registered)
        {
            if (consumer.Type == context.GrainId.Type)
            {
                continue;
            }

            try
            {
                var positions = await grainFactory.GetGrain<IWalOffsetConsumer>(consumer)
                    .GetDurableReadPositionsAsync(sourcePhysicalTreeId);
                if (positions is null)
                {
                    continue;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Unknown is kept: the grace bounds the wait.
            }

            live.Add(consumer);
        }

        return live;
    }

    private async Task<bool> AllConsumersRegisteredOnCopyAsync()
    {
        var registered = await grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(CopyTreeId).GetConsumersAsync();
        foreach (var consumer in state.State.Consumers)
        {
            if (!registered.Contains(consumer))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Withdraws an ended hold through the non-interleaved
    /// <see cref="IWalCopyLogHoldGrain.WithdrawIfEndedAsync"/>, which re-checks
    /// that it has ended, so the withdrawal is ordered with any concurrent arm and
    /// never undoes a renewal. Best effort: until it lands the hold already
    /// answers <see langword="null"/>.
    /// </summary>
    private void WithdrawInBackground() =>
        grainFactory.GetGrain<IWalCopyLogHoldGrain>(CopyTreeId).WithdrawIfEndedAsync().Ignore();
}
