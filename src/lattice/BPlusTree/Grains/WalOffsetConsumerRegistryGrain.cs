using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="IWalOffsetConsumerRegistryGrain"/>. Non-reentrant, so a
/// registration and the GC's read are totally ordered: a GC pass that does not
/// see a consumer read the set before that consumer's registration persisted,
/// and therefore before the consumer read any entry.
/// </summary>
internal sealed class WalOffsetConsumerRegistryGrain(
    IGrainContext context,
    [PersistentState("wal-offset-consumers", LatticeOptions.StorageProviderName)]
    IPersistentState<WalOffsetConsumerRegistryState> state) : IWalOffsetConsumerRegistryGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public Task RegisterAsync(GrainId consumer)
        => state.State.Consumers.Contains(consumer)
            ? Task.CompletedTask
            : MutateAsync(consumers => consumers.Add(consumer));

    /// <inheritdoc />
    public Task UnregisterAsync(GrainId consumer)
        => state.State.Consumers.Contains(consumer)
            ? MutateAsync(consumers => consumers.Remove(consumer))
            : Task.CompletedTask;

    /// <inheritdoc />
    public Task<IReadOnlyList<GrainId>> GetConsumersAsync()
        => Task.FromResult<IReadOnlyList<GrainId>>(state.State.Consumers.ToArray());

    private async Task MutateAsync(Action<List<GrainId>> mutation)
    {
        var previous = state.State.Consumers;
        var next = new List<GrainId>(previous);
        mutation(next);
        state.State.Consumers = next;
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            state.State.Consumers = previous;
            if (GrainStateWriteFaults.IsConflict(ex))
            {
                // Storage holds a set this activation never read; reload it on
                // the next call rather than fail every later write on a stale ETag.
                this.DeactivateOnIdle();
            }

            throw;
        }
    }
}
