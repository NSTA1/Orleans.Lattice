using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable per-tree write-fence primitive for the shard root. A cross-cluster
/// saga (a restore cutover) engages the fence on every shard of the target
/// tree so that, for the bounded cutover window, no post-cut writer can race
/// the cutover. The fence is:
/// <list type="bullet">
///   <item><description><b>Durable</b> - persisted in
///   <see cref="ShardRootState.WriteFenceSagaId"/> /
///   <see cref="ShardRootState.WriteFenceDeadlineTicks"/> so it survives an
///   activation restart.</description></item>
///   <item><description><b>Read-transparent</b> - only the write gates
///   (<c>ThrowIfRejectedForKey</c> / <c>ThrowIfRejectedForAnyKey</c>) consult
///   it, so reads continue while the tree is fenced.</description></item>
///   <item><description><b>Self-lifting</b> - the hot-path gate treats a fence
///   whose <see cref="ShardRootState.WriteFenceDeadlineTicks"/> has passed as
///   lifted, so a coordinator crash mid-saga never strands the tree
///   write-fenced forever even if the explicit lift never
///   arrives.</description></item>
/// </list>
/// The zero-fence steady state costs a single reference-null check on the
/// write path.
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Hot-path write gate. Throws <see cref="LatticeWriteFencedException"/>
    /// when this shard is write-fenced for an as-yet-unexpired saga. No-op on
    /// the steady (unfenced) path and once the fence deadline has passed.
    /// </summary>
    private void ThrowIfWriteFenced()
    {
        var sagaId = state.State.WriteFenceSagaId;
        if (sagaId is null) return;
        if (DateTime.UtcNow.Ticks >= state.State.WriteFenceDeadlineTicks) return;
        throw new LatticeWriteFencedException(
            $"Write to shard '{context.GrainId.Key}' refused: tree '{TreeId}' is write-fenced for "
            + $"cross-cluster saga '{sagaId}'. Retry after the fence lifts.",
            TreeId,
            sagaId);
    }

    /// <inheritdoc />
    public async Task EngageWriteFenceAsync(string sagaId, long deadlineTicks)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        var current = state.State.WriteFenceSagaId;
        if (current is not null
            && !string.Equals(current, sagaId, StringComparison.Ordinal)
            && DateTime.UtcNow.Ticks < state.State.WriteFenceDeadlineTicks)
        {
            throw new InvalidOperationException(
                $"Shard '{context.GrainId.Key}' is already write-fenced for saga '{current}'; "
                + $"cannot engage a fence for saga '{sagaId}' until the current fence lifts.");
        }

        // Idempotent re-engage for the same saga only refreshes the deadline.
        if (string.Equals(current, sagaId, StringComparison.Ordinal)
            && state.State.WriteFenceDeadlineTicks == deadlineTicks)
        {
            return;
        }

        state.State.WriteFenceSagaId = sagaId;
        state.State.WriteFenceDeadlineTicks = deadlineTicks;
        await WriteShardStateAsync();
    }

    /// <inheritdoc />
    public async Task LiftWriteFenceAsync(string sagaId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);

        // A late terminal decision must not clear a newer fence: only lift when
        // the engaged saga matches.
        if (!string.Equals(state.State.WriteFenceSagaId, sagaId, StringComparison.Ordinal))
        {
            return;
        }

        state.State.WriteFenceSagaId = null;
        state.State.WriteFenceDeadlineTicks = 0;
        await WriteShardStateAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsWriteFencedAsync()
    {
        var fenced = state.State.WriteFenceSagaId is not null
            && DateTime.UtcNow.Ticks < state.State.WriteFenceDeadlineTicks;
        return Task.FromResult(fenced);
    }
}
