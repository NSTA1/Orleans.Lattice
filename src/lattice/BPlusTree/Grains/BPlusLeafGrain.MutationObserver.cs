using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Invokes the silo-wide <see cref="MutationObserverDispatcher"/> for each
/// durably-committed mutation produced by this leaf. Helpers are fast-no-ops
/// when no <see cref="IMutationObserver"/> is registered, so the write path
/// pays at most one branch check when the hook is unused.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Publishes a <see cref="MutationKind.Set"/> notification for the given
    /// key / committed LWW entry. The <see cref="LatticeMutation.IsTombstone"/>
    /// flag mirrors the entry — a <c>Set</c> may carry a tombstone when an
    /// externally-supplied value loses LWW to an existing tombstone.
    /// </summary>
    private Task PublishSetAsync(string key, LwwValue<byte[]> committed)
    {
        if (!mutationObservers.HasObservers) return Task.CompletedTask;
        var mutation = new LatticeMutation
        {
            TreeId = state.State.TreeId ?? string.Empty,
            Kind = MutationKind.Set,
            Key = key,
            Value = committed.IsTombstone ? null : committed.Value,
            Timestamp = committed.Timestamp,
            IsTombstone = committed.IsTombstone,
            ExpiresAtTicks = committed.ExpiresAtTicks,
        };
        return mutationObservers.PublishAsync(mutation);
    }

    /// <summary>
    /// Publishes a <see cref="MutationKind.Delete"/> notification for the given
    /// key / tombstone entry stamped with the leaf's latest HLC.
    /// </summary>
    private Task PublishDeleteAsync(string key, LwwValue<byte[]> tombstone)
    {
        if (!mutationObservers.HasObservers) return Task.CompletedTask;
        var mutation = new LatticeMutation
        {
            TreeId = state.State.TreeId ?? string.Empty,
            Kind = MutationKind.Delete,
            Key = key,
            Timestamp = tombstone.Timestamp,
            IsTombstone = true,
        };
        return mutationObservers.PublishAsync(mutation);
    }
}
