using System.Collections.Concurrent;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Incoming grain call filter that blocks external client calls to Lattice
/// internal grains. Only calls carrying the internal call token set by
/// <see cref="LatticeCallContextFilter"/> are allowed through. Calls to
/// <see cref="ILattice"/> (the public API) are always permitted.
/// </summary>
internal sealed class InternalGrainGuardFilter : IIncomingGrainCallFilter
{
    private static readonly Type[] GuardedInterfaces =
    [
        typeof(IShardRootGrain),
        typeof(IBPlusLeafGrain),
        typeof(IBPlusInternalGrain),
        typeof(ILeafCacheGrain),
        typeof(ILatticeRegistry),
        typeof(ITombstoneCompactionGrain),
        typeof(ITreeDeletionGrain),
        typeof(ITreeResizeGrain),
        typeof(ITreeSnapshotGrain),
        typeof(ITreeMergeGrain),
    ];

    private static readonly ConcurrentDictionary<Type, bool> GuardedTypeCache = new();

    /// <inheritdoc />
    public Task Invoke(IIncomingGrainCallContext context)
    {
        if (context.TargetContext.GrainInstance is { } grainInstance
            && IsGuardedGrain(grainInstance.GetType())
            && !HasInternalCallToken())
        {
            throw new InvalidOperationException(
                $"Direct calls to {context.TargetContext.GrainId.Type} are not allowed. Use ILattice instead.");
        }

        return context.Invoke();
    }

    private static bool IsGuardedGrain(Type grainType)
    {
        return GuardedTypeCache.GetOrAdd(grainType, static t =>
        {
            foreach (var iface in GuardedInterfaces)
            {
                if (iface.IsAssignableFrom(t))
                    return true;
            }

            return false;
        });
    }

    private static bool HasInternalCallToken()
    {
        return RequestContext.Get(LatticeConstants.InternalCallTokenKey) is string value
            && value == LatticeConstants.InternalCallTokenValue;
    }
}
