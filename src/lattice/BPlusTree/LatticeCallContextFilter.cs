using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Outgoing grain call filter that stamps grain-to-grain calls with a
/// <see cref="RequestContext"/> token when the calling grain is a Lattice grain,
/// and clears the token when it is not. Clearing prevents a Lattice-originated
/// token from leaking through a non-Lattice intermediary grain.
/// <para>
/// Uses <see cref="IGrainContextAccessor"/> (lazily resolved to avoid a
/// circular DI dependency) to obtain the current grain instance and performs a
/// direct <see cref="Type.IsAssignableFrom"/> check cached per <see cref="Type"/>
/// in a <see cref="ConcurrentDictionary{TKey,TValue}"/>.
/// </para>
/// </summary>
internal sealed class LatticeCallContextFilter(IServiceProvider serviceProvider) : IOutgoingGrainCallFilter
{
    private IGrainContextAccessor? _contextAccessor;

    private static readonly Type[] LatticeInterfaces =
    [
        typeof(ILattice),
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
        typeof(ITreeShardSplitGrain),
        typeof(IHotShardMonitorGrain),
        typeof(IAtomicWriteGrain),
    ];

    private static readonly ConcurrentDictionary<Type, bool> LatticeTypeCache = new();

    /// <inheritdoc />
    public Task Invoke(IOutgoingGrainCallContext context)
    {
        _contextAccessor ??= serviceProvider.GetRequiredService<IGrainContextAccessor>();
        var grainInstance = _contextAccessor.GrainContext?.GrainInstance;
        if (grainInstance is not null && IsLatticeGrain(grainInstance.GetType()))
        {
            RequestContext.Set(LatticeConstants.InternalCallTokenKey, LatticeConstants.InternalCallTokenValue);
        }
        else
        {
            RequestContext.Set(LatticeConstants.InternalCallTokenKey, null!);
        }

        return context.Invoke();
    }

    private static bool IsLatticeGrain(Type grainType)
    {
        return LatticeTypeCache.GetOrAdd(grainType, static t =>
        {
            foreach (var iface in LatticeInterfaces)
            {
                if (iface.IsAssignableFrom(t))
                    return true;
            }

            return false;
        });
    }
}
