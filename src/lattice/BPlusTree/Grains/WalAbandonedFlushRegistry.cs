using System.Runtime.CompilerServices;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Process-wide record of WAL flush windows whose provider call an activation
/// stopped waiting for while it may still land a write (issue #4621), keyed by
/// the provider instance and the shard. A late landing in such a window sits
/// below every offset the shard allocated after it, so the shard's read
/// watermark must not pass the window until the call has settled: until then a
/// reader would be shown the entries above it, advance its cursor past the
/// window, and never see the entry when it lands, while a reader from below
/// would. Kept outside the activation so a reactivation of the shard in the same
/// process, which knows nothing of its predecessor's calls, is bounded too.
/// </summary>
/// <remarks>
/// Once the call settles the window is final: either its entries landed, and
/// the watermark exposes them in order, or it wrote nothing and the hole is
/// permanent, because the allocator has already moved past it. A hole is
/// therefore never filled after a reader has passed it. Readers that rely on
/// offsets being dense must tell a permanent hole from a trim by the shard's
/// lowest retained offset, never by a jump in the offsets they read.
/// </remarks>
internal static class WalAbandonedFlushRegistry
{
    private static readonly ConditionalWeakTable<IWalStorageProvider, ProviderWindows> Providers = new();

    /// <summary>
    /// Returns the abandoned-window record for one shard of one provider,
    /// creating it on first use.
    /// </summary>
    internal static ShardWindows For(IWalStorageProvider provider, string treeId, int shardIndex)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(treeId);
        return Providers.GetValue(provider, static _ => new ProviderWindows()).For(treeId, shardIndex);
    }

    private sealed class ProviderWindows
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<(string Tree, int Shard), ShardWindows> _shards = new();

        internal ShardWindows For(string treeId, int shardIndex)
            => _shards.GetOrAdd((treeId, shardIndex), static _ => new ShardWindows());
    }

    /// <summary>The abandoned flush windows of one shard.</summary>
    internal sealed class ShardWindows
    {
        private readonly List<(long Start, Task Work)> _windows = new();
        private int _count;

        /// <summary>
        /// Records a window starting at <paramref name="startOffset"/> whose provider
        /// call <paramref name="work"/> is still in motion. A settled call is ignored.
        /// </summary>
        internal void Add(long startOffset, Task? work)
        {
            if (work is null || work.IsCompleted)
            {
                return;
            }

            lock (_windows)
            {
                _windows.Add((startOffset, work));
                Volatile.Write(ref _count, _windows.Count);
            }
        }

        /// <summary>
        /// The lowest start offset of a window whose call has not yet settled, or
        /// <see langword="null"/> when none is outstanding. Prunes settled windows.
        /// </summary>
        internal long? LowestUnsettledStart()
        {
            if (Volatile.Read(ref _count) == 0)
            {
                return null;
            }

            lock (_windows)
            {
                _windows.RemoveAll(static w => w.Work.IsCompleted);
                Volatile.Write(ref _count, _windows.Count);
                long? lowest = null;
                foreach (var (start, _) in _windows)
                {
                    if (lowest is not { } current || start < current)
                    {
                        lowest = start;
                    }
                }

                return lowest;
            }
        }
    }
}
