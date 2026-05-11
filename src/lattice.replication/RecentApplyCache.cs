using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Per-tree bounded FIFO cache of recently-applied
/// <see cref="WalRecord"/> identity tuples
/// (<c>(originClusterId, timestamp, key, op)</c>) used by
/// <see cref="ReplicationApplier"/> to drop duplicate-emit pairs that
/// arise when a structural rewrite (shard split / merge / saga
/// compensate) shadow-forwards a user write into a different shard.
/// Both emits ride the WAL with identical
/// <c>(origin, hlc, key, op)</c>; without this cache, a concurrent
/// inbound delivery of the two duplicates can race past the per-origin
/// high-water-mark check (both deliveries observe the same pre-advance
/// HWM and both apply before either advances it). The cache provides
/// the missing in-memory dedupe seam: a successful
/// <see cref="TryAdd"/> wins the race; a losing call short-circuits
/// the apply.
/// <para>
/// Correctness is still bounded by the per-origin HWM. The cache is a
/// fast-path optimisation: it suppresses the duplicate-emit pair
/// before the apply grain hop even when the HWM round-trip would
/// otherwise admit both. Cache eviction under sustained churn cannot
/// cause a re-merge — the HWM check is the authoritative dedupe key
/// and remains in place for any entry the cache has evicted.
/// </para>
/// <para>
/// The cache is per-applier, per-tree; the applier singleton holds a
/// concurrent map of caches, lazily created on first observation of
/// a tree id. Each cache instance is lock-protected for thread safety.
/// </para>
/// </summary>
internal sealed class RecentApplyCache
{
    private readonly object _gate = new();
    private readonly LinkedList<EntryKey> _order = new();
    private readonly Dictionary<EntryKey, LinkedListNode<EntryKey>> _index;
    private readonly int _capacity;

    /// <summary>
    /// Creates a cache with the supplied maximum number of retained
    /// identity tuples. Eviction is FIFO (oldest first) on overflow.
    /// </summary>
    /// <param name="capacity">
    /// Maximum number of identity tuples retained. Must be at least
    /// <c>1</c>. The replication options validator enforces a
    /// floor of <c>64</c> on the user-facing
    /// <see cref="LatticeReplicationOptions.ShadowForwardDedupeCacheSize"/>
    /// option; this constructor accepts any positive value so unit
    /// tests can exercise eviction at small capacities.
    /// </param>
    public RecentApplyCache(int capacity)
    {
        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                "RecentApplyCache capacity must be at least 1.");
        }
        _capacity = capacity;
        // Pre-size the index to the steady-state working set so the
        // fill phase does not pay log(capacity) resize allocations.
        // The cache is bounded — it never exceeds _capacity entries —
        // so a single up-front sizing is exact, not a guess.
        _index = new Dictionary<EntryKey, LinkedListNode<EntryKey>>(capacity);
    }

    /// <summary>The maximum number of identity tuples this cache retains.</summary>
    public int Capacity => _capacity;

    /// <summary>The number of identity tuples currently retained.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _order.Count;
            }
        }
    }

    /// <summary>
    /// Atomically tests whether the entry's identity tuple has been
    /// recorded since the last eviction and, if not, records it.
    /// Returns <see langword="true"/> when the tuple was new (i.e.
    /// the apply path should proceed); <see langword="false"/> when
    /// the tuple was already present (a duplicate emit). On overflow
    /// the oldest tuple is evicted and its
    /// <see cref="LinkedListNode{T}"/> is recycled to host the new
    /// tuple — steady-state miss-with-eviction is allocation-free.
    /// </summary>
    /// <param name="entry">
    /// The replog entry to dedupe. The cache key is built from
    /// <see cref="WalRecord.OriginClusterId"/>,
    /// <see cref="WalRecord.Timestamp"/>, <see cref="WalRecord.Key"/>,

    /// and <see cref="WalRecord.Op"/>; other fields are ignored.
    /// </param>
    public bool TryAdd(WalRecord entry)
    {
        var key = EntryKey.From(entry);
        lock (_gate)
        {
            if (_index.ContainsKey(key))
            {
                return false;
            }

            LinkedListNode<EntryKey> node;
            if (_order.Count >= _capacity)
            {
                // Recycle the oldest node: detach, re-purpose its
                // Value, re-attach at the tail. This eliminates the
                // per-eviction LinkedListNode allocation that would
                // otherwise dominate steady-state apply-path GC churn.
                node = _order.First!;
                _index.Remove(node.Value);
                _order.RemoveFirst();
                node.Value = key;
                _order.AddLast(node);
            }
            else
            {
                node = _order.AddLast(key);
            }
            _index[key] = node;

            return true;
        }
    }

    /// <summary>
    /// Removes the entry's identity tuple from the cache if present.
    /// Returns <see langword="true"/> when the tuple was removed;
    /// <see langword="false"/> when the tuple was not present (the
    /// call is idempotent). Used by <see cref="ReplicationApplier"/>
    /// to roll back a <see cref="TryAdd"/> reservation when the
    /// subsequent apply fails — without rollback, a transient apply
    /// throw would leave a phantom cache entry that suppresses the
    /// transport's retry path and silently drops the entry until
    /// FIFO eviction.
    /// </summary>
    public bool Remove(WalRecord entry)
    {
        var key = EntryKey.From(entry);
        lock (_gate)
        {
            if (!_index.TryGetValue(key, out var node))
            {
                return false;
            }
            _index.Remove(key);
            _order.Remove(node);
            return true;
        }
    }

    /// <summary>
    /// Returns <see langword="true"/> when the entry's identity tuple
    /// is currently retained without modifying the cache. Intended
    /// for tests; production callers use <see cref="TryAdd"/> for
    /// the atomic check-and-record.
    /// </summary>
    public bool Contains(WalRecord entry)
    {
        var key = EntryKey.From(entry);
        lock (_gate)
        {
            return _index.ContainsKey(key);
        }
    }

    private readonly record struct EntryKey(
        string OriginClusterId,
        HybridLogicalClock Timestamp,
        string Key,
        MutationKind Op)
    {
        public static EntryKey From(WalRecord entry) =>
            new(
                entry.OriginClusterId ?? string.Empty,
                entry.Timestamp,
                entry.Key ?? string.Empty,
                entry.Op);
    }
}
