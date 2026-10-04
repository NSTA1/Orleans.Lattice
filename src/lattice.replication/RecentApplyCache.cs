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
/// Correctness is shared with the receiver's other idempotency seams. The cache
/// suppresses recent duplicate identity tuples before the apply grain hop; cache
/// eviction under sustained churn falls through to the leaf-level per-key LWW merge,
/// which makes an identical re-apply a no-op. There is no per-origin HLC drop
/// threshold ahead of the cache (#1060, #4463).
/// </para>
/// <para>
/// A reservation is <em>in flight</em> from <see cref="TryAdd(WalRecord, out bool)"/>
/// until the delivery that took it completes (<see cref="Complete"/>) or is
/// rolled back (<see cref="Remove"/>). A duplicate that finds an in-flight
/// reservation must NOT be acknowledged as applied (#4465): the first delivery
/// can still be aborted - a silo restart mid-apply, or a failure whose
/// transport-level response the sender has already moved past - and an
/// acknowledged duplicate would have moved the sender's cursor past an entry
/// that is then neither applied nor dead-lettered. The applier answers such a
/// duplicate with a deferred, not-accepted ack so the sender keeps its cursor
/// and re-sends. A duplicate of a completed reservation is a genuine
/// re-delivery and is acknowledged as before.
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
    private readonly LinkedList<Slot> _order = new();
    private readonly Dictionary<EntryKey, LinkedListNode<Slot>> _index;
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
        // The cache is bounded - it never exceeds _capacity entries -
        // so a single up-front sizing is exact, not a guess.
        _index = new Dictionary<EntryKey, LinkedListNode<Slot>>(capacity);
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
    /// recorded since the last eviction and, if not, records it as an
    /// in-flight reservation. Equivalent to
    /// <see cref="TryAdd(WalRecord, out bool)"/> discarding the in-flight flag.
    /// </summary>
    /// <param name="entry">The replog entry to dedupe.</param>
    public bool TryAdd(WalRecord entry) => TryAdd(entry, out _);

    /// <summary>
    /// Atomically tests whether the entry's identity tuple has been
    /// recorded since the last eviction and, if not, records it as an
    /// <em>in-flight</em> reservation. Returns <see langword="true"/> when
    /// the tuple was new (i.e. the apply path should proceed);
    /// <see langword="false"/> when the tuple was already present (a
    /// duplicate), in which case <paramref name="duplicateInFlight"/>
    /// reports whether the delivery holding the reservation has not yet
    /// completed. On overflow the oldest tuple is evicted and its
    /// <see cref="LinkedListNode{T}"/> is recycled to host the new
    /// tuple - steady-state miss-with-eviction is allocation-free.
    /// </summary>
    /// <param name="entry">
    /// The replog entry to dedupe. The cache key is built from
    /// <see cref="WalRecord.OriginClusterId"/>,
    /// <see cref="WalRecord.Timestamp"/>, <see cref="WalRecord.Key"/>,
    /// and <see cref="WalRecord.Op"/>; other fields are ignored.
    /// </param>
    /// <param name="duplicateInFlight">
    /// <see langword="true"/> when the tuple was already present and its
    /// reservation is still in flight; <see langword="false"/> otherwise.
    /// </param>
    public bool TryAdd(WalRecord entry, out bool duplicateInFlight)
    {
        var key = EntryKey.From(entry);
        lock (_gate)
        {
            if (_index.TryGetValue(key, out var existing))
            {
                duplicateInFlight = !existing.Value.Completed;
                return false;
            }

            LinkedListNode<Slot> node;
            if (_order.Count >= _capacity)
            {
                // Recycle the oldest node: detach, re-purpose its
                // Value, re-attach at the tail. This eliminates the
                // per-eviction LinkedListNode allocation that would
                // otherwise dominate steady-state apply-path GC churn.
                // Evicting an in-flight reservation is safe: a later
                // duplicate then re-applies idempotently at the leaf.
                node = _order.First!;
                _index.Remove(node.Value.Key);
                _order.RemoveFirst();
                node.Value = new Slot(key, Completed: false);
                _order.AddLast(node);
            }
            else
            {
                node = _order.AddLast(new Slot(key, Completed: false));
            }
            _index[key] = node;

            duplicateInFlight = false;
            return true;
        }
    }

    /// <summary>
    /// Marks the entry's reservation completed: the delivery that took it
    /// has applied the entry (or durably handed it off), so a later
    /// duplicate is a genuine re-delivery and may be acknowledged. A no-op
    /// when the tuple is not present (for example, already evicted).
    /// </summary>
    /// <param name="entry">The replog entry whose reservation to complete.</param>
    public void Complete(WalRecord entry)
    {
        var key = EntryKey.From(entry);
        lock (_gate)
        {
            if (_index.TryGetValue(key, out var node) && !node.Value.Completed)
            {
                node.Value = node.Value with { Completed = true };
            }
        }
    }

    /// <summary>
    /// Removes the entry's identity tuple from the cache if present.
    /// Returns <see langword="true"/> when the tuple was removed;
    /// <see langword="false"/> when the tuple was not present (the
    /// call is idempotent). Used by <see cref="ReplicationApplier"/>
    /// to roll back a <see cref="TryAdd(WalRecord, out bool)"/> reservation when the
    /// subsequent apply fails - without rollback, a transient apply
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
    /// for tests; production callers use <see cref="TryAdd(WalRecord, out bool)"/> for
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

    /// <summary>
    /// Returns <see langword="true"/> when the entry's identity tuple is
    /// retained and its reservation is still in flight. Intended for tests.
    /// </summary>
    public bool IsInFlight(WalRecord entry)
    {
        var key = EntryKey.From(entry);
        lock (_gate)
        {
            return _index.TryGetValue(key, out var node) && !node.Value.Completed;
        }
    }

    private readonly record struct Slot(EntryKey Key, bool Completed);

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