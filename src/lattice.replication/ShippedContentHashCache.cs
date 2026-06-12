namespace Orleans.Lattice.Replication;

/// <summary>
/// Per-<c>(tree, peer)</c> bounded LRU cache mapping a replicated key to
/// the <see cref="ReplicationContentHash"/> of the most recently shipped
/// value for that key. Drives the sender-side content-hash dedup
/// measurement: <see cref="Observe(string, ulong)"/> returns
/// <see langword="true"/> when a key is shipped with content byte-identical
/// to the value most recently shipped for it, which is the "idempotent
/// re-set" signal the payload-re-send-rate counter
/// (<see cref="LatticeReplicationMetrics.ShipRedundantPayloads"/>)
/// surfaces.
/// <para>
/// The cache is purely an observability seam. It does <b>not</b> elide,
/// reorder, or otherwise alter the bytes shipped on the wire: every
/// entry is still shipped verbatim regardless of what
/// <see cref="Observe(string, ulong)"/> returns, so correctness and the
/// receiver-side last-writer-wins / HLC convergence semantics are
/// unaffected. The full sender-manifest / receiver-pull-missing round
/// trip the issue describes would change the
/// <see cref="IReplicationTransport"/> contract and the on-the-wire
/// framing, which is deferred until wire-version capability negotiation
/// lands; this cache is the largest safe, additive, default-off subset
/// that measures the re-send rate operators need to decide whether the
/// round trip is worth it.
/// </para>
/// <para>
/// The cache is activation-scoped on the shipper grain (one peer per
/// activation), so it is not shared across grain turns concurrently;
/// the internal lock guards against any future shared use and keeps the
/// type safe to unit test from multiple threads.
/// </para>
/// </summary>
internal sealed class ShippedContentHashCache
{
    private readonly object _gate = new();
    private readonly LinkedList<KeyHash> _order = new();
    private readonly Dictionary<string, LinkedListNode<KeyHash>> _index;
    private readonly int _capacity;

    /// <summary>
    /// Creates a cache retaining at most <paramref name="capacity"/>
    /// distinct keys. Eviction is least-recently-shipped first on
    /// overflow.
    /// </summary>
    /// <param name="capacity">
    /// Maximum number of distinct keys retained. Must be at least
    /// <c>1</c>. The replication options validator enforces a floor of
    /// <c>64</c> on the user-facing
    /// <see cref="LatticeReplicationOptions.ContentHashDedupCacheSize"/>
    /// option; this constructor accepts any positive value so unit
    /// tests can exercise eviction at small capacities.
    /// </param>
    public ShippedContentHashCache(int capacity)
    {
        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                "ShippedContentHashCache capacity must be at least 1.");
        }
        _capacity = capacity;
        _index = new Dictionary<string, LinkedListNode<KeyHash>>(capacity);
    }

    /// <summary>The maximum number of distinct keys this cache retains.</summary>
    public int Capacity => _capacity;

    /// <summary>The number of distinct keys currently retained.</summary>
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
    /// Records that <paramref name="key"/> is being shipped with content
    /// digest <paramref name="contentHash"/> and reports whether that
    /// content is a byte-identical re-send of the value most recently
    /// shipped for the same key. Returns <see langword="true"/> when the
    /// key was already present with the same digest (a redundant payload
    /// re-send); <see langword="false"/> when the key is new or its
    /// content changed. The digest is recorded as the latest shipped
    /// content for the key in either case, and the key is promoted to
    /// most-recently-used.
    /// </summary>
    public bool Observe(string key, ulong contentHash)
    {
        ArgumentNullException.ThrowIfNull(key);
        lock (_gate)
        {
            if (_index.TryGetValue(key, out var existing))
            {
                var redundant = existing.Value.Hash == contentHash;
                existing.Value = new KeyHash(key, contentHash);
                _order.Remove(existing);
                _order.AddLast(existing);
                return redundant;
            }

            LinkedListNode<KeyHash> node;
            if (_order.Count >= _capacity)
            {
                // Recycle the least-recently-used node: detach, re-purpose
                // its Value, re-attach at the tail. Steady-state
                // miss-with-eviction is allocation-free.
                node = _order.First!;
                _index.Remove(node.Value.Key);
                _order.RemoveFirst();
                node.Value = new KeyHash(key, contentHash);
                _order.AddLast(node);
            }
            else
            {
                node = _order.AddLast(new KeyHash(key, contentHash));
            }
            _index[key] = node;
            return false;
        }
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="key"/> is
    /// currently retained, without modifying the cache. Intended for
    /// tests.
    /// </summary>
    public bool Contains(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        lock (_gate)
        {
            return _index.ContainsKey(key);
        }
    }

    private readonly record struct KeyHash(string Key, ulong Hash);
}
