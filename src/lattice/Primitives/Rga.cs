namespace Orleans.Lattice;

using System.Runtime.InteropServices;

/// <summary>
/// A Replicated Growable Array (RGA) CRDT for collaborative ordered
/// lists / text. Each call to
/// <see cref="InsertAfter(OrSetDot, string, byte[])"/> mints a fresh
/// causal dot and links the new value under the supplied parent dot
/// (or the virtual root); concurrent inserts under the same parent
/// converge on a deterministic order via the descending
/// <c>(Counter, ReplicaId)</c> tie-break applied at materialise time.
/// <see cref="Remove(OrSetDot)"/> tombstones a node but preserves it
/// for causal stability so a re-insert after the same parent still
/// resolves correctly. <see cref="Merge(Rga, Rga)"/> is commutative,
/// associative, and idempotent.
/// <para>
/// State shape: every node (live or tombstoned) is stored in
/// <see cref="Nodes"/> keyed by its dot identity. The materialised
/// <c>index -&gt; dot</c> projection produced by <see cref="ToList"/>
/// (an in-order traversal of the parent / child tree) is cached and
/// reused across reads; every mutation path invalidates the cache so a
/// subsequent read rebuilds it lazily. The cache is transient (never
/// serialized) and is rebuilt on first read after deserialization.
/// <see cref="NextCounter"/> reads the per-replica highest counter in
/// O(1) from the serialized <see cref="Context"/> dot-context cache
/// instead of rescanning every node, so a bulk build is O(N) rather
/// than O(N^2).
/// </para>
/// <para>
/// Values are opaque <see cref="byte"/> arrays; the typed
/// <see cref="RgaAccessor{T}"/> serialises domain values through an
/// injectable <see cref="ILatticeSerializer{T}"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.Rga)]
public sealed class Rga : ICrdt<Rga>
{
    // Below this many incoming nodes (MergeFrom) or delta operations
    // (MergeDelta) a linear scan over the node list beats allocating and
    // filling a full dot->node index over every local node. Steady-state
    // replication delivers one or two operations per merge, so the linear
    // path is the common case; the index is built only for a large
    // catch-up merge. Mirrors the DotLinearScanThreshold fast path the
    // sibling OrSet / OrMap / OrFlag primitives already use.
    private const int MergeLinearScanThreshold = 4;

    /// <summary>
    /// Every node in the sequence, live or tombstoned, in arbitrary
    /// storage order. Materialise via <see cref="ToList"/> for the
    /// deterministic in-order projection.
    /// </summary>
    [Id(0)]
    public List<RgaNode> Nodes { get; set; } = new();

    /// <summary>
    /// Dot context: per-replica highest counter ever minted or observed
    /// for that replica across every node in the sequence (live or
    /// tombstoned). Lets <see cref="NextCounter"/> mint a fresh dot in
    /// O(1) instead of rescanning every node on every
    /// <see cref="InsertAfter(OrSetDot, string, byte[])"/>, so a bulk
    /// build is O(N) rather than O(N^2).
    /// <para>
    /// This is a serialized cache, not a semantic witness: it never
    /// influences the lattice merge (which unions nodes by dot), only the
    /// counter chosen for the next local insert. Every mutator
    /// (<see cref="InsertAfter(OrSetDot, string, byte[])"/>,
    /// <see cref="MergeFrom(Rga)"/>, <see cref="MergeDelta(RgaDelta)"/>,
    /// <see cref="Clone"/>) keeps it consistent, and it is rebuilt lazily
    /// from the nodes on the first insert after loading a legacy payload
    /// that predates this field (older payloads deserialize it as empty,
    /// which is backward compatible).
    /// </para>
    /// </summary>
    [Id(1)]
    public Dictionary<string, long> Context { get; set; } = [];

    /// <summary>
    /// Transient cache of the last <see cref="ToList"/> materialisation.
    /// Never serialized (no <c>[Id]</c>): a deserialized or cloned
    /// sequence starts with a <c>null</c> cache and rebuilds it on first
    /// read. Set to <c>null</c> by every mutation path so the next read
    /// re-materialises.
    /// </summary>
    [NonSerialized]
    private IReadOnlyList<(OrSetDot Dot, byte[] Value)>? _materializedCache;

    /// <summary>
    /// Transient live-node counter: the number of un-tombstoned nodes, or
    /// <c>null</c> when it must be rebuilt from <see cref="Nodes"/> on the
    /// next read. Never serialized (no <c>[Id]</c>): a deserialized or
    /// cloned sequence starts <c>null</c> and rebuilds on first read - so a
    /// legacy payload (empty here but non-empty <see cref="Nodes"/>) is
    /// backward compatible, exactly like <see cref="_materializedCache"/>.
    /// The simple mutators keep it consistent in O(1)
    /// (<see cref="InsertAfter(OrSetDot, string, byte[])"/> increments,
    /// <see cref="Remove(OrSetDot)"/> decrements); the O(N) merge paths
    /// (<see cref="MergeFrom(Rga)"/>, <see cref="MergeDelta(RgaDelta)"/>)
    /// null it so the next read rebuilds, adding no asymptotic cost to a
    /// merge that already walks every node. Derived, never a merge witness.
    /// </summary>
    [NonSerialized]
    private int? _liveCount;

    /// <summary>The empty dot used to represent the virtual sequence root (parent of top-level inserts).</summary>
    public static OrSetDot Root => default;

    /// <summary>Returns <c>true</c> when no live (un-tombstoned) node remains.</summary>
    public bool IsEmpty => LiveCount() == 0;

    /// <inheritdoc />
    /// <remarks>
    /// An <see cref="Rga"/> is bottom when no live node remains.
    /// Tombstoned nodes may still be present and are preserved for
    /// causal-history purposes; a containing composite treats the
    /// slot as empty.
    /// </remarks>
    public bool IsBottom => IsEmpty;

    /// <summary>Returns the number of live (un-tombstoned) nodes.</summary>
    public int Count => LiveCount();

    /// <summary>
    /// Returns the live-node count in O(1) from <see cref="_liveCount"/>,
    /// rebuilding it with a single O(N) scan the first time it is read
    /// after construction, deserialization, a clone, or a merge.
    /// </summary>
    private int LiveCount()
    {
        if (_liveCount is { } cached) return cached;
        var n = 0;
        foreach (var node in Nodes)
        {
            if (!node.IsTombstone) n++;
        }
        _liveCount = n;
        return n;
    }

    /// <summary>
    /// Inserts <paramref name="value"/> as a new child of
    /// <paramref name="parentDot"/> (or the virtual root when
    /// <paramref name="parentDot"/> equals <see cref="Root"/>),
    /// minting a fresh dot
    /// <c>(<paramref name="replicaId"/>, NextCounter)</c> where
    /// <c>NextCounter</c> is one greater than the highest counter
    /// observed for <paramref name="replicaId"/> across every node
    /// in the sequence. Returns the new node's dot.
    /// </summary>
    /// <param name="parentDot">The parent dot to link under, or <see cref="Root"/> for a top-level insert.</param>
    /// <param name="replicaId">The replica authoring the insert. Must be non-empty.</param>
    /// <param name="value">The value bytes to attach. Must not be <c>null</c>.</param>
    public OrSetDot InsertAfter(OrSetDot parentDot, string replicaId, byte[] value)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);

        var counter = NextCounter(replicaId);
        var node = new RgaNode
        {
            ReplicaId = replicaId,
            Counter = counter,
            ParentDot = parentDot,
            Value = value,
            IsTombstone = false,
        };
        Nodes.Add(node);

        // counter is strictly greater than any prior counter for this
        // replica, so record it as the new per-replica maximum.
        Context[replicaId] = counter;
        // A fresh insert always adds one live node; keep the counter O(1).
        if (_liveCount is { } live) _liveCount = live + 1;
        InvalidateMaterializedCache();
        return node.Dot;
    }

    /// <summary>
    /// Tombstones the node identified by <paramref name="dot"/>. A
    /// no-op when the dot is not present or already tombstoned.
    /// Returns <c>true</c> when a live node was newly tombstoned.
    /// </summary>
    public bool Remove(OrSetDot dot)
    {
        for (var i = 0; i < Nodes.Count; i++)
        {
            var n = Nodes[i];
            if (n.Counter == dot.Counter && string.Equals(n.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
            {
                if (n.IsTombstone) return false;
                n.IsTombstone = true;
                // A live node just became tombstoned; keep the counter O(1).
                if (_liveCount is { } live) _liveCount = live - 1;
                InvalidateMaterializedCache();
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="dot"/> identifies a
    /// node that has been authored against this sequence (live or
    /// tombstoned).
    /// </summary>
    public bool ContainsDot(OrSetDot dot)
    {
        foreach (var n in Nodes)
        {
            if (n.Counter == dot.Counter && string.Equals(n.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Materialises the sequence into the deterministic in-order
    /// projection: a depth-first walk from <see cref="Root"/> in which
    /// sibling children of any parent are visited in descending
    /// <c>(Counter, ReplicaId)</c> order. Tombstoned nodes are
    /// traversed (so their descendants still resolve) but are not
    /// emitted to the result.
    /// </summary>
    /// <returns>
    /// The live node dots and values in insertion-resolved order.
    /// Tuple element <c>Dot</c> is the stable cursor identity that
    /// callers can use as a parent for subsequent
    /// <see cref="InsertAfter(OrSetDot, string, byte[])"/> calls.
    /// </returns>
    public IReadOnlyList<(OrSetDot Dot, byte[] Value)> ToList()
    {
        if (_materializedCache is { } cached) return cached;
        var nodes = Nodes;
        var n = nodes.Count;
        if (n == 0) return _materializedCache = Array.Empty<(OrSetDot, byte[])>();

        // Build the parent -> children index as a flat compressed layout
        // instead of a Dictionary<OrSetDot, List<RgaNode>>. The previous
        // shape allocated one List per distinct parent, which for the
        // dominant collaborative-text pattern (a linear chain where every
        // parent has exactly one child) meant one throwaway list object per
        // node on every uncached rebuild. Here a single dot->index map plus
        // a handful of pooled-shape int[] arrays (counting-sort buckets, the
        // scatter cursor, and the DFS stack) carry the whole traversal, so
        // the rebuild's allocation profile no longer scales with the number
        // of parents. The resolved order is identical: siblings under a
        // shared parent are still sorted descending by (Counter, ReplicaId),
        // and the pre-order DFS still emits live nodes in the same sequence.
        // Slot index n is the virtual Root; a node maps to slot n when its
        // parent is Root, to its parent's storage index when the parent is a
        // known node, or to -1 (excluded, never reachable from Root) for an
        // orphan whose parent is absent - exactly the nodes the old DFS never
        // visited.
        var indexByDot = new Dictionary<OrSetDot, int>(n);
        for (var i = 0; i < n; i++) indexByDot[nodes[i].Dot] = i;

        var parentSlot = new int[n];
        var starts = new int[n + 2];
        for (var i = 0; i < n; i++)
        {
            var parent = nodes[i].ParentDot;
            int slot;
            if (parent == default) slot = n;
            else if (indexByDot.TryGetValue(parent, out var pj)) slot = pj;
            else slot = -1;
            parentSlot[i] = slot;
            // Tally into starts[slot + 1] so the prefix sum below turns the
            // per-slot counts directly into exclusive start offsets.
            if (slot >= 0) starts[slot + 1]++;
        }

        for (var s = 0; s <= n; s++) starts[s + 1] += starts[s];
        var totalChildren = starts[n + 1];

        // Scatter each node index into its parent's contiguous run. A cursor
        // seeded from the start offsets keeps the fill append-only per slot.
        var childrenFlat = new int[totalChildren];
        var cursor = new int[n + 1];
        Array.Copy(starts, cursor, n + 1);
        for (var i = 0; i < n; i++)
        {
            var slot = parentSlot[i];
            if (slot >= 0) childrenFlat[cursor[slot]++] = i;
        }

        // Sort each parent's run into descending (Counter, ReplicaId) sibling
        // order. The struct comparer is passed by value through the generic
        // Span.Sort overload, so the whole rebuild allocates no ordering
        // delegate.
        var comparer = new SiblingComparer(nodes);
        for (var s = 0; s <= n; s++)
        {
            var start = starts[s];
            var len = starts[s + 1] - start;
            if (len > 1) childrenFlat.AsSpan(start, len).Sort(comparer);
        }

        var result = new List<(OrSetDot, byte[])>(n);
        // Iterative DFS over an int index stack (bounded by the child count,
        // which never exceeds n) to avoid stack overflows on deep histories.
        var stack = new int[n];
        var top = 0;
        for (var k = starts[n + 1] - 1; k >= starts[n]; k--) stack[top++] = childrenFlat[k];
        while (top > 0)
        {
            var i = stack[--top];
            var node = nodes[i];
            if (!node.IsTombstone) result.Add((node.Dot, node.Value));
            for (var k = starts[i + 1] - 1; k >= starts[i]; k--) stack[top++] = childrenFlat[k];
        }
        return _materializedCache = result;
    }

    /// <summary>
    /// Lattice merge: unions the two sides' node sets by dot. A
    /// node tombstoned on either side is tombstoned in the result
    /// (tombstone is monotonic). Same-dot value collisions resolve
    /// deterministically by keeping the larger byte array under
    /// ordinal comparison; in practice dots are unique per
    /// (replica, counter) so collisions only arise from a
    /// transport/forgery error, not from normal authoring.
    /// </summary>
    public static Rga Merge(Rga left, Rga right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <inheritdoc />
    public void MergeFrom(Rga other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (other.Nodes.Count == 0) return;

        // For a small incoming side (the steady-state replication merge)
        // probe the node list directly; only a large catch-up merge builds
        // the full dot->node index over every local node.
        Dictionary<OrSetDot, RgaNode>? localByDot = null;
        if (other.Nodes.Count > MergeLinearScanThreshold)
        {
            localByDot = new Dictionary<OrSetDot, RgaNode>(Nodes.Count);
            foreach (var n in Nodes) localByDot[n.Dot] = n;
        }

        RgaNode? Lookup(OrSetDot dot)
        {
            if (localByDot is not null) return localByDot.TryGetValue(dot, out var found) ? found : null;
            foreach (var n in Nodes)
            {
                if (n.Counter == dot.Counter && n.ReplicaId == dot.ReplicaId) return n;
            }
            return null;
        }

        foreach (var n in other.Nodes)
        {
            if (Lookup(n.Dot) is { } existing)
            {
                // Tombstone is monotonic: once observed-removed on
                // either side, the result is tombstoned.
                if (n.IsTombstone) existing.IsTombstone = true;
                // Same-dot value collision: pick the lexicographically
                // larger byte sequence so the choice is deterministic
                // and self-inverse under repeated merges.
                if (!ReferenceEquals(existing.Value, n.Value)
                    && CompareBytes(existing.Value, n.Value) < 0)
                {
                    existing.Value = n.Value;
                }
            }
            else
            {
                var added = new RgaNode
                {
                    ReplicaId = n.ReplicaId,
                    Counter = n.Counter,
                    ParentDot = n.ParentDot,
                    Value = n.Value,
                    IsTombstone = n.IsTombstone,
                };
                Nodes.Add(added);
                localByDot?[n.Dot] = added;
            }
        }

        MergeContextFrom(other);
        // A merge can flip nodes live<->tombstoned and add nodes on either
        // side; rebuild the counter lazily on the next read rather than
        // tracking every transition here (the merge is already O(N)).
        _liveCount = null;
        InvalidateMaterializedCache();
    }

    /// <summary>
    /// Folds an <see cref="RgaDelta"/> into this sequence. Inserts are
    /// added as live nodes keyed by their dot identity (idempotent on
    /// duplicate delivery); a node already present has its structural
    /// info (<see cref="RgaNode.ParentDot"/> and a non-empty
    /// <see cref="RgaNode.Value"/>) refreshed from the insert while its
    /// tombstone flag is preserved (tombstone is monotonic). Tombstones
    /// mark the matching node tombstoned, creating a tombstoned
    /// placeholder when the node has not yet been observed so a
    /// later-arriving insert for the same dot reattaches its parent.
    /// <para>
    /// The merge is commutative, associative, and idempotent: replaying
    /// the same delta, or applying inserts and tombstones in any order,
    /// converges to the same node set. Steady-state replication delivers
    /// an insert before the matching tombstone (the producer-side commit
    /// of the insert carries a strictly-earlier HLC than the remove, and
    /// the causal-plus dependency gate preserves that order), so the
    /// placeholder branch is a robustness backstop rather than the
    /// common path.
    /// </para>
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Null
    /// inner collections are treated as empty.
    /// </param>
    public void MergeDelta(RgaDelta delta)
    {
        var insertCount = delta.Inserts?.Count ?? 0;
        var tombstoneCount = delta.Tombstones?.Count ?? 0;

        // For a small incoming delta (the steady-state replication case)
        // probe the node list directly; only a large catch-up delta builds
        // the full dot->node index over every local node.
        Dictionary<OrSetDot, RgaNode>? byDot = null;
        if (insertCount + tombstoneCount > MergeLinearScanThreshold)
        {
            byDot = new Dictionary<OrSetDot, RgaNode>(Nodes.Count);
            foreach (var n in Nodes) byDot[n.Dot] = n;
        }

        RgaNode? Lookup(OrSetDot dot)
        {
            if (byDot is not null) return byDot.TryGetValue(dot, out var found) ? found : null;
            foreach (var n in Nodes)
            {
                if (n.Counter == dot.Counter && n.ReplicaId == dot.ReplicaId) return n;
            }
            return null;
        }

        var inserts = delta.Inserts;
        if (inserts is { Count: > 0 })
        {
            foreach (var ins in inserts)
            {
                var dot = ins.Dot;
                BumpContext(ins.ReplicaId, ins.Counter);
                if (Lookup(dot) is { } existing)
                {
                    // Idempotent refresh: the insert is authoritative for
                    // the structural parent link and value; the tombstone
                    // flag is monotonic and never cleared here.
                    existing.ParentDot = ins.ParentDot;
                    if (ins.Value is { Length: > 0 }) existing.Value = ins.Value;
                }
                else
                {
                    var node = new RgaNode
                    {
                        ReplicaId = ins.ReplicaId,
                        Counter = ins.Counter,
                        ParentDot = ins.ParentDot,
                        Value = ins.Value ?? Array.Empty<byte>(),
                        IsTombstone = false,
                    };
                    Nodes.Add(node);
                    byDot?[dot] = node;
                }
            }
        }

        var tombstones = delta.Tombstones;
        if (tombstones is { Count: > 0 })
        {
            foreach (var dot in tombstones)
            {
                BumpContext(dot.ReplicaId, dot.Counter);
                if (Lookup(dot) is { } node)
                {
                    node.IsTombstone = true;
                }
                else
                {
                    // Tombstone observed before its insert (out-of-order or
                    // partial delivery). Record a tombstoned placeholder so
                    // the merge stays total and a later insert for the same
                    // dot reattaches the correct parent and value.
                    var placeholder = new RgaNode
                    {
                        ReplicaId = dot.ReplicaId,
                        Counter = dot.Counter,
                        ParentDot = Root,
                        Value = Array.Empty<byte>(),
                        IsTombstone = true,
                    };
                    Nodes.Add(placeholder);
                    byDot?[dot] = placeholder;
                }
            }
        }

        // A delta can add live inserts and tombstone existing nodes;
        // rebuild the counter lazily on the next read (the fold is O(N)).
        _liveCount = null;
        InvalidateMaterializedCache();
    }

    /// <summary>Creates a deep copy of this sequence (every node is duplicated; value byte arrays are referenced as-is).</summary>
    public Rga Clone()
    {
        var copy = new Rga();
        copy.Nodes.Capacity = Nodes.Count;
        foreach (var n in Nodes)
        {
            copy.Nodes.Add(new RgaNode
            {
                ReplicaId = n.ReplicaId,
                Counter = n.Counter,
                ParentDot = n.ParentDot,
                Value = n.Value,
                IsTombstone = n.IsTombstone,
            });
        }
        // Copy the maintained dot-context cache so the clone mints its
        // next counter in O(1) without a rebuild.
        copy.Context = new Dictionary<string, long>(Context);
        // Carry over the live-node counter (may be null = rebuild on read).
        copy._liveCount = _liveCount;
        return copy;
    }

    private long NextCounter(string replicaId)
    {
        EnsureContextRebuilt();
        return (Context.TryGetValue(replicaId, out var current) ? current : 0) + 1;
    }

    /// <summary>
    /// Rebuilds <see cref="Context"/> from the nodes the first time it is
    /// needed on a sequence loaded from a legacy payload that predates the
    /// field (deserialized with an empty <see cref="Context"/> but
    /// non-empty <see cref="Nodes"/>). A no-op once the cache is populated
    /// - every mutator keeps it consistent from then on - and a no-op on a
    /// genuinely empty sequence. O(node count) exactly once per legacy load.
    /// </summary>
    private void EnsureContextRebuilt()
    {
        if (Context.Count > 0) return;
        if (Nodes.Count == 0) return;

        foreach (var n in Nodes) BumpContext(n.ReplicaId, n.Counter);
    }

    /// <summary>
    /// Folds <paramref name="other"/>'s per-replica maxima into this
    /// sequence's <see cref="Context"/> so the cache still dominates every
    /// node after a merge. New payloads carry a maintained
    /// <see cref="Context"/> (pointwise-max fold); a legacy
    /// <paramref name="other"/> with an empty context but non-empty nodes
    /// is folded directly from its nodes without mutating it.
    /// </summary>
    private void MergeContextFrom(Rga other)
    {
        foreach (var (replicaId, counter) in other.Context) BumpContext(replicaId, counter);

        if (other.Context.Count == 0 && other.Nodes.Count > 0)
        {
            foreach (var n in other.Nodes) BumpContext(n.ReplicaId, n.Counter);
        }
    }

    private void BumpContext(string replicaId, long counter)
    {
        if (string.IsNullOrEmpty(replicaId)) return;

        // Single-probe pointwise-max: bump the slot only when the incoming
        // counter is strictly greater. A missing slot is added
        // zero-initialised, so the !existed branch installs counter.
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Context, replicaId, out var existed);
        if (!existed || counter > slot) slot = counter;
    }

    /// <summary>
    /// Discards the cached <see cref="ToList"/> materialisation so the next
    /// read rebuilds it. Called by every mutation path.
    /// </summary>
    private void InvalidateMaterializedCache() => _materializedCache = null;

    private static int CompareBytes(byte[] a, byte[] b)
    {
        var min = Math.Min(a.Length, b.Length);
        for (var i = 0; i < min; i++)
        {
            var c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }
        return a.Length.CompareTo(b.Length);
    }

    /// <summary>
    /// Orders node storage indices into descending
    /// <c>(Counter, ReplicaId)</c> RGA sibling order - the highest counter
    /// wins, ReplicaId breaks counter ties - by resolving each index against
    /// the shared <see cref="Nodes"/> list. A <c>readonly struct</c> so the
    /// generic <see cref="MemoryExtensions.Sort{T, TComparer}(Span{T}, TComparer)"/>
    /// overload sorts each sibling run without allocating an ordering
    /// delegate on the <see cref="ToList"/> rebuild path.
    /// </summary>
    private readonly struct SiblingComparer(List<RgaNode> nodes) : IComparer<int>
    {
        public int Compare(int a, int b)
        {
            var na = nodes[a];
            var nb = nodes[b];
            var c = nb.Counter.CompareTo(na.Counter);
            return c != 0 ? c : string.CompareOrdinal(nb.ReplicaId, na.ReplicaId);
        }
    }
}
