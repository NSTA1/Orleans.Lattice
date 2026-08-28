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
    /// Transient cache of the last <see cref="MaterializeShared"/>
    /// materialisation. Never serialized (no <c>[Id]</c>): a deserialized or
    /// cloned sequence starts with a <c>null</c> cache and rebuilds it on first
    /// read. Set to <c>null</c> by every mutation path so the next read
    /// re-materialises.
    /// <para>
    /// The cached tuples alias the live <see cref="RgaNode.Value"/> buffers,
    /// which is why the cache backs the internal
    /// <see cref="MaterializeShared"/> view and never the public
    /// <see cref="ToList"/> projection - see the buffer-ownership remarks on
    /// <see cref="ICrdt{TSelf}"/>.
    /// </para>
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
    /// <para>
    /// Each returned value array is a <em>copy</em>: this is the
    /// materialised-projection egress seam of the buffer-ownership rule
    /// documented on <see cref="ICrdt{TSelf}"/>. Sharing the live
    /// <see cref="RgaNode.Value"/> buffers here would let a caller write
    /// straight into the sequence's nodes without passing any mutation API -
    /// and because the resolved order is cached and nothing would invalidate
    /// it, every later read would observe the corruption. A tombstone or empty
    /// value costs nothing (an empty span's <c>ToArray</c> returns the shared
    /// <see cref="Array.Empty{T}"/> singleton), and the expensive part of the
    /// projection - the traversal and sibling sort - is still cached across
    /// reads by <see cref="MaterializeShared"/>, so a repeat read pays only the
    /// per-value copy.
    /// </para>
    /// </returns>
    public IReadOnlyList<(OrSetDot Dot, byte[] Value)> ToList()
    {
        var shared = MaterializeShared();
        var count = shared.Count;
        if (count == 0) return shared;

        // A fresh array per call, so a caller that downcasts it mutates only
        // its own copy. Presized exactly; no List + AsReadOnly wrapper needed.
        var copy = new (OrSetDot, byte[])[count];
        for (var i = 0; i < count; i++)
        {
            var (dot, value) = shared[i];
            copy[i] = (dot, value.AsSpan().ToArray());
        }
        return copy;
    }

    /// <summary>
    /// The ordering-only view behind <see cref="ToList"/>: the same
    /// insertion-resolved projection, cached across reads, but sharing the live
    /// <see cref="RgaNode.Value"/> buffers rather than copying them.
    /// <para>
    /// Internal by design. The returned arrays are this sequence's own durable
    /// buffers, so a caller must treat them as read-only and must not let one
    /// escape to an external caller - that is exactly what the public
    /// <see cref="ToList"/> copy exists to prevent. It is offered so an internal
    /// consumer that only reads the dots (resolving a visible index to a parent)
    /// or immediately deserialises each value (the typed <c>RgaAccessor</c>)
    /// does not pay a copy it would discard on the next line.
    /// </para>
    /// </summary>
    internal IReadOnlyList<(OrSetDot Dot, byte[] Value)> MaterializeShared()
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
        // AsReadOnly wraps rather than copies: one small wrapper allocation on a
        // cache rebuild (not per read - the wrapper itself is what is cached),
        // in exchange for an internal caller never holding a mutable handle on
        // the cached projection's list shape. The value buffers are still the
        // live node arrays; ToList is what copies those on the way out.
        return _materializedCache = result.AsReadOnly();
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

        // Capture this sequence's own per-replica maxima before merging so a
        // legacy self (empty Context, non-empty nodes) is not left with a
        // Context that reflects only the incoming side - which would let the
        // next local insert re-mint an already-authored dot. This must stay
        // ahead of MergeContextFrom below: EnsureContextRebuilt bails out on a
        // non-empty Context, so folding the incoming side first would suppress
        // the rebuild of our own maxima.
        EnsureContextRebuilt();

        // Fold the incoming per-replica maxima before the empty-nodes
        // short-circuit. Context is a second, independent piece of merge state
        // (the node union is the first), and an incoming sequence can carry a
        // populated Context with no nodes - a decoded wire/storage payload, or
        // any future tombstone GC, which Context exists precisely to survive.
        // Dropping those maxima lets the next local insert re-mint an
        // already-authored dot, so two nodes share a dot identity and the
        // sequence stops converging. The early return predates Context by many
        // releases and was simply never revisited when it was introduced.
        MergeContextFrom(other);

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
                // and self-inverse under repeated merges. Copy the winning
                // value: a fold from a peer must not leave the receiver
                // aliased to the peer's buffer (an empty span's ToArray()
                // returns the shared Array.Empty<byte>() singleton, so this
                // stays zero-allocation on empty values).
                if (!ReferenceEquals(existing.Value, n.Value)
                    && CompareBytes(existing.Value, n.Value) < 0)
                {
                    existing.Value = n.Value.AsSpan().ToArray();
                }
                // Structural parent reattachment. A tombstone-before-insert
                // placeholder is recorded with ParentDot == Root (see the
                // MergeDelta tombstone branch); when the authoritative insert
                // for the same dot arrives here on the incoming side its real
                // parent must win, or the placeholder's live children stay
                // mis-rooted under Root and the two merge orders diverge.
                // Fold the parent by the same deterministic max rule as the
                // value (a real parent has Counter >= 1 so it dominates the
                // Root placeholder's Counter 0) so the merge stays commutative.
                // MergeDelta reattaches the parent on its insert path; MergeFrom
                // must do the equivalent.
                if (existing.ParentDot != n.ParentDot
                    && CompareDot(n.ParentDot, existing.ParentDot) > 0)
                {
                    existing.ParentDot = n.ParentDot;
                }
            }
            else
            {
                var added = new RgaNode
                {
                    ReplicaId = n.ReplicaId,
                    Counter = n.Counter,
                    ParentDot = n.ParentDot,
                    // Copy the adopted value: a fold from a peer must not leave
                    // the receiver holding a live handle on the peer's buffer.
                    Value = n.Value.AsSpan().ToArray(),
                    IsTombstone = n.IsTombstone,
                };
                Nodes.Add(added);
                localByDot?[n.Dot] = added;
            }
        }

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
        // Keep the counter cache dominating every node, including on a legacy
        // sequence whose Context is still empty on first delta apply - otherwise
        // the next local insert re-mints an already-authored dot.
        EnsureContextRebuilt();

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
                    // Idempotent, order-independent refresh: fold the delta
                    // insert's structural parent link and value into the
                    // existing node by the SAME deterministic max rules
                    // MergeFrom uses (CompareDot for the parent, CompareBytes
                    // for the value), so a delta-fed replica converges to the
                    // identical node a full-state merge produces even under a
                    // same-dot parent/value disagreement. Overwriting
                    // unconditionally here (last-arrival-wins) made MergeDelta
                    // diverge from MergeFrom and from other delta-fed replicas.
                    // A real parent (Counter >= 1) still dominates a
                    // tombstone-before-insert placeholder's Root parent
                    // (Counter 0), so the placeholder still reattaches; a
                    // re-delivered identical insert is a no-op. The tombstone
                    // flag is monotonic and never cleared here.
                    if (existing.ParentDot != ins.ParentDot
                        && CompareDot(ins.ParentDot, existing.ParentDot) > 0)
                    {
                        existing.ParentDot = ins.ParentDot;
                    }
                    if (ins.Value is { Length: > 0 }
                        && !ReferenceEquals(existing.Value, ins.Value)
                        && CompareBytes(existing.Value, ins.Value) < 0)
                    {
                        // Copy the winning value: a fold from a delta must not
                        // adopt the producer's buffer (the delta may be retried
                        // or fanned out to several peers).
                        existing.Value = ins.Value.AsSpan().ToArray();
                    }
                }
                else
                {
                    var node = new RgaNode
                    {
                        ReplicaId = ins.ReplicaId,
                        Counter = ins.Counter,
                        ParentDot = ins.ParentDot,
                        // Copy the adopted value: a fold from a delta must not
                        // adopt the producer's buffer. An empty span's ToArray()
                        // returns the shared Array.Empty<byte>() singleton, so
                        // an empty insert stays zero-allocation.
                        Value = ins.Value is null ? Array.Empty<byte>() : ins.Value.AsSpan().ToArray(),
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

    /// <summary>
    /// Creates a deep, independent copy of this sequence, per the
    /// <see cref="ICrdt{TSelf}.Clone"/> contract: every node is duplicated and
    /// each node's value byte array is copied, not shared.
    /// <para>
    /// The value arrays are treated as immutable <em>inside</em> this type, but
    /// that invariant stops at its boundary. <c>Clone</c> is the egress seam: a
    /// caller that reads a sequence out of an <c>OrMap&lt;string, Rga&gt;</c> gets
    /// it through <c>OrMap.Get</c>, which hands back <c>Clone()</c>, so sharing
    /// the node values there would give the caller a live handle on the map's
    /// durable state and a write through a returned <see cref="RgaNode.Value"/>
    /// would corrupt the stored CRDT without going through any mutation API -
    /// the same defect already fixed one level up in <c>OrMap.Clone</c> and
    /// <c>OrMap.Get</c>. The per-node copy is the price of the contract, exactly
    /// as <see cref="BoundedRegister.Clone"/> pays it.
    /// </para>
    /// <para>
    /// The copy is expressed as a span copy rather than <see cref="Array.Clone"/>,
    /// which allocates identically but goes through the non-generic
    /// <see cref="Array"/> path and measured roughly 3-4x slower on the
    /// <c>ordedup</c> microbench suite. A tombstoned node carries an empty value,
    /// and an empty span's <c>ToArray</c> returns the shared
    /// <see cref="Array.Empty{T}"/> singleton, so tombstones add no allocation.
    /// </para>
    /// </summary>
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
                Value = n.Value.AsSpan().ToArray(),
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

    // Lexicographic byte-order comparison used as a convergence tie-breaker:
    // only the *sign* of the result is load-bearing, never its magnitude, so
    // the vectorized SequenceCompareTo is interchangeable with the scalar loop
    // it replaced. Both agree on sign for every input: a differing byte decides
    // (unsigned, since byte is unsigned in both forms), otherwise the shorter
    // prefix sorts before the longer extension. Measured on the ordedup
    // microbench suite at 2.2 ns / 5.3 ns / 14.2 ns against 7.4 ns / 125.7 ns /
    // 384.4 ns for the scalar loop at 16 / 256 / 1024 bytes.
    private static int CompareBytes(byte[] a, byte[] b) =>
        ((ReadOnlySpan<byte>)a).SequenceCompareTo(b);

    /// <summary>
    /// Deterministic total order over parent dots used when folding a
    /// same-dot structural disagreement in <see cref="MergeFrom(Rga)"/>.
    /// Orders by <see cref="OrSetDot.Counter"/> then
    /// <see cref="OrSetDot.ReplicaId"/> (ordinal), so the <see cref="Root"/>
    /// placeholder (Counter 0) sorts below every authored parent (Counter
    /// >= 1) and any same-dot parent disagreement resolves the same way in
    /// either merge order.
    /// </summary>
    private static int CompareDot(OrSetDot a, OrSetDot b)
    {
        var c = a.Counter.CompareTo(b.Counter);
        return c != 0 ? c : string.CompareOrdinal(a.ReplicaId, b.ReplicaId);
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
