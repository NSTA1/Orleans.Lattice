namespace Orleans.Lattice.Primitives;

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
/// <c>index -&gt; dot</c> projection is rebuilt on every read by
/// <see cref="ToList"/> via an in-order traversal of the parent /
/// child tree. A future cache layer is an optional follow-on
/// optimisation, not part of the base primitive.
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
    /// <summary>
    /// Every node in the sequence, live or tombstoned, in arbitrary
    /// storage order. Materialise via <see cref="ToList"/> for the
    /// deterministic in-order projection.
    /// </summary>
    [Id(0)]
    public List<RgaNode> Nodes { get; set; } = new();

    /// <summary>The empty dot used to represent the virtual sequence root (parent of top-level inserts).</summary>
    public static OrSetDot Root => default;

    /// <summary>Returns <c>true</c> when no live (un-tombstoned) node remains.</summary>
    public bool IsEmpty
    {
        get
        {
            foreach (var n in Nodes)
            {
                if (!n.IsTombstone) return false;
            }
            return true;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// An <see cref="Rga"/> is bottom when no live node remains.
    /// Tombstoned nodes may still be present and are preserved for
    /// causal-history purposes; a containing composite treats the
    /// slot as empty.
    /// </remarks>
    public bool IsBottom => IsEmpty;

    /// <summary>Returns the number of live (un-tombstoned) nodes.</summary>
    public int Count
    {
        get
        {
            var n = 0;
            foreach (var node in Nodes)
            {
                if (!node.IsTombstone) n++;
            }
            return n;
        }
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
        if (Nodes.Count == 0) return Array.Empty<(OrSetDot, byte[])>();

        // Build the parent -> children index once per call. The map
        // value is a list of (counter, replicaId, nodeIndex) triples
        // sorted descending by (counter, replicaId) so the in-order
        // traversal is a straight foreach on the sorted children.
        var childrenByParent = new Dictionary<OrSetDot, List<RgaNode>>();
        foreach (var n in Nodes)
        {
            if (!childrenByParent.TryGetValue(n.ParentDot, out var bucket))
            {
                bucket = new List<RgaNode>();
                childrenByParent[n.ParentDot] = bucket;
            }
            bucket.Add(n);
        }
        foreach (var bucket in childrenByParent.Values)
        {
            bucket.Sort(static (a, b) =>
            {
                var c = b.Counter.CompareTo(a.Counter);
                if (c != 0) return c;
                return string.CompareOrdinal(b.ReplicaId, a.ReplicaId);
            });
        }

        var result = new List<(OrSetDot, byte[])>(Nodes.Count);
        // Iterative DFS to avoid stack overflows on deep histories.
        var stack = new Stack<RgaNode>();
        if (childrenByParent.TryGetValue(Root, out var top))
        {
            // Push in reverse so the highest-priority sibling is
            // popped first.
            for (var i = top.Count - 1; i >= 0; i--) stack.Push(top[i]);
        }
        while (stack.Count > 0)
        {
            var node = stack.Pop();
            if (!node.IsTombstone) result.Add((node.Dot, node.Value));
            if (childrenByParent.TryGetValue(node.Dot, out var kids))
            {
                for (var i = kids.Count - 1; i >= 0; i--) stack.Push(kids[i]);
            }
        }
        return result;
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

        var localByDot = new Dictionary<OrSetDot, RgaNode>(Nodes.Count);
        foreach (var n in Nodes) localByDot[n.Dot] = n;

        foreach (var n in other.Nodes)
        {
            if (localByDot.TryGetValue(n.Dot, out var existing))
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
                Nodes.Add(new RgaNode
                {
                    ReplicaId = n.ReplicaId,
                    Counter = n.Counter,
                    ParentDot = n.ParentDot,
                    Value = n.Value,
                    IsTombstone = n.IsTombstone,
                });
                localByDot[n.Dot] = Nodes[^1];
            }
        }
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
        return copy;
    }

    private long NextCounter(string replicaId)
    {
        long max = 0;
        foreach (var n in Nodes)
        {
            if (string.Equals(n.ReplicaId, replicaId, StringComparison.Ordinal) && n.Counter > max)
                max = n.Counter;
        }
        return max + 1;
    }

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
}