namespace Orleans.Lattice.Primitives;

/// <summary>
/// A single node in an <see cref="Rga"/>: a causally-tagged value
/// position carrying its parent link, the value bytes (or empty when
/// tombstoned), and the tombstone flag preserved for causal stability
/// so a concurrent insert under the same parent still resolves
/// against a stable predecessor.
/// <para>
/// Sibling order under a shared parent is the deterministic descending
/// <c>(Counter, ReplicaId)</c> sort applied by <see cref="Rga.ToList"/>:
/// the highest counter wins ties, and the highest <see cref="ReplicaId"/>
/// breaks counter ties. This is the standard RGA tie-break and yields
/// the same resolved order on every replica regardless of merge
/// arrival sequence.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RgaNode)]
public sealed class RgaNode
{
    /// <summary>The id of the replica that authored this node.</summary>
    [Id(0)] public string ReplicaId { get; set; } = string.Empty;

    /// <summary>The replica-local monotonic counter at the moment the node was authored.</summary>
    [Id(1)] public long Counter { get; set; }

    /// <summary>
    /// The parent dot under which this node was inserted. The empty
    /// dot (<see cref="OrSetDot"/> with <see cref="OrSetDot.ReplicaId"/>
    /// empty and <see cref="OrSetDot.Counter"/> zero) represents the
    /// virtual sequence root.
    /// </summary>
    [Id(2)] public OrSetDot ParentDot { get; set; }

    /// <summary>The value bytes attached at this position. Empty when <see cref="IsTombstone"/> is <c>true</c>.</summary>
    [Id(3)] public byte[] Value { get; set; } = Array.Empty<byte>();

    /// <summary>
    /// <c>true</c> when the node has been observed-removed. Tombstoned
    /// nodes are preserved so concurrent inserts that target the same
    /// parent still find a stable predecessor and converge on the same
    /// resolved order.
    /// </summary>
    [Id(4)] public bool IsTombstone { get; set; }

    /// <summary>Returns this node's dot identity.</summary>
    public OrSetDot Dot => new() { ReplicaId = ReplicaId, Counter = Counter };
}