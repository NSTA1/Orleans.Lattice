using System.Text.Json.Serialization;

namespace Orleans.Lattice;

/// <summary>
/// A single inserted node inside an <see cref="RgaDelta"/>: the
/// structural intent of one <c>InsertAfter</c> operation captured at the
/// producing call site as the triple
/// <c>(<see cref="Dot"/>, <see cref="ParentDot"/>, <see cref="Value"/>)</c>.
/// The dot identity (<see cref="ReplicaId"/> + <see cref="Counter"/>) and
/// the parent link are what let a receiver rebuild the exact causal
/// position regardless of the post-merge sequence it currently holds -
/// shipping the post-merge materialised order instead would lose the
/// concurrent-insert information an <see cref="Rga"/> needs to converge.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RgaDeltaNode)]
[Immutable]
public readonly record struct RgaDeltaNode
{
    /// <summary>The id of the replica that authored this node.</summary>
    [Id(0)] public string ReplicaId { get; init; }

    /// <summary>The replica-local monotonic counter at the moment the node was authored.</summary>
    [Id(1)] public long Counter { get; init; }

    /// <summary>
    /// The parent dot under which this node was inserted. The empty dot
    /// (<see cref="Rga.Root"/>) represents the virtual sequence root.
    /// </summary>
    [Id(2)] public OrSetDot ParentDot { get; init; }

    /// <summary>The value bytes attached at this position. Never <c>null</c> on emitter-produced nodes.</summary>
    [Id(3)] public byte[] Value { get; init; }

    /// <summary>
    /// Returns this node's dot identity, composed from
    /// <see cref="ReplicaId"/> and <see cref="Counter"/>. Computed and
    /// not serialised; the receiver rebuilds it from the persisted dot
    /// components.
    /// </summary>
    [JsonIgnore]
    public OrSetDot Dot => new() { ReplicaId = ReplicaId, Counter = Counter };

    /// <summary>
    /// Compares two nodes by value, with <see cref="Value"/> compared by
    /// content. The compiler-generated record-struct equality compares the
    /// <see cref="Value"/> byte array with <see cref="EqualityComparer{T}.Default"/> -
    /// reference equality for a <see cref="byte"/> array - so two nodes built
    /// from independently allocated but byte-identical values (including a
    /// node and its post-serialization self) would otherwise never compare
    /// equal, silently breaking any dedup or round-trip check framed as
    /// record equality over these nodes. The computed <see cref="Dot"/> is
    /// derived from <see cref="ReplicaId"/> and <see cref="Counter"/>, so it
    /// is not compared independently.
    /// </summary>
    /// <param name="other">The node to compare against.</param>
    public bool Equals(RgaDeltaNode other) =>
        BytesEqual(Value, other.Value)
        && string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal)
        && Counter == other.Counter
        && ParentDot.Equals(other.ParentDot);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        hash.Add(ReplicaId, StringComparer.Ordinal);
        hash.Add(Counter);
        hash.Add(ParentDot);
        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
