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
/// <para>
/// <strong>Equality caveat.</strong> The synthesized record-struct
/// equality delegates to the default comparer for each field, and the
/// default comparer for <see cref="byte"/><c>[]</c> is <em>reference</em>
/// equality. Two structurally-identical nodes built from independently
/// allocated <see cref="Value"/> arrays therefore compare unequal;
/// consumers matching nodes across deltas should compare on the dot
/// tuple (<see cref="ReplicaId"/>, <see cref="Counter"/>), not via record
/// equality.
/// </para>
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
}
