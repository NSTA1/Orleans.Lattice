using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Typed delta record for a version-vector mutation. Carries the
/// per-replica <see cref="HybridLogicalClock"/> entries that have
/// advanced on the originator since the receiver's cursor. Version
/// vectors merge by pointwise-max, so a delta is structurally identical
/// to a full vector — the difference is only that callers ship the
/// minimum advancing subset to keep payload sizes bounded.
/// <para>
/// Apply semantics on the receiver: for every <c>(replicaId, clock)</c>
/// pair, set the local entry to <c>max(local, clock)</c>. Commutative,
/// associative, idempotent: late or duplicate delivery is a no-op.
/// </para>
/// <para>
/// Emitters always populate <see cref="Entries"/> (use an empty
/// <see cref="Dictionary{TKey, TValue}"/> for a no-op delta); use
/// <see cref="Empty"/> to author one without allocating a fresh
/// dictionary. The <see cref="default"/> instance has a <c>null</c>
/// map and is intended only as the zero-value of the struct.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.VersionVectorDelta)]
[Immutable]
public readonly record struct VersionVectorDelta
{
    /// <summary>
    /// Per-replica advanced entries since the receiver's cursor. An
    /// empty map represents a no-op delta.
    /// </summary>
    [Id(0)] public Dictionary<string, HybridLogicalClock> Entries { get; init; }

    /// <summary>
    /// A reusable no-op delta with an empty (but non-null)
    /// <see cref="Entries"/> map. The backing dictionary is shared -
    /// callers must not mutate it.
    /// </summary>
    public static VersionVectorDelta Empty { get; } = new()
    {
        Entries = new Dictionary<string, HybridLogicalClock>(0),
    };
}
