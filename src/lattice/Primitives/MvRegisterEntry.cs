namespace Orleans.Lattice;

/// <summary>
/// A single dot-tagged value in an <see cref="MvRegister"/>: a
/// <c>(<see cref="ReplicaId"/>, <see cref="Counter"/>, <see cref="Value"/>)</c>
/// triple stamped at the moment a write was authored. The dot context
/// (<see cref="ReplicaId"/> + <see cref="Counter"/>) is what makes the
/// multi-value register converge under concurrent active-active
/// updates: a write on replica A with dot <c>(A, 1)</c> and a
/// concurrent write on replica B with dot <c>(B, 1)</c> survive a
/// merge together because neither dot is dominated by the other side's
/// dot context, so both values remain visible until a future write
/// observes them.
/// <para>
/// Deliberately <em>not</em> <c>[Immutable]</c>: <see cref="Value"/> is a
/// mutable <c>byte[]</c> and the entry crosses the grain-proxy boundary as part
/// of an <see cref="MvRegister"/>'s durable state, so Orleans must deep-copy it
/// on a same-silo call. Marking the type immutable elides that copy and hands
/// the caller a live handle on the grain's persisted bytes.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.MvRegisterDot)]
public readonly record struct MvRegisterEntry
{
    /// <summary>The id of the replica that authored this dot.</summary>
    [Id(0)] public string ReplicaId { get; init; }

    /// <summary>The replica-local monotonic counter at the moment the dot was authored.</summary>
    [Id(1)] public long Counter { get; init; }

    /// <summary>The opaque value bytes stamped under this dot.</summary>
    [Id(2)] public byte[] Value { get; init; }
}
