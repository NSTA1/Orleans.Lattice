using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A single mutation observed by an <see cref="IMutationObserver"/>.
/// For <see cref="MutationKind.Set"/> and <see cref="MutationKind.Delete"/>
/// the record describes a single key's post-commit LWW metadata; for
/// <see cref="MutationKind.DeleteRange"/> it describes the half-open range
/// <c>[StartKey, EndExclusiveKey)</c> that was tombstoned.
/// <para>
/// The shape is deliberately flat (instead of embedding
/// <c>LwwValue&lt;byte[]&gt;</c>) to keep the public extensibility contract
/// independent of the library's internal wire DTOs.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeMutation)]
[Immutable]
public readonly record struct LatticeMutation
{
    /// <summary>The logical tree id the mutation was committed to.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The kind of mutation.</summary>
    [Id(1)] public MutationKind Kind { get; init; }

    /// <summary>
    /// The key for <see cref="MutationKind.Set"/> / <see cref="MutationKind.Delete"/>,
    /// or the inclusive start key for <see cref="MutationKind.DeleteRange"/>.
    /// </summary>
    [Id(2)] public string Key { get; init; }

    /// <summary>
    /// The exclusive end key for <see cref="MutationKind.DeleteRange"/>;
    /// <c>null</c> for <see cref="MutationKind.Set"/> and <see cref="MutationKind.Delete"/>.
    /// </summary>
    [Id(3)] public string? EndExclusiveKey { get; init; }

    /// <summary>
    /// The committed value for <see cref="MutationKind.Set"/>; <c>null</c>
    /// for deletes and range deletes.
    /// </summary>
    [Id(4)] public byte[]? Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> stamped on the committed entry
    /// for <see cref="MutationKind.Set"/> and <see cref="MutationKind.Delete"/>.
    /// For <see cref="MutationKind.DeleteRange"/> this carries the HLC of the
    /// tombstone batch (or <see cref="HybridLogicalClock.Zero"/> when the
    /// range matched nothing).
    /// </summary>
    [Id(5)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// <c>true</c> when the committed entry is a tombstone
    /// (<see cref="MutationKind.Delete"/> and <see cref="MutationKind.DeleteRange"/>
    /// always set this).
    /// </summary>
    [Id(6)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the committed entry expires, or <c>0</c>
    /// when it does not expire. Preserved end-to-end for
    /// <see cref="MutationKind.Set"/>; always <c>0</c> for deletes.
    /// </summary>
    [Id(7)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored this mutation, or
    /// <c>null</c> for a local write. Populated at commit time from the
    /// ambient <see cref="LatticeOriginContext"/> so replication-aware
    /// observers can skip re-forwarding mutations that originated
    /// elsewhere and avoid replication loops. Always <c>null</c> on
    /// <see cref="MutationKind.DeleteRange"/> unless the range-delete call
    /// was itself stamped with an origin — range deletes read the context
    /// at publish time rather than pulling from a per-key <c>LwwValue</c>.
    /// </summary>
    [Id(8)] public string? OriginClusterId { get; init; }
}
