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

    /// <summary>
    /// Sparse vector-clock frontier captured at commit time, or
    /// <c>null</c> when the writer did not supply one. Mirrors
    /// <see cref="LwwValue{T}.VectorClock"/> on per-key
    /// <see cref="MutationKind.Set"/> / <see cref="MutationKind.Delete"/>
    /// observations and the ambient
    /// <see cref="LatticeVectorClockContext"/> on
    /// <see cref="MutationKind.DeleteRange"/>. Replication-aware observers
    /// pin or compare the frontier as needed; the library itself does
    /// not interpret it.
    /// </summary>
    /// <remarks>
    /// <see cref="Primitives.VersionVector"/> is a mutable reference type
    /// whose <see cref="Primitives.VersionVector.Entries"/> dictionary is
    /// publicly mutable. The instance carried on this slot is shared with
    /// the originating commit site and may continue to be advanced after
    /// <see cref="IMutationObserver.OnMutationAsync"/> returns. Observers
    /// that retain the frontier past the observer call - for example to
    /// stamp it on a downstream wire envelope - must defensively snapshot
    /// the value (typically via <see cref="Primitives.VersionVector.Clone"/>);
    /// the replication package's built-in observer does this internally so
    /// every emitted <c>ReplogEntry</c> is detached from later producer-side
    /// advances.
    /// </remarks>
    [Id(9)] public Primitives.VersionVector? VectorClock { get; init; }

    /// <summary>
    /// Identifier of the logical transaction that produced this mutation.
    /// Single-key writes (<c>SetAsync</c>, <c>DeleteAsync</c>, <c>SetIfVersionAsync</c>,
    /// <c>GetOrSetAsync</c>) get a fresh <see cref="Guid"/> per call.
    /// A non-atomic <c>SetManyAsync</c> batch shares one id across every
    /// per-key emit. A user <c>DeleteRangeAsync</c> call shares one id
    /// across every per-shard <see cref="MutationKind.DeleteRange"/> emit.
    /// An atomic-write saga (<c>SetManyAtomicAsync</c>) shares a single,
    /// persisted id across every per-key emit produced by both the
    /// execute and compensate phases — replication consumers can therefore
    /// capture vector-clock frontier (or any other batch-wide invariant)
    /// once per transaction and apply it identically to every emit.
    /// Defaults to <see cref="Guid.Empty"/> for wire compatibility with
    /// observers persisted before this field existed.
    /// </summary>
    [Id(10)] public Guid TransactionId { get; init; }

    /// <summary>
    /// Classifies the mutation as a user-driven write
    /// (<see cref="MutationCategory.User"/>, the default) or a
    /// library-internal maintenance write
    /// (<see cref="MutationCategory.Maintenance"/>). Replication-aware
    /// observers skip the WAL append for
    /// <see cref="MutationCategory.Maintenance"/> emits on replicated
    /// trees so structural maintenance does not cross cluster boundaries.
    /// Independent of <see cref="OriginClusterId"/> — a remote-origin
    /// maintenance emit would still be
    /// <see cref="MutationCategory.Maintenance"/>. Defaults to
    /// <see cref="MutationCategory.User"/> for wire compatibility with
    /// observers persisted before this field existed.
    /// </summary>
    [Id(11)] public MutationCategory Category { get; init; }

    /// <summary>
    /// Stable identifier for the encoding of <see cref="DeltaPayload"/>,

    /// or <see langword="null"/> when the producer did not supply an
    /// author's delta. Typically the fully-qualified type name or a short
    /// alias of a typed delta record from the replication package
    /// (<c>LwwRegisterDelta</c>, <c>OrSetDelta</c>, <c>PnCounterDelta</c>,
    /// <c>VersionVectorDelta</c>, <c>MvRegisterDelta</c>). The lattice
    /// library itself never opens the payload — consumers (the
    /// replication observer in particular) decode based on this slot.
    /// Defaults to <see langword="null"/> for wire compatibility with
    /// observers persisted before this field existed.
    /// </summary>
    [Id(12)] public string? DeltaKind { get; init; }

    /// <summary>
    /// Pre-merge author's delta in opaque-bytes form, or
    /// <see langword="null"/> when the producer did not supply one. The
    /// minimal record the producer would replay against an in-memory
    /// projection to reach the same converged state — distinct from
    /// <see cref="Value"/>, which always carries the post-merge committed
    /// bytes. Carrying the author's delta lets a deterministic replay path
    /// (e.g. a future leaf-projection rebuild from the WAL) reach the
    /// same convergence the originating writer reached, which the
    /// post-merge bytes alone cannot guarantee for non-LWW CRDTs. The
    /// lattice library itself never opens the payload. Defaults to
    /// <see langword="null"/> for wire compatibility with observers
    /// persisted before this field existed.
    /// </summary>
    [Id(13)] public byte[]? DeltaPayload { get; init; }
}
