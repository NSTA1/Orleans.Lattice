namespace Orleans.Lattice;

/// <summary>
/// Result of a leaf-level range delete operation (see
/// <see cref="BPlusTree.IBPlusLeafGrain.DeleteRangeAsync"/>).
/// <para>
/// <see cref="Deleted"/> is the number of live entries that were tombstoned
/// inside the leaf. <see cref="PastRange"/> is <c>true</c> when the leaf
/// has observed at least one key whose ordinal comparison is
/// <c>&gt;= endExclusive</c> — i.e. all subsequent leaves in the chain
/// are guaranteed to lie strictly beyond the delete range and do not need
/// to be walked.
/// </para>
/// <para>
/// The shard-root coordinator (FX-011) uses <see cref="PastRange"/> to
/// terminate the leaf-chain walk deterministically. Relying solely on
/// <see cref="Deleted"/> = 0 is unsafe on sparse multi-shard trees where
/// an early leaf legitimately has no range-matching keys but later
/// leaves do — the coordinator must keep walking until a leaf reports
/// <see cref="PastRange"/> = <c>true</c>, at which point it can stop.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RangeDeleteResult)]
[Immutable]
[System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
public readonly record struct RangeDeleteResult
{
    /// <summary>The number of live entries tombstoned by this operation.</summary>
    [Id(0)] public int Deleted { get; init; }

    /// <summary>
    /// <c>true</c> when the leaf has at least one key <c>&gt;= endExclusive</c>,
    /// signalling that no subsequent leaf in the chain can contain range-matching keys.
    /// </summary>
    [Id(1)] public bool PastRange { get; init; }
}
