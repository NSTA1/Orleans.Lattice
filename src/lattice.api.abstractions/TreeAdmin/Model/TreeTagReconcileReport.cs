namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The idempotent outcome of an operator-driven tag-index reconciliation sweep,
/// pairing the index identity with the counts the core reconcile pass reports:
/// how much of the covered keyspace was inspected and how many orphaned membership
/// rows were repaired.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeTagReconcileReport)]
[Immutable]
public sealed record TreeTagReconcileReport
{
    /// <summary>The logical tag-index name that was reconciled.</summary>
    [Id(0)] public required string IndexName { get; init; }

    /// <summary>The backing membership tree id (<c>tag-{IndexName}</c>).</summary>
    [Id(1)] public required string TreeId { get; init; }

    /// <summary>The number of subject trees inspected during the sweep.</summary>
    [Id(2)] public int TreesCovered { get; init; }

    /// <summary>The number of live primary-tree keys scanned.</summary>
    [Id(3)] public int KeysScanned { get; init; }

    /// <summary>The number of membership rows examined.</summary>
    [Id(4)] public int MembershipRowsScanned { get; init; }

    /// <summary>The number of membership rows deleted because their key no longer exists in the primary tree.</summary>
    [Id(5)] public int OrphanRowsRemoved { get; init; }
}
