namespace Orleans.Lattice;

/// <summary>
/// Selects the durability-coupling guarantee between a key's value write and
/// its tag-membership rows when both are written together through
/// <see cref="ILatticeValueTagWrite"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TagConsistency)]
public enum TagConsistency
{
    /// <summary>
    /// Default. The value write and each tag-membership row are two independent
    /// durable writes. A partial failure can leave the value stored without
    /// (some of) its membership rows, or vice versa; the drift is repaired
    /// idempotently by
    /// <see cref="ILatticeTagIndex.ReconcileAsync(string?, string?, System.Threading.CancellationToken)"/>.
    /// </summary>
    Eventual = 0,

    /// <summary>
    /// Opt-in. The value write and every tag-membership add are lowered to a
    /// single cross-tree atomic-write saga, so the value and its tag rows become
    /// visible together or not at all.
    /// </summary>
    Atomic = 1,
}
