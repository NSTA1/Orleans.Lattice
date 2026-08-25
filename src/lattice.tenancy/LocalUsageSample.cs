namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An immutable, additively-mergeable snapshot of one cluster's local resource
/// consumption for a single tenant: the summed stored bytes, live keys, resident
/// memory, and owned tree count rolled up from that cluster's per-tree admission
/// samples. It is the value stored in each per-cluster slot of a
/// <see cref="TenantUsageRecord"/>; the record's fold sums these samples across
/// clusters into the tenant's global usage, and admission control compares the
/// selected sample against the tenant's <see cref="TenantQuotas"/>.
/// </summary>
/// <remarks>
/// Every dimension is a non-negative running total. <see cref="Add"/> is the
/// join used by the cross-cluster fold: it is commutative, associative, and has
/// <see cref="Empty"/> as its identity, so summing any set of per-cluster samples
/// converges to the same aggregate regardless of the order they are summed.
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.LocalUsageSample)]
[Immutable]
public readonly record struct LocalUsageSample
{
    /// <summary>The total stored value bytes attributed to the tenant on this cluster.</summary>
    [Id(0)]
    public long Bytes { get; init; }

    /// <summary>The total live key count attributed to the tenant on this cluster.</summary>
    [Id(1)]
    public long Keys { get; init; }

    /// <summary>The resident memory in bytes attributed to the tenant on this cluster.</summary>
    [Id(2)]
    public long MemoryBytes { get; init; }

    /// <summary>The number of trees the tenant owns on this cluster.</summary>
    [Id(3)]
    public long TreeCount { get; init; }

    /// <summary>The empty sample: every dimension zero. The identity of <see cref="Add"/>.</summary>
    public static LocalUsageSample Empty => default;

    /// <summary><c>true</c> when every dimension is zero.</summary>
    public bool IsEmpty => Bytes == 0 && Keys == 0 && MemoryBytes == 0 && TreeCount == 0;

    /// <summary>
    /// Returns the dimension-wise sum of this sample and <paramref name="other"/>.
    /// The operation is commutative, associative, and has <see cref="Empty"/> as
    /// its identity, so it is the join used by the cross-cluster usage fold.
    /// </summary>
    /// <param name="other">The sample to add.</param>
    /// <returns>The summed sample.</returns>
    public LocalUsageSample Add(LocalUsageSample other) =>
        new()
        {
            Bytes = Bytes + other.Bytes,
            Keys = Keys + other.Keys,
            MemoryBytes = MemoryBytes + other.MemoryBytes,
            TreeCount = TreeCount + other.TreeCount,
        };

    /// <summary>
    /// Rolls up a set of per-tree usage samples into a single local sample: the
    /// stored bytes, live keys, and resident memory are summed across the trees,
    /// and <see cref="TreeCount"/> is the number of trees supplied.
    /// </summary>
    /// <param name="trees">The per-tree samples to roll up. Must not be <c>null</c>.</param>
    /// <returns>The rolled-up local sample.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="trees"/> is <c>null</c>.</exception>
    public static LocalUsageSample RollUp(IReadOnlyCollection<TreeUsageSample> trees)
    {
        ArgumentNullException.ThrowIfNull(trees);

        long bytes = 0, keys = 0, memoryBytes = 0;
        foreach (var tree in trees)
        {
            bytes += tree.Bytes;
            keys += tree.Keys;
            memoryBytes += tree.MemoryBytes;
        }

        return new LocalUsageSample
        {
            Bytes = bytes,
            Keys = keys,
            MemoryBytes = memoryBytes,
            TreeCount = trees.Count,
        };
    }
}
