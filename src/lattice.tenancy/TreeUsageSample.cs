namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An ephemeral, per-tree resource-usage measurement for one tenant on the local
/// cluster: the stored value bytes, live key count, and resident memory a single
/// tree contributes. It is the input to
/// <see cref="LocalUsageSample.RollUp(IReadOnlyCollection{TreeUsageSample})"/>,
/// which sums a tenant's per-tree samples into the cluster-local
/// <see cref="LocalUsageSample"/> that is published and folded.
/// </summary>
/// <remarks>
/// This is a transient input to the low-frequency roll-up, never persisted and
/// never crossing a grain boundary, so it carries no Orleans serialization
/// attributes. Only the rolled-up <see cref="LocalUsageSample"/> and the
/// per-cluster-slot <see cref="TenantUsageRecord"/> are durable.
/// </remarks>
public readonly record struct TreeUsageSample
{
    /// <summary>Initializes a per-tree usage sample.</summary>
    /// <param name="bytes">The tree's stored value bytes for the tenant.</param>
    /// <param name="keys">The tree's live key count for the tenant.</param>
    /// <param name="memoryBytes">The tree's resident memory in bytes for the tenant.</param>
    public TreeUsageSample(long bytes, long keys, long memoryBytes)
    {
        Bytes = bytes;
        Keys = keys;
        MemoryBytes = memoryBytes;
    }

    /// <summary>The tree's stored value bytes for the tenant.</summary>
    public long Bytes { get; init; }

    /// <summary>The tree's live key count for the tenant.</summary>
    public long Keys { get; init; }

    /// <summary>The tree's resident memory in bytes for the tenant.</summary>
    public long MemoryBytes { get; init; }
}
