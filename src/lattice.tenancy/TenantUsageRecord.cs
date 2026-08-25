namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The durable, conflict-free-mergeable aggregate of a single tenant's resource
/// usage across the clusters that host it: its immutable <see cref="Id"/> plus a
/// map of per-cluster usage slots keyed by cluster id, each a last-writer-wins
/// register over that cluster's <see cref="LocalUsageSample"/>. Each cluster
/// writes <b>only its own slot</b> (via <see cref="SetLocalSample"/>), and the
/// global usage is the sum-fold of every slot (<see cref="Fold()"/>); this is the
/// per-cluster-slot state-CRDT shape used elsewhere in the codebase for
/// cross-cluster convergence.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="MergeFrom"/> / <see cref="Merge"/> join the slot maps field by
/// field with the shared <see cref="TenantClock"/> last-writer-wins order, so the
/// join is commutative, associative, and idempotent: any number of cluster
/// replicas converge to the same record independent of the order updates are
/// applied. The fold over the converged slots is itself commutative and
/// associative (see <see cref="LocalUsageSample.Add"/>).
/// </para>
/// <para>
/// Quota consistency is converged best-effort with <b>bounded overshoot</b>:
/// because each cluster admits against a fold that includes other clusters' last
/// <em>published</em> slot (not their live, unpublished usage), concurrent
/// cross-cluster writes can momentarily push the true global usage slightly over
/// a quota before the slots re-converge. This is expected and is the cost of a
/// lock-free, consensus-free aggregate.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantUsageRecord)]
public sealed class TenantUsageRecord
{
    /// <summary>The immutable identity of the tenant this usage record accounts for.</summary>
    [Id(0)]
    public TenantId Id { get; private init; }

    /// <summary>
    /// The per-cluster usage slots, keyed by cluster id. Each slot is a
    /// last-writer-wins register over that cluster's rolled-up local sample.
    /// </summary>
    [Id(1)]
    internal Dictionary<string, TenantLwwRegister<LocalUsageSample>> ClusterSlots { get; set; } =
        new(StringComparer.Ordinal);

    /// <summary>Parameterless constructor for the Orleans serializer.</summary>
    public TenantUsageRecord()
    {
    }

    private TenantUsageRecord(TenantId id) => Id = id;

    /// <summary>
    /// Creates an empty usage record for a tenant, with no cluster slots yet
    /// published.
    /// </summary>
    /// <param name="id">The tenant identity. Must be an initialised (parsed) tenant id.</param>
    /// <returns>The constructed record.</returns>
    /// <exception cref="ArgumentException"><paramref name="id"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    public static TenantUsageRecord Create(TenantId id)
    {
        if (id.Value is null)
        {
            throw new ArgumentException(
                "Cannot create a usage record for the uninitialised 'no tenant' value.",
                nameof(id));
        }

        return new TenantUsageRecord(id);
    }

    /// <summary>
    /// Writes <paramref name="cluster"/>'s local usage sample into its own slot if
    /// the stamp supersedes the slot's current stamp. A cluster only ever writes
    /// its own slot, so the map grows one slot per participating cluster and the
    /// fold sums them.
    /// </summary>
    /// <param name="cluster">The writing cluster's id. Must not be <c>null</c>.</param>
    /// <param name="sample">The cluster's rolled-up local usage sample.</param>
    /// <param name="clock">The write clock.</param>
    /// <param name="writerId">The write writer id (may be <c>null</c>; typically the cluster id).</param>
    /// <exception cref="ArgumentNullException"><paramref name="cluster"/> is <c>null</c>.</exception>
    public void SetLocalSample(string cluster, LocalUsageSample sample, HybridLogicalClock clock, string? writerId)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ClusterSlots[cluster] = ClusterSlots.TryGetValue(cluster, out var existing)
            ? existing.Set(sample, clock, writerId)
            : TenantLwwRegister<LocalUsageSample>.Create(sample, clock, writerId);
    }

    /// <summary>
    /// Returns the last published usage sample for <paramref name="cluster"/>, or
    /// <see cref="LocalUsageSample.Empty"/> when that cluster has published none. A
    /// zero-allocation lookup used by the per-cluster enforcement scope.
    /// </summary>
    /// <param name="cluster">The cluster id whose slot to read. Must not be <c>null</c>.</param>
    /// <returns>The cluster's sample, or the empty sample when absent.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="cluster"/> is <c>null</c>.</exception>
    public LocalUsageSample LocalSample(string cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        return ClusterSlots.TryGetValue(cluster, out var slot) ? slot.Value : LocalUsageSample.Empty;
    }

    /// <summary>
    /// The number of cluster slots currently published in this record. Exposed for
    /// diagnostics and tests.
    /// </summary>
    public int ClusterCount => ClusterSlots.Count;

    /// <summary>
    /// Folds every published cluster slot into the tenant's global usage by
    /// summing the samples. The sum is commutative and associative, so the result
    /// is independent of iteration order. A zero-allocation aggregate over the
    /// stored slots.
    /// </summary>
    /// <returns>The tenant's global usage across all published clusters.</returns>
    public LocalUsageSample Fold()
    {
        var accumulator = LocalUsageSample.Empty;
        foreach (var slot in ClusterSlots.Values)
        {
            accumulator = accumulator.Add(slot.Value);
        }

        return accumulator;
    }

    /// <summary>
    /// Folds only the cluster slots whose id is in <paramref name="residentClusters"/>
    /// into the tenant's global usage, so a stale slot for a cluster that no longer
    /// hosts the tenant (or is offline) is excluded from the converged aggregate
    /// the default enforcement scope admits against.
    /// </summary>
    /// <param name="residentClusters">The set of cluster ids to include. Must not be <c>null</c>.</param>
    /// <returns>The tenant's global usage restricted to the resident clusters.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="residentClusters"/> is <c>null</c>.</exception>
    public LocalUsageSample Fold(IReadOnlySet<string> residentClusters)
    {
        ArgumentNullException.ThrowIfNull(residentClusters);

        var accumulator = LocalUsageSample.Empty;
        foreach (var (cluster, slot) in ClusterSlots)
        {
            if (residentClusters.Contains(cluster))
            {
                accumulator = accumulator.Add(slot.Value);
            }
        }

        return accumulator;
    }

    /// <summary>
    /// Produces an independent deep copy of this record, so it can be merged or
    /// mutated without affecting the original.
    /// </summary>
    /// <returns>The cloned record.</returns>
    public TenantUsageRecord Clone() =>
        new(Id)
        {
            ClusterSlots = new Dictionary<string, TenantLwwRegister<LocalUsageSample>>(ClusterSlots, StringComparer.Ordinal),
        };

    /// <summary>
    /// Merges <paramref name="other"/> into this record in place, joining every
    /// cluster slot with the shared last-writer-wins order. The join is
    /// commutative, associative, and idempotent.
    /// </summary>
    /// <param name="other">The record to merge in. Must share this record's <see cref="Id"/>.</param>
    /// <returns>This record, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="other"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="other"/> accounts for a different tenant.</exception>
    public TenantUsageRecord MergeFrom(TenantUsageRecord other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (!Id.Equals(other.Id))
        {
            throw new ArgumentException(
                $"Cannot merge a usage record for tenant '{other.Id}' into a record for tenant '{Id}'.",
                nameof(other));
        }

        foreach (var (cluster, slot) in other.ClusterSlots)
        {
            ClusterSlots[cluster] = ClusterSlots.TryGetValue(cluster, out var mine)
                ? TenantLwwRegister<LocalUsageSample>.Merge(mine, slot)
                : slot;
        }

        return this;
    }

    /// <summary>
    /// Merges two records into a new record, leaving both inputs unchanged. The
    /// join is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">One record. Must not be <c>null</c>.</param>
    /// <param name="right">The other record. Must share <paramref name="left"/>'s <see cref="Id"/>. Must not be <c>null</c>.</param>
    /// <returns>The merged record.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/> or <paramref name="right"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The two records account for different tenants.</exception>
    public static TenantUsageRecord Merge(TenantUsageRecord left, TenantUsageRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return left.Clone().MergeFrom(right);
    }
}
