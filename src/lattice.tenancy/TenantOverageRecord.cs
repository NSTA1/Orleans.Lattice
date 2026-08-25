namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The durable, conflict-free-mergeable meter of a single tenant's quota
/// <b>overage</b> across the clusters that host it: its immutable <see cref="Id"/>
/// plus four grow-only <see cref="GCounter"/> dimensions (bytes, keys, memory,
/// trees). Each cluster is a counter <b>replica</b> keyed by its cluster id and
/// advances <b>only its own</b> component (via <see cref="MeterLocal"/>); the
/// global metered overage is the sum-fold of every replica (<see cref="Fold()"/>).
/// This is the billing-ready, first-class overage counterpart of the
/// <see cref="TenantUsageRecord"/> usage aggregate, sharing its per-cluster-slot,
/// state-CRDT convergence shape.
/// </summary>
/// <remarks>
/// <para>
/// Overage is metered as a Riemann sum: each accrual tick adds the current overage
/// (<see cref="TenantOverageSample.Above"/>) to the counters, so the meter only
/// ever grows - the correct semantics for a billing meter, and exactly what a
/// <see cref="GCounter"/> models. <see cref="MergeFrom"/> / <see cref="Merge"/>
/// join each dimension with the grow-only pointwise-max per replica, so the join
/// is commutative, associative, and idempotent: any number of cluster replicas
/// converge to the same record independent of delivery order, and re-delivering a
/// counter never double-counts.
/// </para>
/// <para>
/// A replica only advances on a non-empty overage: metering a zero increment
/// records no component, so a tenant that never breaches carries an empty record
/// and folds to <see cref="TenantOverageSample.Empty"/>.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantOverageRecord)]
public sealed class TenantOverageRecord
{
    /// <summary>The immutable identity of the tenant this overage meter accounts for.</summary>
    [Id(0)]
    public TenantId Id { get; private init; }

    /// <summary>The grow-only meter of byte-quota overage, one component per cluster replica.</summary>
    [Id(1)]
    internal GCounter OverageBytes { get; set; } = new();

    /// <summary>The grow-only meter of key-quota overage, one component per cluster replica.</summary>
    [Id(2)]
    internal GCounter OverageKeys { get; set; } = new();

    /// <summary>The grow-only meter of memory-quota overage, one component per cluster replica.</summary>
    [Id(3)]
    internal GCounter OverageMemoryBytes { get; set; } = new();

    /// <summary>The grow-only meter of tree-count-quota overage, one component per cluster replica.</summary>
    [Id(4)]
    internal GCounter OverageTreeCount { get; set; } = new();

    /// <summary>Parameterless constructor for the Orleans serializer.</summary>
    public TenantOverageRecord()
    {
    }

    private TenantOverageRecord(TenantId id) => Id = id;

    /// <summary>
    /// Creates an empty overage meter for a tenant, with no cluster having metered
    /// any overage yet.
    /// </summary>
    /// <param name="id">The tenant identity. Must be an initialised (parsed) tenant id.</param>
    /// <returns>The constructed record.</returns>
    /// <exception cref="ArgumentException"><paramref name="id"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    public static TenantOverageRecord Create(TenantId id)
    {
        if (id.Value is null)
        {
            throw new ArgumentException(
                "Cannot create an overage record for the uninitialised 'no tenant' value.",
                nameof(id));
        }

        return new TenantOverageRecord(id);
    }

    /// <summary>
    /// Advances <paramref name="cluster"/>'s own grow-only component on each
    /// dimension by <paramref name="increment"/>. A cluster only ever advances its
    /// own replica, so replaying an older publish never regresses a fresher one and
    /// a zero increment records nothing. Each dimension delegates to
    /// <see cref="GCounter.Increment"/>, which rejects a negative amount.
    /// </summary>
    /// <param name="cluster">The metering cluster's id (the replica key). Must not be <c>null</c> or empty.</param>
    /// <param name="increment">The overage observed this tick to add to the meter.</param>
    /// <exception cref="ArgumentException"><paramref name="cluster"/> is <c>null</c> or empty.</exception>
    public void MeterLocal(string cluster, TenantOverageSample increment)
    {
        ArgumentException.ThrowIfNullOrEmpty(cluster);
        OverageBytes.Increment(cluster, increment.Bytes);
        OverageKeys.Increment(cluster, increment.Keys);
        OverageMemoryBytes.Increment(cluster, increment.MemoryBytes);
        OverageTreeCount.Increment(cluster, increment.TreeCount);
    }

    /// <summary>
    /// Returns the overage metered by <paramref name="cluster"/>'s own replica
    /// component, or <see cref="TenantOverageSample.Empty"/> when that cluster has
    /// metered none. A zero-allocation per-dimension lookup.
    /// </summary>
    /// <param name="cluster">The cluster id whose component to read. Must not be <c>null</c> or empty.</param>
    /// <returns>The cluster's metered overage, or the empty overage when absent.</returns>
    /// <exception cref="ArgumentException"><paramref name="cluster"/> is <c>null</c> or empty.</exception>
    public TenantOverageSample LocalOverage(string cluster)
    {
        ArgumentException.ThrowIfNullOrEmpty(cluster);
        return new TenantOverageSample
        {
            Bytes = Component(OverageBytes, cluster),
            Keys = Component(OverageKeys, cluster),
            MemoryBytes = Component(OverageMemoryBytes, cluster),
            TreeCount = Component(OverageTreeCount, cluster),
        };
    }

    /// <summary>
    /// Folds every cluster's metered overage into the tenant's global metered
    /// overage by summing each dimension's grow-only counter
    /// (<see cref="GCounter.Value"/>). The sum is commutative and associative, so
    /// the result is independent of replica order. A zero-allocation aggregate.
    /// </summary>
    /// <returns>The tenant's global metered overage across all clusters.</returns>
    public TenantOverageSample Fold() =>
        new()
        {
            Bytes = OverageBytes.Value,
            Keys = OverageKeys.Value,
            MemoryBytes = OverageMemoryBytes.Value,
            TreeCount = OverageTreeCount.Value,
        };

    /// <summary>
    /// Folds only the components whose cluster id is in
    /// <paramref name="residentClusters"/> into the tenant's global metered
    /// overage, so a stale component for a cluster that no longer hosts the tenant
    /// is excluded from the converged aggregate.
    /// </summary>
    /// <param name="residentClusters">The set of cluster ids to include. Must not be <c>null</c>.</param>
    /// <returns>The tenant's metered overage restricted to the resident clusters.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="residentClusters"/> is <c>null</c>.</exception>
    public TenantOverageSample Fold(IReadOnlySet<string> residentClusters)
    {
        ArgumentNullException.ThrowIfNull(residentClusters);
        return new TenantOverageSample
        {
            Bytes = FoldResident(OverageBytes, residentClusters),
            Keys = FoldResident(OverageKeys, residentClusters),
            MemoryBytes = FoldResident(OverageMemoryBytes, residentClusters),
            TreeCount = FoldResident(OverageTreeCount, residentClusters),
        };
    }

    /// <summary>
    /// The number of distinct cluster replicas that have metered any overage on any
    /// dimension. Exposed for diagnostics and tests; it materialises the replica
    /// union, so it is not a hot-path read.
    /// </summary>
    public int ClusterCount
    {
        get
        {
            var replicas = new HashSet<string>(StringComparer.Ordinal);
            replicas.UnionWith(OverageBytes.Increments.Keys);
            replicas.UnionWith(OverageKeys.Increments.Keys);
            replicas.UnionWith(OverageMemoryBytes.Increments.Keys);
            replicas.UnionWith(OverageTreeCount.Increments.Keys);
            return replicas.Count;
        }
    }

    /// <summary>
    /// Produces an independent deep copy of this record, so it can be merged or
    /// mutated without affecting the original.
    /// </summary>
    /// <returns>The cloned record.</returns>
    public TenantOverageRecord Clone() =>
        new(Id)
        {
            OverageBytes = OverageBytes.Clone(),
            OverageKeys = OverageKeys.Clone(),
            OverageMemoryBytes = OverageMemoryBytes.Clone(),
            OverageTreeCount = OverageTreeCount.Clone(),
        };

    /// <summary>
    /// Merges <paramref name="other"/> into this record in place, joining each
    /// dimension's grow-only counter with the pointwise-max per replica. The join
    /// is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="other">The record to merge in. Must share this record's <see cref="Id"/>.</param>
    /// <returns>This record, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="other"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="other"/> accounts for a different tenant.</exception>
    public TenantOverageRecord MergeFrom(TenantOverageRecord other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (!Id.Equals(other.Id))
        {
            throw new ArgumentException(
                $"Cannot merge an overage record for tenant '{other.Id}' into a record for tenant '{Id}'.",
                nameof(other));
        }

        OverageBytes.MergeFrom(other.OverageBytes);
        OverageKeys.MergeFrom(other.OverageKeys);
        OverageMemoryBytes.MergeFrom(other.OverageMemoryBytes);
        OverageTreeCount.MergeFrom(other.OverageTreeCount);
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
    public static TenantOverageRecord Merge(TenantOverageRecord left, TenantOverageRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return left.Clone().MergeFrom(right);
    }

    private static long Component(GCounter dimension, string cluster) =>
        dimension.Increments.TryGetValue(cluster, out var value) ? value : 0;

    private static long FoldResident(GCounter dimension, IReadOnlySet<string> residentClusters)
    {
        long total = 0;
        foreach (var (cluster, value) in dimension.Increments)
        {
            if (residentClusters.Contains(cluster))
            {
                total += value;
            }
        }

        return total;
    }
}
