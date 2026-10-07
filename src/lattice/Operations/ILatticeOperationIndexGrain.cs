namespace Orleans.Lattice.Operations;

/// <summary>
/// The per-tenant index of coordinated operations, keyed by
/// <see cref="LatticeOperationKey.ForIndex"/>. It records which operations exist so
/// they can be listed newest-first after whatever started them is gone, and
/// prunes finished operations once their retention window has elapsed.
/// </summary>
[Alias(TypeAliases.ILatticeOperationIndexGrain)]
internal interface ILatticeOperationIndexGrain : IGrainWithStringKey
{
    /// <summary>Reconciles one durable operation snapshot, ignoring superseded generations and expired additions.</summary>
    /// <param name="record">The persisted snapshot, including its generation's start time.</param>
    /// <param name="remove">Whether this generation has expired and must be removed.</param>
    Task ReconcileAsync(LatticeOperationRecord record, bool remove);

    /// <summary>Records a newly started operation. Idempotent.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="kind">The operation kind.</param>
    /// <param name="startedAtUtc">When it started.</param>
    Task AddAsync(string operationId, string kind, DateTimeOffset startedAtUtc);

    /// <summary>Records that an operation finished. A no-op for an unknown id.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="finishedAtUtc">When it finished.</param>
    Task MarkFinishedAsync(string operationId, DateTimeOffset finishedAtUtc);

    /// <summary>Removes an operation. A no-op for an unknown id.</summary>
    /// <param name="operationId">The operation id.</param>
    Task RemoveAsync(string operationId);

    /// <summary>Lists one page of operation ids, newest-first by start time.</summary>
    /// <param name="kindPrefix">When not <see langword="null"/>, only kinds starting with this prefix (ordinal) are listed.</param>
    /// <param name="pageToken">The previous page's token, or <see langword="null"/> to start at the newest.</param>
    /// <param name="pageSize">The maximum ids to return. Must be positive.</param>
    /// <returns>The page.</returns>
    /// <exception cref="ArgumentException"><paramref name="pageToken"/> is malformed.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    Task<LatticeOperationIndexPage> ListAsync(string? kindPrefix, string? pageToken, int pageSize);
}
