namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Publishes a tenant-registry change to every silo in the cluster. Called by the
/// silo whose grain committed the write, before that write returns to its caller.
/// </summary>
internal interface ITenantPolicyEpochPublisher
{
    /// <summary>
    /// Advances the cluster epoch and completes once every silo has either marked
    /// its compiled tenant-policy snapshot out of date or lost its authority.
    /// </summary>
    /// <param name="cancellationToken">Cancels the caller's wait.</param>
    Task AdvanceAsync(CancellationToken cancellationToken);
}
