namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Thrown when a per-tenant usage-slot publish cannot be applied because a
/// competing writer kept winning the optimistic-concurrency check. The usage
/// store reads the stored usage record with its version, merges this cluster's
/// slot with the CRDT join, and writes back conditionally on that version; a
/// concurrent writer that advances the version between the read and the write
/// forces a re-read and retry. This exception surfaces only when the bounded
/// retry budget is exhausted - a pathological, sustained publish-contention
/// condition on a single tenant - rather than silently dropping the update.
/// </summary>
/// <remarks>
/// Derives directly from <see cref="Exception"/>. It is raised and observed
/// locally on the low-frequency publish cadence and never crosses an Orleans
/// grain boundary, so it carries no <c>[GenerateSerializer]</c> contract.
/// </remarks>
public sealed class TenantUsageConcurrencyException : Exception
{
    /// <summary>Initializes a new <see cref="TenantUsageConcurrencyException"/>.</summary>
    /// <param name="tenant">The tenant whose usage publish could not be applied.</param>
    /// <param name="attempts">The number of attempts that were made before giving up.</param>
    public TenantUsageConcurrencyException(TenantId tenant, int attempts)
        : base($"Failed to publish a tenant usage slot for tenant '{tenant}' after " +
               $"{attempts} optimistic-concurrency attempts due to sustained concurrent writes.")
    {
        Tenant = tenant;
        Attempts = attempts;
    }

    /// <summary>The tenant whose usage publish could not be applied.</summary>
    public TenantId Tenant { get; }

    /// <summary>The number of optimistic-concurrency attempts made before giving up.</summary>
    public int Attempts { get; }
}
