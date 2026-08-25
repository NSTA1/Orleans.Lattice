namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Thrown when a per-tenant overage meter update cannot be applied because a
/// competing writer kept winning the optimistic-concurrency check. The overage
/// store reads the stored overage record with its version, merges this cluster's
/// grow-only counter increment with the CRDT join, and writes back conditionally
/// on that version; a concurrent writer that advances the version between the read
/// and the write forces a re-read and retry. This exception surfaces only when the
/// bounded retry budget is exhausted - a pathological, sustained metering-contention
/// condition on a single tenant - rather than silently dropping the update.
/// </summary>
/// <remarks>
/// Derives directly from <see cref="Exception"/>. It is raised and observed locally
/// on the low-frequency metering cadence and never crosses an Orleans grain
/// boundary, so it carries no <c>[GenerateSerializer]</c> contract.
/// </remarks>
public sealed class TenantOverageConcurrencyException : Exception
{
    /// <summary>Initializes a new <see cref="TenantOverageConcurrencyException"/>.</summary>
    /// <param name="tenant">The tenant whose overage meter update could not be applied.</param>
    /// <param name="attempts">The number of attempts that were made before giving up.</param>
    public TenantOverageConcurrencyException(TenantId tenant, int attempts)
        : base($"Failed to meter tenant overage for tenant '{tenant}' after " +
               $"{attempts} optimistic-concurrency attempts due to sustained concurrent writes.")
    {
        Tenant = tenant;
        Attempts = attempts;
    }

    /// <summary>The tenant whose overage meter update could not be applied.</summary>
    public TenantId Tenant { get; }

    /// <summary>The number of optimistic-concurrency attempts made before giving up.</summary>
    public int Attempts { get; }
}
