namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Thrown when a tenant-registry write cannot be applied because a competing
/// writer kept winning the optimistic-concurrency check. The registry reads the
/// stored record with its version, merges the caller's change with the CRDT
/// join, and writes back conditionally on that version; a concurrent writer that
/// advances the version between the read and the write forces a re-read and
/// retry. This exception surfaces only when the bounded retry budget is exhausted
/// - a pathological, sustained write-contention condition on a single tenant -
/// rather than silently dropping the caller's change.
/// </summary>
/// <remarks>
/// Derives directly from <see cref="Exception"/>. It is raised and observed
/// locally in the registry singleton and never crosses an Orleans grain
/// boundary, so it carries no <c>[GenerateSerializer]</c> contract.
/// </remarks>
public sealed class TenantRegistryConcurrencyException : Exception
{
    /// <summary>Initializes a new <see cref="TenantRegistryConcurrencyException"/>.</summary>
    /// <param name="tenant">The tenant whose write could not be applied.</param>
    /// <param name="attempts">The number of attempts that were made before giving up.</param>
    public TenantRegistryConcurrencyException(TenantId tenant, int attempts)
        : base($"Failed to apply a tenant-registry write for tenant '{tenant}' after " +
               $"{attempts} optimistic-concurrency attempts due to sustained concurrent writes.")
    {
        Tenant = tenant;
        Attempts = attempts;
    }

    /// <summary>The tenant whose registry write could not be applied.</summary>
    public TenantId Tenant { get; }

    /// <summary>The number of optimistic-concurrency attempts made before giving up.</summary>
    public int Attempts { get; }
}
