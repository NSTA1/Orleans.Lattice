namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Thrown when a tenant-registry update targets a tenant whose record is not
/// stored. <see cref="ITenantRegistry.PutAsync"/> merges into an existing record
/// and never creates one, so a caller that read a record and wrote it back after
/// the tenant was deleted is refused rather than re-creating the deleted tenant
/// from its stale copy. Only <see cref="ITenantRegistry.CreateAsync"/> creates a
/// record.
/// </summary>
/// <remarks>
/// Derives directly from <see cref="Exception"/>. Like
/// <see cref="TenantRegistryConcurrencyException"/>, it is raised and observed
/// locally in the registry singleton and never crosses an Orleans grain
/// boundary, so it carries no <c>[GenerateSerializer]</c> contract.
/// </remarks>
public sealed class TenantRecordNotFoundException : Exception
{
    /// <summary>Initializes a new <see cref="TenantRecordNotFoundException"/>.</summary>
    /// <param name="tenant">The tenant whose record was not stored.</param>
    public TenantRecordNotFoundException(TenantId tenant)
        : base($"Tenant '{tenant}' has no registry record to update; it may have been deleted.")
    {
        Tenant = tenant;
    }

    /// <summary>The tenant whose record was not stored.</summary>
    public TenantId Tenant { get; }
}
