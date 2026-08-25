namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a tenant-creation request targets a tenant id that is already
/// registered. Create is not an idempotent upsert: it fails closed on a
/// pre-existing tenant so a caller can distinguish "created" from "already
/// present" and never accidentally reuse or reset another tenant's definition. A
/// transport binding surfaces this as a distinct, typed <c>AlreadyExists</c>
/// outcome. Mirrors the sibling <c>TreeNotEmptyException</c> shape: a plain
/// exception deriving directly from <see cref="Exception"/>, carrying the
/// offending tenant id.
/// </summary>
public sealed class TenantAlreadyExistsException : Exception
{
    /// <summary>Initialises the exception for <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The already-registered tenant the create was rejected for.</param>
    public TenantAlreadyExistsException(string tenantId)
        : base($"Tenant '{tenantId}' already exists.")
        => TenantId = tenantId;

    /// <summary>Initialises the exception with a custom <paramref name="message"/>.</summary>
    public TenantAlreadyExistsException(string tenantId, string message)
        : base(message)
        => TenantId = tenantId;

    /// <summary>The already-registered tenant the create was rejected for.</summary>
    public string TenantId { get; }
}
