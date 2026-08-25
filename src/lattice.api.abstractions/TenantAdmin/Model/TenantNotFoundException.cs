namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a tenant-administration operation (suspend, resume, or delete)
/// targets a tenant that is not registered. The control facade fails closed on an
/// unknown tenant rather than silently succeeding, so a transport binding can
/// surface a distinct, typed <c>NotFound</c> outcome instead of a generic
/// failure. Mirrors the sibling <c>TreeNotEmptyException</c> shape: a plain
/// exception deriving directly from <see cref="Exception"/>, carrying the
/// offending tenant id.
/// </summary>
public sealed class TenantNotFoundException : Exception
{
    /// <summary>Initialises the exception for <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The unknown tenant the operation was rejected for.</param>
    public TenantNotFoundException(string tenantId)
        : base($"Tenant '{tenantId}' is not registered.")
        => TenantId = tenantId;

    /// <summary>Initialises the exception with a custom <paramref name="message"/>.</summary>
    public TenantNotFoundException(string tenantId, string message)
        : base(message)
        => TenantId = tenantId;

    /// <summary>The unknown tenant the operation was rejected for.</summary>
    public string TenantId { get; }
}
