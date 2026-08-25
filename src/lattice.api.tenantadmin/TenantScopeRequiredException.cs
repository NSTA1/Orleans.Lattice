namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a tenant-scoped tree-administration or schema operation is invoked
/// with no active tenant in scope. The tenant-scoped facade never accepts a
/// wire-supplied tenant id: it derives the operating tenant solely from the
/// ambient <see cref="Orleans.Lattice.LatticeActiveTenantContext"/>, so a call
/// made outside a validated active-tenant scope has no namespace to confine to
/// and is refused fail-closed rather than silently defaulting to a tenant. A
/// transport binding surfaces this as an unauthenticated / failed-precondition
/// outcome. Mirrors the sibling tenant-admin exceptions: a plain exception
/// deriving directly from <see cref="Exception"/>.
/// </summary>
public sealed class TenantScopeRequiredException : Exception
{
    private const string DefaultMessage =
        "No active tenant is in scope. A tenant-scoped tree or schema operation must run inside a validated "
        + "active-tenant scope (LatticeActiveTenantContext); the operation is refused because it has no tenant "
        + "namespace to confine to.";

    /// <summary>
    /// Initialises a new <see cref="TenantScopeRequiredException"/> with the
    /// default fail-closed message.
    /// </summary>
    public TenantScopeRequiredException()
        : base(DefaultMessage)
    {
    }

    /// <summary>
    /// Initialises a new <see cref="TenantScopeRequiredException"/> with a custom
    /// <paramref name="message"/>.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public TenantScopeRequiredException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initialises a new <see cref="TenantScopeRequiredException"/> with a custom
    /// <paramref name="message"/> and an <paramref name="innerException"/>.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of this exception.</param>
    public TenantScopeRequiredException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
