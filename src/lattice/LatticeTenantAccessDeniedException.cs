namespace Orleans.Lattice;

/// <summary>
/// The fail-closed denial surfaced at the <see cref="ILattice"/> tenant-resolution
/// boundary when the active-tenant context resolver denies an operation because
/// the caller has no valid active tenant (an absent or invalid active tenant, or
/// - for a multi-membership subject - none asserted). Composing a tenant-scoped
/// tree id is refused rather than silently defaulting, so a request that cannot
/// be attributed to a tenant never resolves an ambiguous or cross-tenant tree.
/// </summary>
/// <remarks>
/// <para>
/// The core no-op <see cref="NullTenantContextResolver"/> always resolves the
/// reserved <see cref="TenantId.Default"/> and therefore never raises this
/// exception, so a cluster with no tenancy add-on behaves byte-for-byte as it
/// did before tenancy existed. The tenancy add-on's real resolver applies the
/// membership rules (a single-membership subject defaults implicitly; a
/// multi-membership subject must assert one; absent or invalid is denied) and
/// signals a denial by resolving the uninitialised "no tenant" value, which the
/// resolution boundary turns into this exception.
/// </para>
/// <para>
/// It derives directly from <see cref="Exception"/> so its
/// <see cref="GenerateSerializerAttribute"/> serializer and same-silo deep
/// copier are correct with no additional copier registration.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTenantAccessDenied)]
public sealed class LatticeTenantAccessDeniedException : Exception
{
    /// <summary>
    /// Initializes a new instance of the
    /// <see cref="LatticeTenantAccessDeniedException"/> class with a default
    /// message.
    /// </summary>
    public LatticeTenantAccessDeniedException()
        : base("The operation was denied: no valid active tenant is present for the caller.")
    {
    }

    /// <summary>
    /// Initializes a new instance of the
    /// <see cref="LatticeTenantAccessDeniedException"/> class with the specified
    /// <paramref name="message"/>.
    /// </summary>
    /// <param name="message">The message that describes the denial.</param>
    public LatticeTenantAccessDeniedException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the
    /// <see cref="LatticeTenantAccessDeniedException"/> class with the specified
    /// <paramref name="message"/> and <paramref name="innerException"/>.
    /// </summary>
    /// <param name="message">The message that describes the denial.</param>
    /// <param name="innerException">The underlying cause of the denial.</param>
    public LatticeTenantAccessDeniedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
