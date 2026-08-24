namespace Orleans.Lattice;

/// <summary>
/// The reserved <c>t/</c> structural tenant namespace: a third reserved
/// tree-name namespace alongside the existing <c>_lattice_</c> (system-internal)
/// and <c>sys-</c> (system-data) namespaces. A tenant's trees are named
/// <c>t/{tenantId}/{name}</c>, where <c>{tenantId}</c> is a valid
/// <see cref="TenantId"/> and <c>{name}</c> is the tenant-local, unqualified
/// tree name. This helper is the single place that defines the segment prefix
/// and the compose / parse operations over it, mirroring how
/// <c>Orleans.Lattice.Schema.LatticeSchemaReservedTrees</c> centralises the
/// schema package's reserved namespace.
/// </summary>
/// <remarks>
/// The parse helpers are allocation-free on the common negative path: a legacy
/// (non-tenant) tree id fails the <see cref="SegmentPrefix"/> check with a span
/// comparison and allocates nothing. A single string is materialised only when a
/// tenant id is actually extracted, which is unavoidable because
/// <see cref="TenantId"/> holds a <see cref="string"/>.
/// </remarks>
public static class LatticeTenantTrees
{
    /// <summary>
    /// The reserved segment prefix (<c>t/</c>) that opens every tenant-scoped
    /// tree id. A tree id starting with this prefix is owned by a tenant; every
    /// other tree id is cluster-global (legacy) state.
    /// </summary>
    public const string SegmentPrefix = "t/";

    /// <summary>
    /// Composes the segmented tree id <c>t/{tenant}/{name}</c> for a tenant-local
    /// tree.
    /// </summary>
    /// <param name="tenant">The owning tenant. Must be an initialised (parsed) tenant id.</param>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <returns>The fully-qualified, tenant-scoped tree id.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>
    /// ("no tenant"), or <paramref name="name"/> is <c>null</c> or empty.
    /// </exception>
    public static string Compose(TenantId tenant, string name)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "Cannot compose a tenant-scoped tree id from the uninitialised 'no tenant' value.",
                nameof(tenant));
        }

        ArgumentException.ThrowIfNullOrEmpty(name);

        return string.Concat(SegmentPrefix, tenant.Value, "/", name);
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="treeId"/> is a tenant-scoped tree
    /// id (it opens with the reserved <see cref="SegmentPrefix"/>). A cheap,
    /// allocation-free prefix check.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the id is tenant-scoped; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static bool IsTenantScoped(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return treeId.StartsWith(SegmentPrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Extracts the owning <see cref="TenantId"/> from a segmented tree id.
    /// Allocation-free unless a valid tenant id is found (then one string for the
    /// tenant id is materialised).
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <param name="tenant">
    /// The owning tenant when this returns <c>true</c>; otherwise <c>default</c>.
    /// </param>
    /// <returns>
    /// <c>true</c> when <paramref name="treeId"/> is a well-formed tenant-scoped
    /// tree id (<c>t/{tenantId}/{name}</c> with a valid tenant id and a non-empty
    /// name); otherwise <c>false</c>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static bool TryGetTenant(string treeId, out TenantId tenant)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var span = treeId.AsSpan();
        if (!span.StartsWith(SegmentPrefix, StringComparison.Ordinal))
        {
            tenant = default;
            return false;
        }

        var rest = span[SegmentPrefix.Length..];
        var slash = rest.IndexOf('/');

        // A tenant id must be present (slash > 0) and a non-empty tenant-local
        // name must follow it (slash < rest.Length - 1).
        if (slash <= 0 || slash >= rest.Length - 1)
        {
            tenant = default;
            return false;
        }

        var idSpan = rest[..slash];
        if (!TenantId.IsValid(idSpan))
        {
            tenant = default;
            return false;
        }

        tenant = TenantId.ForValidated(new string(idSpan));
        return true;
    }
}
