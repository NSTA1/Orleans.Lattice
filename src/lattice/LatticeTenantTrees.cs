using Orleans.Lattice.BPlusTree;

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
    /// Returns the tree-id prefix that every tree owned by <paramref name="tenant"/>
    /// begins with - <c>t/{tenantId}/</c>. Because tree ids are ordinally sorted,
    /// this prefix bounds the tenant's trees into a single contiguous key range,
    /// which lets an enumeration be pushed down to a range scan instead of a full
    /// catalog walk.
    /// </summary>
    /// <param name="tenant">The owning tenant. Must not be the "no tenant" value.</param>
    /// <returns>The tenant's tree-id prefix, including the trailing separator.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is the uninitialised "no tenant" value.
    /// </exception>
    public static string ComposePrefix(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "Cannot compose a tenant-scoped tree prefix from the uninitialised 'no tenant' value.",
                nameof(tenant));
        }

        return string.Concat(SegmentPrefix, tenant.Value, "/");
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

    /// <summary>
    /// Derives the owning <see cref="TreeOwnership"/> of a tree from its id
    /// alone. Ownership is never stored (the tree registry keeps no tenant
    /// column), so it is always re-computed from the id's structural prefix.
    /// </summary>
    /// <param name="treeId">The tree id to classify. Must not be <c>null</c>.</param>
    /// <returns>
    /// <see cref="TreeOwnership.Platform"/> for a system id (the
    /// <c>_lattice_</c> system-internal or <c>sys-</c> system-data namespace);
    /// the owning tenant for a well-formed tenant-scoped id
    /// (<c>t/{tenantId}/{name}</c>); and the reserved
    /// <see cref="TenantId.Default"/> tenant for a bare, unsegmented legacy id
    /// (default-tenant adoption).
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    /// <remarks>
    /// A tree id that opens with <see cref="SegmentPrefix"/> but is malformed
    /// (no valid tenant id and local name) is not a bare legacy id, so it is not
    /// adopted by the default tenant; it classifies as
    /// <see cref="TreeOwnership.Platform"/> so it can never leak into a tenant's
    /// view. Such an id is uncreatable in any case - the public data-plane
    /// user-write guard refuses direct writes to the reserved <c>t/</c>
    /// namespace.
    /// </remarks>
    public static TreeOwnership GetOwner(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var span = treeId.AsSpan();

        // System namespaces are platform-owned and sit outside every tenant.
        if (span.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            || span.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal))
        {
            return TreeOwnership.Platform;
        }

        // A well-formed tenant-scoped id is owned by the tenant it names.
        if (TryGetTenant(treeId, out var tenant))
        {
            return TreeOwnership.ForTenant(tenant);
        }

        // A malformed id in the reserved t/ namespace is not a bare legacy id;
        // treat it as platform-owned so it is excluded from every tenant view.
        if (span.StartsWith(SegmentPrefix, StringComparison.Ordinal))
        {
            return TreeOwnership.Platform;
        }

        // A bare, unsegmented legacy id is adopted by the reserved default tenant.
        return TreeOwnership.ForTenant(TenantId.Default);
    }
}
