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
        ArgumentException.ThrowIfNullOrEmpty(name);
        return Compose(tenant, name.AsSpan());
    }

    /// <summary>
    /// Span overload of <see cref="Compose(TenantId, string)"/>, so a caller
    /// holding a slice of a larger id composes without first materialising the
    /// intermediate name.
    /// </summary>
    /// <param name="tenant">The owning tenant. Must be an initialised (parsed) tenant id.</param>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be empty.</param>
    /// <returns>The fully-qualified, tenant-scoped tree id.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>
    /// ("no tenant"), or <paramref name="name"/> is empty.
    /// </exception>
    public static string Compose(TenantId tenant, ReadOnlySpan<char> name)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "Cannot compose a tenant-scoped tree id from the uninitialised 'no tenant' value.",
                nameof(tenant));
        }

        if (name.IsEmpty)
        {
            throw new ArgumentException("The tenant-local tree name must not be empty.", nameof(name));
        }

        // Built in a single pass into one exactly-sized buffer rather than by
        // concatenating intermediate strings.
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
    /// Returns the tenant-local portion of <paramref name="treeId"/>: the
    /// <c>{name}</c> of a well-formed <c>t/{tenant}/{name}</c> id, or the id
    /// itself when it is not tenant-scoped.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the seam that keeps <em>classification</em> working after
    /// <em>composition</em>. Several structural decisions are made by testing a
    /// tree id's leading prefix - is this a materialised view, a reserved tree,
    /// a system-data tree - and composing a name into <c>t/{tenant}/</c> moves
    /// that prefix off the front of the string, silently turning every such test
    /// negative. Classifying the local name instead makes the decision
    /// independent of whether the id has been tenant-composed, so a guard cannot
    /// be retired by composition alone.
    /// </para>
    /// <para>
    /// Allocation-free for a non-tenant id (the common case, and every id on a
    /// cluster with tenancy off): the input reference is returned unchanged.
    /// </para>
    /// </remarks>
    /// <param name="treeId">The tree id to reduce to its tenant-local name. Must not be <c>null</c>.</param>
    /// <returns>The tenant-local name, or <paramref name="treeId"/> unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static string LocalName(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var local = LocalName(treeId.AsSpan());

        // Only a genuinely scoped id costs a copy; everything else hands back the
        // caller's own reference.
        return local.Length == treeId.Length ? treeId : new string(local);
    }

    /// <summary>
    /// Span overload of <see cref="LocalName(string)"/>: returns the tenant-local
    /// slice of <paramref name="treeId"/>, or the whole span when it is not
    /// tenant-scoped.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Allocation-free, always.</b> This is the overload every classification
    /// path must use. Deciding "is this a view tree / a tag index / reserved" is
    /// a prefix test on the tenant-local name, and running it through the string
    /// overload would allocate a throwaway copy on every read and every write of
    /// a tenant-scoped tree purely to look at its first few characters.
    /// </para>
    /// </remarks>
    /// <param name="treeId">The tree id to reduce to its tenant-local name.</param>
    /// <returns>The tenant-local slice, or the input span unchanged.</returns>
    public static ReadOnlySpan<char> LocalName(ReadOnlySpan<char> treeId)
    {
        if (!treeId.StartsWith(SegmentPrefix, StringComparison.Ordinal))
        {
            return treeId;
        }

        var rest = treeId[SegmentPrefix.Length..];
        var slash = rest.IndexOf('/');
        if (slash <= 0 || slash >= rest.Length - 1)
        {
            // Malformed: it opens with the reserved prefix but names no tenant or
            // no local name. It is not a legitimate tenant id, so there is no
            // local name to expose; return it unchanged rather than inventing one.
            return treeId;
        }

        if (!TenantId.IsValid(rest[..slash]))
        {
            return treeId;
        }

        return rest[(slash + 1)..];
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
