namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// Client-side helpers for reasoning about tenant ownership of a physical tree
/// id, mirrored from the cluster's <c>LatticeTenantTrees</c> convention. The
/// Explorer's Core project must not reference Orleans.Lattice core, so the
/// <c>t/</c> ownership grammar is reproduced here as string literals held locally
/// (the same pattern <c>CatalogReader</c> uses for the <c>view-</c> prefix).
/// </summary>
/// <remarks>
/// The cluster composes a tenant-owned tree as <c>t/{tenant}/{name}</c>. A tree
/// id with no such prefix is a legacy, un-prefixed id owned by the default tenant
/// (<see cref="ExplorerTenantId.Default"/>). Ids that name platform-internal
/// state (reserved <c>_</c>-prefixed trees and <c>sys-</c> system-data trees) are
/// owned by no tenant and are never attributed to one.
/// </remarks>
public static class ExplorerTenantTrees
{
    /// <summary>The reserved ownership prefix for a tenant-owned tree, mirrored from <c>LatticeTenantTrees.SegmentPrefix</c>.</summary>
    public const string SegmentPrefix = "t/";

    /// <summary>The default tenant id owning legacy, un-prefixed trees, mirrored from <c>TenantId.DefaultId</c>.</summary>
    public const string DefaultTenantId = "default";

    /// <summary>The reserved prefix for system-data trees, mirrored from the cluster's system-data convention.</summary>
    private const string SystemDataPrefix = "sys-";

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="treeId"/> is owned by
    /// <paramref name="tenant"/>. A well-formed <c>t/{owner}/{name}</c> id is owned
    /// by <c>owner</c>; a legacy, un-prefixed id is owned by
    /// <see cref="ExplorerTenantId.Default"/>; a platform-internal or malformed id
    /// is owned by no tenant. The comparison is allocation-free (span-based,
    /// ordinal).
    /// </summary>
    /// <param name="treeId">The physical tree id to classify. Must not be <see langword="null"/>.</param>
    /// <param name="tenant">The candidate owning tenant.</param>
    /// <returns><see langword="true"/> when the tree is owned by the tenant.</returns>
    public static bool IsOwnedBy(string treeId, ExplorerTenantId tenant)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var span = treeId.AsSpan();
        if (span.StartsWith(SegmentPrefix, StringComparison.Ordinal))
        {
            var rest = span[SegmentPrefix.Length..];
            var slash = rest.IndexOf('/');

            // A well-formed id needs a non-empty owner and a non-empty name after
            // it; anything else is malformed and owned by no tenant.
            if (slash <= 0 || slash >= rest.Length - 1)
            {
                return false;
            }

            return rest[..slash].Equals(tenant.Value.AsSpan(), StringComparison.Ordinal);
        }

        if (IsPlatformInternal(span))
        {
            return false;
        }

        // A legacy, un-prefixed id belongs to the default tenant.
        return tenant.Value.AsSpan().Equals(DefaultTenantId, StringComparison.Ordinal);
    }

    /// <summary>
    /// Resolves the tenant that owns <paramref name="treeId"/>. Returns
    /// <see langword="false"/> (with <paramref name="owner"/> set to
    /// <see langword="default"/>) for a platform-internal or malformed id that
    /// belongs to no tenant.
    /// </summary>
    /// <param name="treeId">The physical tree id to classify. Must not be <see langword="null"/>.</param>
    /// <param name="owner">The owning tenant when the method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when an owning tenant was resolved.</returns>
    public static bool TryGetOwner(string treeId, out ExplorerTenantId owner)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var span = treeId.AsSpan();
        if (span.StartsWith(SegmentPrefix, StringComparison.Ordinal))
        {
            var rest = span[SegmentPrefix.Length..];
            var slash = rest.IndexOf('/');
            if (slash <= 0 || slash >= rest.Length - 1)
            {
                owner = default;
                return false;
            }

            owner = new ExplorerTenantId(rest[..slash].ToString());
            return true;
        }

        if (IsPlatformInternal(span))
        {
            owner = default;
            return false;
        }

        owner = ExplorerTenantId.Default;
        return true;
    }

    private static bool IsPlatformInternal(ReadOnlySpan<char> treeId) =>
        (treeId.Length > 0 && treeId[0] == '_') ||
        treeId.StartsWith(SystemDataPrefix, StringComparison.Ordinal);
}
