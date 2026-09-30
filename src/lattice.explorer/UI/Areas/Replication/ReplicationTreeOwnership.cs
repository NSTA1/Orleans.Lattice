using System.Diagnostics.CodeAnalysis;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// App ownership of a replicated tree, derived from its id: a tree whose id is
/// <c>a/{slug}/...</c> lives in the app's structural namespace (#2235 D1), so its
/// enrolment follows the app install rather than a per-tree toggle here. Under a
/// non-default tenant both replication reports name the tenant's trees by their
/// qualified <c>t/{tenant}/{name}</c> id (#4000), so the tenant qualification is
/// read past before the app prefix is looked for.
/// </summary>
/// <remarks>
/// The replication facades carry no app provenance (<c>ReplicationEnrollmentSource</c>
/// is Runtime, Static or both), so the prefix is the only evidence. The slug is shown
/// as text and only ever used as an address segment.
/// </remarks>
internal static class ReplicationTreeOwnership
{
    /// <summary>The structural prefix of an app-owned tree.</summary>
    public const string AppPrefix = "a/";

    /// <summary>Reads the owning app's slug from a tree id, bare or tenant-qualified.</summary>
    /// <param name="treeId">The tree id, as a replication report names it.</param>
    /// <param name="slug">The owning app's slug.</param>
    /// <returns>
    /// <see langword="true"/> when the tree is <c>a/{slug}/{name}</c>, or
    /// <c>t/{tenant}/a/{slug}/{name}</c>, with a well-formed slug and a name.
    /// </returns>
    public static bool TryGetAppSlug(string? treeId, [NotNullWhen(true)] out string? slug)
    {
        slug = null;
        if (treeId is null)
        {
            return false;
        }

        var start = TenantLocalStart(treeId);
        if (!treeId.AsSpan(start).StartsWith(AppPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var slugStart = start + AppPrefix.Length;
        var end = treeId.IndexOf('/', slugStart);
        if (end <= slugStart || end == treeId.Length - 1)
        {
            return false;
        }

        var candidate = treeId[slugStart..end];
        if (!ExplorerAddressEncoding.IsKeyword(candidate))
        {
            return false;
        }

        slug = candidate;
        return true;
    }

    /// <summary>Whether a tree id, bare or tenant-qualified, belongs to an app.</summary>
    /// <param name="treeId">The tree id.</param>
    public static bool IsAppOwned(string? treeId) => TryGetAppSlug(treeId, out _);

    /// <summary>The app's own replication page, where its enrolment is managed.</summary>
    /// <param name="slug">The app slug.</param>
    public static ExplorerAddress AppReplicationAddress(string slug) =>
        ExplorerAddress.ForArea("apps", slug, "replication");

    /// <summary>
    /// Where the tenant-local name starts: past a well-formed <c>t/{tenant}/</c>
    /// qualification with something after it, otherwise at the start.
    /// </summary>
    private static int TenantLocalStart(string treeId)
    {
        if (!treeId.StartsWith(ExplorerTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            return 0;
        }

        var ownerEnd = treeId.IndexOf('/', ExplorerTenantTrees.SegmentPrefix.Length);
        return ownerEnd > ExplorerTenantTrees.SegmentPrefix.Length && ownerEnd < treeId.Length - 1 ? ownerEnd + 1 : 0;
    }
}
