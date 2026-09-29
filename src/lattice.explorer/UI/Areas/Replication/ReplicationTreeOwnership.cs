using System.Diagnostics.CodeAnalysis;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// App ownership of a replicated tree, derived from its logical id: a tree whose id
/// is <c>a/{slug}/...</c> lives in the app's structural namespace (#2235 D1), so its
/// enrolment follows the app install rather than a per-tree toggle here.
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

    /// <summary>Reads the owning app's slug from a logical tree id.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="slug">The owning app's slug.</param>
    /// <returns><see langword="true"/> when the tree is <c>a/{slug}/{name}</c> with a well-formed slug and a name.</returns>
    public static bool TryGetAppSlug(string? treeId, [NotNullWhen(true)] out string? slug)
    {
        slug = null;
        if (treeId is null || !treeId.StartsWith(AppPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var end = treeId.IndexOf('/', AppPrefix.Length);
        if (end <= AppPrefix.Length || end == treeId.Length - 1)
        {
            return false;
        }

        var candidate = treeId[AppPrefix.Length..end];
        if (!ExplorerAddressEncoding.IsKeyword(candidate))
        {
            return false;
        }

        slug = candidate;
        return true;
    }

    /// <summary>Whether a logical tree id belongs to an app.</summary>
    /// <param name="treeId">The logical tree id.</param>
    public static bool IsAppOwned(string? treeId) => TryGetAppSlug(treeId, out _);

    /// <summary>The app's own replication page, where its enrolment is managed.</summary>
    /// <param name="slug">The app slug.</param>
    public static ExplorerAddress AppReplicationAddress(string slug) =>
        ExplorerAddress.ForArea("apps", slug, "replication");
}
