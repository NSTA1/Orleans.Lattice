using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The fail-closed pre-check a group create or a member add makes against the
/// identity directory and the auth store: when the cluster has a directory, an
/// id that no source resolves, or that resolves to the other kind, is refused
/// before anything is written. A group stored in the Access area is a source of
/// its own unless the cluster requires every group to be in the directory
/// (<see cref="AccessModelDescriptor.DirectoryValidationRequired"/>). Without a
/// directory the id is taken as typed, as the form says. A reserved tenant group
/// id (<c>t/...</c>) is refused whatever the directory: it is never a cluster
/// group or a member of one. The server validates again on write, and its
/// refusal is shown the same way.
/// </summary>
internal static class AccessPrincipalValidation
{
    /// <summary>
    /// Returns the sentence to show beside the id field, or <see langword="null"/>
    /// when the id may be written as an existing member.
    /// </summary>
    /// <param name="admin">The auth facade.</param>
    /// <param name="model">The access model, or <see langword="null"/> when unknown.</param>
    /// <param name="principalId">The id to check.</param>
    /// <param name="expected">The kind the id must be.</param>
    /// <param name="cancellationToken">Cancels the lookup.</param>
    public static async Task<string?> ValidateAsync(
        ILatticeAuthAdmin admin,
        AccessModelDescriptor? model,
        string principalId,
        DirectoryPrincipalKind expected,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        ArgumentNullException.ThrowIfNull(principalId);
        if (ClusterSubjectSuggestionSource.IsTenantGroupId(principalId))
        {
            return TenantGroupMessage(principalId);
        }

        if (model?.DirectoryAvailable != true)
        {
            return null;
        }

        var principal = await admin.ResolveDirectoryPrincipalAsync(principalId, cancellationToken).ConfigureAwait(true);
        var directory = DirectoryName(model);
        if (principal is not null)
        {
            return principal.Kind == expected
                ? null
                : $"{principalId} is a {Word(principal.Kind)} in the identity directory ({directory}), not a {Word(expected)}.";
        }

        if (expected == DirectoryPrincipalKind.Group && !model.DirectoryValidationRequired)
        {
            // A group administered here need not be in the directory.
            return await admin.GetGroupAsync(principalId, cancellationToken).ConfigureAwait(true) is null
                ? $"{principalId} is not a group in the identity directory ({directory}) or a group stored here."
                : null;
        }

        return $"{principalId} is not a {Word(expected)} in the identity directory ({directory}).";
    }

    /// <summary>
    /// Returns the sentence to show beside a new group's id, or <see langword="null"/>
    /// when the group may be created. With a directory that the cluster requires,
    /// the id must be a directory group; otherwise any id that is not a directory
    /// user may name a group stored here.
    /// </summary>
    /// <param name="admin">The auth facade.</param>
    /// <param name="model">The access model, or <see langword="null"/> when unknown.</param>
    /// <param name="groupId">The new group's id.</param>
    /// <param name="cancellationToken">Cancels the lookup.</param>
    public static async Task<string?> ValidateNewGroupAsync(
        ILatticeAuthAdmin admin,
        AccessModelDescriptor? model,
        string groupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        ArgumentNullException.ThrowIfNull(groupId);
        if (ClusterSubjectSuggestionSource.IsTenantGroupId(groupId))
        {
            return TenantGroupMessage(groupId);
        }

        if (model?.DirectoryAvailable != true)
        {
            return null;
        }

        var principal = await admin.ResolveDirectoryPrincipalAsync(groupId, cancellationToken).ConfigureAwait(true);
        var directory = DirectoryName(model);
        if (principal is not null)
        {
            return principal.Kind == DirectoryPrincipalKind.Group
                ? null
                : $"{groupId} is a {Word(principal.Kind)} in the identity directory ({directory}), not a group.";
        }

        return model.DirectoryValidationRequired
            ? $"{groupId} is not a group in the identity directory ({directory}), and this cluster stores only directory groups."
            : null;
    }

    /// <summary>The sentence a reserved tenant group id is refused with here.</summary>
    /// <param name="id">The id.</param>
    internal static string TenantGroupMessage(string id) =>
        $"{id} is in the reserved tenant group namespace (t/). Tenant groups belong to their tenant and are never cluster groups or their members.";

    /// <summary>
    /// The identity directory's name as a sentence shows it: <c>static roster</c>,
    /// <c>Microsoft Entra ID</c>, or the provider's own id for any other directory.
    /// </summary>
    /// <param name="model">The access model.</param>
    public static string DirectoryName(AccessModelDescriptor model)
    {
        ArgumentNullException.ThrowIfNull(model);
        return model.DirectoryProviderId switch
        {
            "static" => "static roster",
            "entra" => "Microsoft Entra ID",
            { Length: > 0 } provider => provider,
            _ => "unnamed",
        };
    }

    private static string Word(DirectoryPrincipalKind kind) => kind == DirectoryPrincipalKind.Group ? "group" : "user";
}
