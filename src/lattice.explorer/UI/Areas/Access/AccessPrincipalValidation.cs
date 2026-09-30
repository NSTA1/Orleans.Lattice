using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The fail-closed pre-check a group create or a member add makes against the
/// identity directory: when the cluster has a directory, an id that does not
/// resolve, or resolves to the other kind, is refused before anything is written.
/// Without a directory the id is taken as typed, as the form says. The server
/// validates again on write, and its refusal is shown the same way.
/// </summary>
internal static class AccessPrincipalValidation
{
    /// <summary>
    /// Returns the sentence to show beside the id field, or <see langword="null"/>
    /// when the id may be written.
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
        if (model?.DirectoryAvailable != true)
        {
            return null;
        }

        var principal = await admin.ResolveDirectoryPrincipalAsync(principalId, cancellationToken).ConfigureAwait(true);
        var directory = DirectoryName(model);
        if (principal is null)
        {
            return $"{principalId} is not a {Word(expected)} in the identity directory ({directory}).";
        }

        return principal.Kind == expected
            ? null
            : $"{principalId} is a {Word(principal.Kind)} in the identity directory ({directory}), not a {Word(expected)}.";
    }

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
