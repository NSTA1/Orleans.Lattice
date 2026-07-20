namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// A single principal record as read from Microsoft Graph by
/// <see cref="IEntraGraphDirectoryClient"/>, before subject-id shaping. Carries
/// both the object id and (for users) the user principal name so
/// <see cref="EntraGraphIdentityDirectory"/> can shape
/// <see cref="DirectoryPrincipal.Id"/> per the configured
/// <see cref="EntraDirectorySubjectIdSource"/>. Transport-free: it never crosses a
/// grain or wire boundary, so it carries no serialization attributes.
/// </summary>
/// <param name="ObjectId">The Entra object id (<c>oid</c>) of the principal.</param>
/// <param name="DisplayName">A human-readable display name for the principal.</param>
/// <param name="UserPrincipalName">
/// The user principal name for a user, or <c>null</c> for a group (groups have no
/// UPN).
/// </param>
/// <param name="Kind">Whether the principal is a user or a group.</param>
internal sealed record EntraDirectoryRecord(
    string ObjectId,
    string DisplayName,
    string? UserPrincipalName,
    DirectoryPrincipalKind Kind);
