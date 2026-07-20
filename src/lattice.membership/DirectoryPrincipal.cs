namespace Orleans.Lattice.Membership;

/// <summary>
/// A principal (user or group) as it exists in the configured external identity
/// source, returned by <see cref="ILatticeIdentityDirectory"/> when browsing,
/// searching, or resolving an id. This is the upstream directory's view of a
/// principal, distinct from the locally-persisted <see cref="MembershipGroup"/>
/// records: it is used to validate that a supplied id actually exists (and what
/// it is) before an operator grants it access.
/// <para>
/// A transport-free model: it never crosses a grain or wire boundary in the
/// server-only identity-directory seam, so it carries no Orleans serialization
/// attributes. The wire DTOs the facade later exposes are owned separately.
/// </para>
/// </summary>
/// <param name="Id">The stable principal id in the external identity source.</param>
/// <param name="DisplayName">A human-readable display name for the principal.</param>
/// <param name="Kind">Whether the principal is a user or a group.</param>
/// <param name="Claims">
/// An optional flat claim bag carried from the identity source. <c>null</c> when
/// the provider surfaces no claims for the principal.
/// </param>
public sealed record DirectoryPrincipal(
    string Id,
    string DisplayName,
    DirectoryPrincipalKind Kind,
    IReadOnlyDictionary<string, string>? Claims = null);
