namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// Selects which Entra identifier the <see cref="EntraGraphIdentityDirectory"/>
/// records as a <see cref="DirectoryPrincipal.Id"/>, so directory validation
/// matches what the active authenticator uses as its subject claim. Applies to
/// users; groups always use the Entra object id because they have no user
/// principal name.
/// </summary>
public enum EntraDirectorySubjectIdSource
{
    /// <summary>
    /// Use the Entra object id (<c>oid</c>). The default, matching a typical
    /// Entra deployment whose <see cref="JwtAuthenticatorOptions.SubjectClaimTypes"/>
    /// resolves the subject to the object id.
    /// </summary>
    ObjectId = 0,

    /// <summary>
    /// Use the user principal name (UPN). Choose this when the configured
    /// subject claim maps to the UPN rather than the object id. Groups still use
    /// the object id.
    /// </summary>
    UserPrincipalName = 1,
}
