namespace Orleans.Lattice.Membership;

/// <summary>
/// The identity-provider-asserted principal produced by an
/// <see cref="ILatticeCredentialAuthenticator"/> after validating a credential.
/// This is the raw IDP view of the caller <em>before</em> it is merged with the
/// local membership directory into a final <see cref="LatticeSubject"/>.
/// </summary>
[GenerateSerializer]
[Alias(MembershipTypeAliases.LatticePrincipal)]
[Immutable]
public sealed record LatticePrincipal
{
    /// <summary>
    /// Initializes a new <see cref="LatticePrincipal"/>.
    /// </summary>
    /// <param name="subjectId">The stable subject id the IDP asserted. Must not be <c>null</c>.</param>
    /// <param name="issuer">The issuer that vouched for the principal. Must not be <c>null</c>.</param>
    /// <param name="claims">Optional flat claim bag carried from the token, or <c>null</c>.</param>
    /// <param name="assertedGroups">
    /// Optional group ids carried directly in the token, or <c>null</c> when the
    /// token asserts no groups. Merged with directory-derived groups per the
    /// configured <see cref="ILatticeSubjectMapper"/> policy.
    /// </param>
    /// <param name="expiresAt">
    /// The token's expiry (from its <c>exp</c> claim), or <c>null</c> when the
    /// credential carries no expiry. Surfaced so the resolution cache can bound
    /// a resolved subject by token validity.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="subjectId"/> or <paramref name="issuer"/> is <c>null</c>.
    /// </exception>
    public LatticePrincipal(
        string subjectId,
        string issuer,
        IReadOnlyDictionary<string, string>? claims = null,
        IReadOnlyCollection<string>? assertedGroups = null,
        DateTimeOffset? expiresAt = null)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        ArgumentNullException.ThrowIfNull(issuer);
        SubjectId = subjectId;
        Issuer = issuer;
        Claims = claims;
        AssertedGroups = assertedGroups;
        ExpiresAt = expiresAt;
    }

    /// <summary>The stable subject id the identity provider asserted.</summary>
    [Id(0)]
    public string SubjectId { get; init; }

    /// <summary>The issuer that vouched for the principal.</summary>
    [Id(1)]
    public string Issuer { get; init; }

    /// <summary>Optional flat claim bag carried from the token, or <c>null</c>.</summary>
    [Id(2)]
    public IReadOnlyDictionary<string, string>? Claims { get; init; }

    /// <summary>
    /// Optional group ids carried directly in the token, or <c>null</c> when the
    /// token asserts no groups.
    /// </summary>
    [Id(3)]
    public IReadOnlyCollection<string>? AssertedGroups { get; init; }

    /// <summary>
    /// The token's expiry (from its <c>exp</c> claim), or <c>null</c> when the
    /// credential carries no expiry. Bounds how long a resolved subject may be
    /// served from the per-silo resolution cache.
    /// </summary>
    [Id(4)]
    public DateTimeOffset? ExpiresAt { get; init; }
}
