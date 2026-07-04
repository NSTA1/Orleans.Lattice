namespace Orleans.Lattice.Membership;

/// <summary>
/// The independent-IDP seam: validates a <see cref="LatticeCredential"/> and
/// resolves it into a <see cref="LatticePrincipal"/>. Multiple authenticators
/// coexist on a silo (for example one per issuer - Entra, Okta, mTLS,
/// shared-secret); the resolution path selects the first whose
/// <see cref="CanHandle"/> returns <c>true</c>. Selection is by the
/// credential's scheme / issuer hint, parsed from the token only when the hint
/// is absent, so an authenticator never validates a token it does not own.
/// </summary>
public interface ILatticeCredentialAuthenticator
{
    /// <summary>
    /// Returns <c>true</c> when this authenticator recognizes
    /// <paramref name="credential"/> (by its scheme / issuer hint, or by an
    /// issuer parsed from the token when the hint is absent) and should be given
    /// the chance to resolve it. Must be cheap and side-effect free: it runs for
    /// every registered authenticator until one matches.
    /// </summary>
    /// <param name="credential">The ambient credential to test.</param>
    bool CanHandle(in LatticeCredential credential);

    /// <summary>
    /// Validates <paramref name="credential"/> and resolves it into a
    /// <see cref="LatticePrincipal"/>, or returns <c>null</c> when the credential
    /// is invalid or expired, or is validly signed but asserts no authorizable
    /// subject (a missing / empty subject claim, or a subject that collides with a
    /// reserved well-known id such as <see cref="LatticeSubject.AnonymousSubjectId"/>
    /// or <see cref="LatticeSubject.SystemSubjectId"/>). Only called after
    /// <see cref="CanHandle"/> returned <c>true</c>. A <c>null</c> result resolves
    /// the caller to <see cref="LatticeSubject.Anonymous"/> - never to a stale
    /// authorized subject, and never to an anonymous-labelled subject that still
    /// carries token-asserted groups or roles.
    /// </summary>
    /// <param name="credential">The credential to validate and resolve.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default);
}
