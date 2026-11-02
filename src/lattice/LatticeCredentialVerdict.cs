namespace Orleans.Lattice;

/// <summary>
/// What the cluster made of the credential on the ambient
/// <see cref="LatticeCredentialContext"/>, as reported by
/// <see cref="ILatticeMembershipContext.VerifyCurrentCredentialAsync"/>. It
/// separates a caller who presented no credential from one whose credential a
/// registered authenticator refused - two callers the access gate treats alike
/// (both resolve to <see cref="LatticeSubject.Anonymous"/>), but whom a transport
/// binding answers differently: a refused credential is unauthenticated, while an
/// anonymous caller may still be served whatever anonymous access allows.
/// </summary>
public enum LatticeCredentialVerdict
{
    /// <summary>No credential was presented.</summary>
    Absent = 0,

    /// <summary>A registered authenticator recognised the credential and accepted it.</summary>
    Accepted = 1,

    /// <summary>
    /// A registered authenticator recognised the credential and refused it (for
    /// example a wrong password, or an invalid or expired token).
    /// </summary>
    Refused = 2,

    /// <summary>
    /// A credential was presented, but nothing could judge it: no registered
    /// authenticator recognises its scheme, or no membership context is
    /// registered. The caller resolves to <see cref="LatticeSubject.Anonymous"/>,
    /// exactly as it did before the verdict existed.
    /// </summary>
    Unverified = 3,
}
