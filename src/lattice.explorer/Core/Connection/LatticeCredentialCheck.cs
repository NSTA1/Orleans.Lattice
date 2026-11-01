namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// What an endpoint made of a credential checked with
/// <see cref="ILatticeStateConnection.VerifyCredentialAsync"/>.
/// </summary>
public enum LatticeCredentialCheck
{
    /// <summary>
    /// The endpoint could not judge the credential: it was unreachable, the call
    /// failed for a reason other than authentication, or no check is available.
    /// A sign-in is not refused on this outcome; every later call still carries the
    /// credential to the cluster, which decides each one.
    /// </summary>
    Unverified = 0,

    /// <summary>
    /// The endpoint accepted the credential. The caller may still hold no grants:
    /// a call the endpoint refuses as forbidden is a caller it recognised.
    /// </summary>
    Accepted = 1,

    /// <summary>The endpoint refused the credential as unauthenticated (for example a wrong password).</summary>
    Refused = 2,
}
