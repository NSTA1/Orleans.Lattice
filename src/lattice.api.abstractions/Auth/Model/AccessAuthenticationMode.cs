namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// The authentication posture a silo can <b>authoritatively</b> determine for
/// itself from its own registrations, surfaced by
/// <see cref="ILatticeAuthAdmin.GetAccessModelAsync"/> so a UI can render the
/// right create-form guidance and warn when access is effectively open.
/// </summary>
/// <remarks>
/// This is a <b>best-effort</b> classification of what the in-silo identity
/// registrations reveal, not a claim about every transport in front of the
/// cluster. A transport-specific authorizer (for example the flat-Basic
/// authorizer that lives at the gRPC state layer) is not visible to this facade,
/// so a deployment reporting <see cref="Claims"/> may still terminate a
/// different scheme at its edge. The transport-authorizer refinement is the job
/// of the capability probe layered above this facade.
/// </remarks>
public enum AccessAuthenticationMode
{
    /// <summary>
    /// The authentication posture could not be determined - no credential
    /// authenticator (not even the anonymous fallback) is registered.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Only the anonymous fallback authenticator is registered: every caller
    /// resolves to the anonymous subject, so no caller is ever authenticated.
    /// </summary>
    Anonymous = 1,

    /// <summary>
    /// At least one real credential authenticator (for example a JWT / claims
    /// authenticator) is registered, so the silo can authenticate a caller from
    /// its token claims.
    /// </summary>
    Claims = 2,

    /// <summary>
    /// A flat username / password (Basic) scheme is in force. Never reported by
    /// this facade - the flat-Basic authorizer lives at the transport layer and
    /// is out of this facade's view - but reserved so the transport capability
    /// probe layered above can surface it.
    /// </summary>
    Basic = 3,
}
