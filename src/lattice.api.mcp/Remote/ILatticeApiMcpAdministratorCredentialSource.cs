namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Resolves the administrator service credential the remote-host discovery core
/// forwards for its trusted, read-only permission introspection of a caller. This
/// is the single seam <see cref="LatticeApiMcpRemoteCredentialSource"/> consults
/// on a system-origin introspection call, so the administrator credential's
/// lifetime (a static value or a self-refreshing managed-identity token) is owned
/// and tested in one place, independent of the gRPC pipeline.
/// </summary>
internal interface ILatticeApiMcpAdministratorCredentialSource
{
    /// <summary>
    /// Returns the administrator credential to forward on the next system-origin
    /// introspection call, or <see langword="null"/> when no administrator
    /// credential is configured or one could not be acquired (in which case the
    /// remote credential source falls through to the caller credential and the
    /// remote cluster fails closed).
    /// </summary>
    LatticeCredential? Resolve();
}
