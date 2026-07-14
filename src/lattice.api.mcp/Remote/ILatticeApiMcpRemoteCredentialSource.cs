namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Resolves the caller credential to forward on an outbound gRPC call under the
/// remote-host topology. The single seam the credential-forwarding interceptor
/// reads per call so credential selection is written and tested once, in
/// isolation from the gRPC pipeline.
/// </summary>
internal interface ILatticeApiMcpRemoteCredentialSource
{
    /// <summary>
    /// Returns the credential to stamp on the next outbound gRPC call, or
    /// <see langword="null"/> when the call should be made anonymously (which the
    /// remote cluster fails closed).
    /// </summary>
    LatticeCredential? ResolveOutbound();
}
