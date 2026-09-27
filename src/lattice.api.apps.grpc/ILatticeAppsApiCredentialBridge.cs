using Grpc.Core;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Resolves an opaque inbound credential for the facade's independently enforced identity boundary.</summary>
public interface ILatticeAppsApiCredentialBridge
{
    /// <summary>Returns the call's credential, or null for an anonymous call. Does not itself grant authority.</summary>
    LatticeCredential? Resolve(ServerCallContext context);
}
