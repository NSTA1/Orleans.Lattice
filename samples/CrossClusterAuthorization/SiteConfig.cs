namespace Orleans.Lattice.Samples.CrossClusterAuthorization;

/// <summary>
/// Immutable description of one in-process Orleans cluster ("site") in the
/// cross-cluster authorization topology: its cluster id, its Orleans silo/gateway ports,
/// the local Kestrel port that serves its inbound replication gRPC endpoint, and
/// the single peer it ships the reserved membership/auth system trees to.
/// </summary>
internal sealed record SiteConfig(
    string ClusterId,
    int SiloPort,
    int GatewayPort,
    int GrpcPort,
    string PeerClusterId,
    int PeerGrpcPort);
