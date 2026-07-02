namespace Orleans.Lattice.Samples.CrossClusterReplication;

/// <summary>
/// Immutable description of one in-process Orleans cluster ("site") that
/// participates in cross-cluster replication: its cluster id, its Orleans
/// silo/gateway ports, the local Kestrel port that serves its inbound
/// replication gRPC endpoint, and the single peer it ships to.
/// </summary>
internal sealed record SiteConfig(
    string ClusterId,
    int SiloPort,
    int GatewayPort,
    int GrpcPort,
    string PeerClusterId,
    int PeerGrpcPort);
