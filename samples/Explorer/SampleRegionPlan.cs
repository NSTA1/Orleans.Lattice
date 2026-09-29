namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// What one region of the sample runs: its cluster id and ports, its peer when
/// the estate has two regions, and - for the region that hosts it - where the
/// Explorer console is served and which region it connects to.
/// </summary>
/// <param name="Id">The region id, used as both the Orleans cluster id and the replication cluster id.</param>
/// <param name="GrpcPort">The h2c port serving the region's facades and its replication receiver.</param>
/// <param name="SiloPort">The region's silo-to-silo port.</param>
/// <param name="GatewayPort">The region's client gateway port.</param>
/// <param name="Peer">The peer region's id and gRPC port, or <see langword="null"/> for a single-region run.</param>
/// <param name="Console">Where this region serves the Explorer console, or <see langword="null"/> when it serves none.</param>
internal sealed record SampleRegionPlan(
    string Id,
    int GrpcPort,
    int SiloPort,
    int GatewayPort,
    SampleRegionPeer? Peer,
    SampleConsolePlan? Console)
{
    /// <summary>The region's gRPC endpoint.</summary>
    public Uri GrpcEndpoint => new($"http://localhost:{GrpcPort}");

    /// <summary>Whether the region is one of two: tenancy, runtime replication control and the shared sink are on.</summary>
    public bool IsEstate => Peer is not null;
}
