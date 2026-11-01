namespace Orleans.Lattice.Samples.Explorer;

/// <summary>The region a sample region replicates with.</summary>
/// <param name="Id">The peer region's id.</param>
/// <param name="GrpcPort">The peer region's h2c port, where its replication receiver listens.</param>
/// <param name="LinkSecret">The replication credential both regions present on their link, generated for each run.</param>
internal sealed record SampleRegionPeer(string Id, int GrpcPort, string LinkSecret)
{
    /// <summary>The peer's replication endpoint.</summary>
    public Uri Endpoint => new($"http://localhost:{GrpcPort}");

    /// <inheritdoc />
    public override string ToString() => $"{nameof(SampleRegionPeer)} {{ Id = {Id}, GrpcPort = {GrpcPort} }}";
}
