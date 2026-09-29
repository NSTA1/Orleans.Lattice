namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// The loopback ports the sample binds: each region's gRPC endpoint and Orleans
/// silo and gateway ports, and the web port the Explorer console is served on.
/// Every port is distinct, so both regions run in one process.
/// </summary>
/// <param name="EastGrpc">The east region's h2c gRPC endpoint (its facades and replication receiver).</param>
/// <param name="EastWeb">The HTTP port the Explorer console is served on.</param>
/// <param name="EastSilo">The east region's silo-to-silo port.</param>
/// <param name="EastGateway">The east region's client gateway port.</param>
/// <param name="WestGrpc">The west region's h2c gRPC endpoint.</param>
/// <param name="WestSilo">The west region's silo-to-silo port.</param>
/// <param name="WestGateway">The west region's client gateway port.</param>
internal sealed record SamplePorts(
    int EastGrpc,
    int EastWeb,
    int EastSilo,
    int EastGateway,
    int WestGrpc,
    int WestSilo,
    int WestGateway)
{
    /// <summary>The ports <c>dotnet run</c> uses.</summary>
    public static SamplePorts Default { get; } = new(
        EastGrpc: 5199,
        EastWeb: 5080,
        EastSilo: 11111,
        EastGateway: 30000,
        WestGrpc: 5198,
        WestSilo: 11112,
        WestGateway: 30001);

    /// <summary>These ports, each shifted by <paramref name="offset"/>.</summary>
    /// <param name="offset">The shift, which must not be negative.</param>
    /// <returns>The shifted ports.</returns>
    public SamplePorts Offset(int offset)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        return new SamplePorts(
            EastGrpc + offset,
            EastWeb + offset,
            EastSilo + offset,
            EastGateway + offset,
            WestGrpc + offset,
            WestSilo + offset,
            WestGateway + offset);
    }
}
