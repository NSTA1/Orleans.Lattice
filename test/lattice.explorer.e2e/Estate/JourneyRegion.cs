namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>One silo of a region: its process and its ports.</summary>
public sealed class JourneySiloHost
{
    internal JourneySiloHost(int index, JourneyProcess process, int httpPort, int grpcPort, int siloPort, int gatewayPort)
    {
        Index = index;
        Process = process;
        HttpPort = httpPort;
        GrpcPort = grpcPort;
        SiloPort = siloPort;
        GatewayPort = gatewayPort;
    }

    /// <summary>The silo's index in its region, from 0.</summary>
    public int Index { get; }

    /// <summary>The internal HTTP/1 port: <c>/health</c>, <c>/metrics</c> and the journey seams.</summary>
    public int HttpPort { get; }

    /// <summary>The gRPC (HTTPS) port the gateway balances across.</summary>
    public int GrpcPort { get; }

    /// <summary>The Orleans silo-to-silo port, by which an activation's host is reported.</summary>
    public int SiloPort { get; }

    /// <summary>The Orleans client gateway port.</summary>
    public int GatewayPort { get; }

    /// <summary>Whether the silo's process is running.</summary>
    public bool IsRunning => Process.IsRunning;

    /// <summary>Where the silo's output is captured.</summary>
    public string LogFile => Process.LogFile;

    internal JourneyProcess Process { get; }
}

/// <summary>
/// One region of an estate: its silos, the gateway in front of them - the region's single
/// gRPC endpoint - and its Explorer heads.
/// </summary>
public sealed class JourneyRegion
{
    internal JourneyRegion(
        string name,
        IReadOnlyList<JourneySiloHost> silos,
        JourneyProcess gateway,
        int gatewayPort,
        int gatewayControlPort,
        int? gatewayPlaintextPort,
        IReadOnlyList<JourneyProcess> explorers,
        IReadOnlyList<Uri> explorerUris)
    {
        Name = name;
        Silos = silos;
        GatewayProcess = gateway;
        GatewayAddress = new Uri($"https://127.0.0.1:{gatewayPort}/");
        GatewayControl = new Uri($"http://127.0.0.1:{gatewayControlPort}/");
        GatewayPlaintextPort = gatewayPlaintextPort;
        ExplorerProcesses = explorers;
        ExplorerUris = explorerUris;
    }

    /// <summary>The region's name: <see cref="JourneyEstate.West"/> or <see cref="JourneyEstate.East"/>.</summary>
    public string Name { get; }

    /// <summary>The region's silos.</summary>
    public IReadOnlyList<JourneySiloHost> Silos { get; }

    /// <summary>The region's gRPC endpoint: the gateway, over TLS from the run-scoped CA.</summary>
    public Uri GatewayAddress { get; }

    /// <summary>The HTTPS base address of each Explorer head, ending in <c>/</c>.</summary>
    public IReadOnlyList<Uri> ExplorerUris { get; }

    /// <summary>The first Explorer head's base address.</summary>
    public Uri ExplorerUri => ExplorerUris[0];

    internal Uri GatewayControl { get; }

    internal int? GatewayPlaintextPort { get; }

    internal JourneyProcess GatewayProcess { get; }

    internal IReadOnlyList<JourneyProcess> ExplorerProcesses { get; }
}
