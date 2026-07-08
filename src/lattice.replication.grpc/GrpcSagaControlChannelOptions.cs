using Grpc.Net.Client;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Internal per-transport options projected from the unified public
/// <see cref="LatticeReplicationGrpcOptions"/> by
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc"/>.
/// Consumed by <see cref="GrpcSagaControlChannel"/>. Hosts configure the
/// binding via the unified public options type; this projection exists
/// so the transport implementation keeps a focused, single-purpose
/// options surface, mirroring
/// <see cref="GrpcRemoteSnapshotTransportOptions"/>.
/// </summary>
internal sealed class GrpcSagaControlChannelOptions
{
    /// <summary>
    /// Map from participant cluster id to the gRPC endpoint that hosts
    /// that cluster's <c>LatticeSagaGrpcService</c>. The saga channel
    /// looks the caller-supplied <c>clusterId</c> up in this map to find
    /// the channel target; an unknown cluster id raises an
    /// <see cref="InvalidOperationException"/> on the first call.
    /// </summary>
    public IDictionary<string, Uri> PeerEndpoints { get; }
        = new Dictionary<string, Uri>(StringComparer.Ordinal);

    /// <summary>
    /// When <see langword="true"/>, the channel accepts non-<c>https</c>
    /// endpoints (typically <c>http://</c> on loopback for
    /// local-development tests). The default is <see langword="false"/>;
    /// a plaintext saga control call still mutates cluster state and is
    /// gated behind an explicit opt-in matching the live-incremental
    /// <see cref="GrpcPushTransportOptions.AllowPlaintextEndpoints"/>
    /// gate.
    /// </summary>
    public bool AllowPlaintextEndpoints { get; set; }

    /// <summary>
    /// Optional host hook invoked once per peer channel after the
    /// hardened defaults have been applied but before the channel is
    /// constructed.
    /// </summary>
    public Action<string, GrpcChannelOptions>? ConfigureChannel { get; set; }

    /// <summary>
    /// Overrides the local cluster id stamped on the
    /// <c>x-lattice-replication-origin</c> header of every outbound saga
    /// RPC. When <see langword="null"/> or whitespace, the channel falls
    /// back to <see cref="LatticeReplicationOptions.ClusterId"/>.
    /// </summary>
    public string? LocalClusterId { get; set; }
}
