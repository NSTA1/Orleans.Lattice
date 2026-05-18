using Grpc.Net.Client;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Internal per-transport options projected from the unified public
/// <see cref="LatticeReplicationGrpcOptions"/> by
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc"/>.
/// Consumed by <see cref="GrpcRemoteSnapshotTransport"/>. Hosts
/// configure the binding via the unified public options type; this
/// projection exists so the transport implementation keeps a focused,
/// single-purpose options surface.
/// </summary>
internal sealed class GrpcRemoteSnapshotTransportOptions
{
    /// <summary>
    /// Map from sender cluster id to the gRPC endpoint that hosts
    /// that cluster's <c>LatticeRemoteSnapshotGrpcService</c>. The
    /// snapshot transport looks the receiver-supplied
    /// <c>sourceClusterId</c> up in this map to find the channel
    /// target; an unknown cluster id raises an
    /// <see cref="InvalidOperationException"/> on the first call.
    /// </summary>
    public IDictionary<string, Uri> SenderEndpoints { get; }
        = new Dictionary<string, Uri>(StringComparer.Ordinal);

    /// <summary>
    /// When <see langword="true"/>, the transport accepts
    /// non-<c>https</c> endpoints (typically <c>http://</c> on
    /// loopback for local-development tests). The default is
    /// <see langword="false"/>; a plaintext snapshot stream still
    /// carries cluster state and is gated behind an explicit opt-in
    /// matching the live-incremental
    /// <see cref="GrpcPushTransportOptions.AllowPlaintextEndpoints"/>
    /// gate.
    /// </summary>
    public bool AllowPlaintextEndpoints { get; set; }

    /// <summary>
    /// Optional host hook invoked once per peer channel after the
    /// hardened defaults have been applied but before the channel is
    /// constructed. The host can use it to supply a custom
    /// <see cref="ChannelCredentials"/> chain, a service-config retry
    /// policy, or any other channel-level customisation.
    /// </summary>
    public Action<string, GrpcChannelOptions>? ConfigureChannel { get; set; }

    /// <summary>
    /// Overrides the local cluster id stamped on the
    /// <c>x-lattice-replication-origin</c> header of every outbound
    /// snapshot RPC. When <see langword="null"/> or whitespace, the
    /// transport falls back to
    /// <see cref="LatticeReplicationOptions.ClusterId"/>.
    /// </summary>
    public string? LocalClusterId { get; set; }
}