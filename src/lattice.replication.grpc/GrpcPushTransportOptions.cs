namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Configuration options for the gRPC streaming push transport. Hosts
/// register the transport via
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpcPushTransport"/>
/// and supply a delegate that populates this object.
/// </summary>
/// <remarks>
/// The transport is sender-side only; the receiver-side route is wired
/// up via
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpcService"/>
/// on the ASP.NET Core endpoint route builder and does not consume
/// these options.
/// </remarks>
public sealed class GrpcPushTransportOptions
{
    /// <summary>
    /// Map of remote cluster id to the gRPC endpoint URI it accepts
    /// push batches at. Each entry produces a long-lived
    /// <c>GrpcChannel</c> that is reused across every
    /// <c>SendAsync</c> call targeting that peer; HTTP/2 multiplexes
    /// concurrent batches on the underlying TCP connection.
    /// <para>
    /// A <see cref="ReplicationBatch"/> whose <c>TargetClusterId</c>
    /// is not present in this map causes <c>SendAsync</c> to throw
    /// <see cref="InvalidOperationException"/> - the host is expected
    /// to populate every peer it intends to ship to before the first
    /// dispatch. The map is read once at the first <c>SendAsync</c>
    /// call per peer; subsequent edits are not observed.
    /// </para>
    /// </summary>
    public IDictionary<string, Uri> PeerEndpoints { get; } = new Dictionary<string, Uri>(StringComparer.Ordinal);

    /// <summary>
    /// Optional callback invoked when the transport constructs a
    /// <c>GrpcChannel</c> for a peer. Lets the host attach custom
    /// <c>HttpHandler</c>s, mTLS credentials, retry policies, and
    /// keep-alive options without the transport having to surface a
    /// pass-through option per setting. The default (null) leaves the
    /// channel options at <c>Grpc.Net.Client</c> defaults, which is
    /// sufficient for plaintext-loopback test scenarios.
    /// </summary>
    public Action<string, global::Grpc.Net.Client.GrpcChannelOptions>? ConfigureChannel { get; set; }

    /// <summary>
    /// When <see langword="false"/> (the default), the transport
    /// refuses to construct a channel for any peer whose endpoint URI
    /// is not <c>https://</c>. Replication batches carry secret-shaped
    /// CRDT mutations and must travel over TLS; the gate fails closed
    /// during startup-style misconfiguration. Set to <see langword="true"/>
    /// only for loopback / diagnostic scenarios where the receiver
    /// runs inside the same trust domain and the host has explicitly
    /// signed off on plaintext.
    /// </summary>
    public bool AllowPlaintextEndpoints { get; set; }

    /// <summary>
    /// The local cluster id stamped on every outbound batch as the
    /// <c>x-lattice-replication-origin</c> metadata header. The
    /// receiver-side interceptor uses this to choose the correct
    /// per-peer secret when the host's
    /// <see cref="ILatticeReplicationSecretSource"/> partitions
    /// secrets per origin. When unset, the transport reads
    /// <see cref="LatticeReplicationOptions.ClusterId"/> from
    /// <c>IOptionsMonitor&lt;LatticeReplicationOptions&gt;</c> at
    /// channel-construction time.
    /// </summary>
    public string? LocalClusterId { get; set; }
}
