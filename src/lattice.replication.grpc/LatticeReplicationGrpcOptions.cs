using Grpc.Net.Client;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Unified configuration for the <c>Orleans.Lattice.Replication.Grpc</c>
/// binding. A single options instance configures both the outbound
/// live-push transport (<see cref="IReplicationTransport"/>) and the
/// outbound snapshot-bootstrap transport
/// (<see cref="IRemoteSnapshotTransport"/>), because a real deployment
/// hosts both gRPC services on one ASP.NET Core process behind one
/// endpoint per peer.
/// </summary>
/// <remarks>
/// <para>
/// Active-active is the default: registering the binding via
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc"/>
/// wires the silo as both a sender (peer receivers can pull live
/// push batches and bootstrap snapshots from it) and a receiver (the
/// silo can dial peer endpoints listed in <see cref="Peers"/> to ship
/// outbound batches and to bootstrap from a peer). Push-only or
/// receiver-only deployments use the same helper - a silo that never
/// expects to bootstrap from a peer simply leaves <see cref="Peers"/>
/// empty, and a silo that never expects peers to dial it omits the
/// endpoint-mapping call.
/// </para>
/// <para>
/// The push and snapshot transports share a per-peer
/// <see cref="GrpcChannel"/> cache: one channel per
/// <c>(sourceClusterId, sender URI)</c> tuple carries both live-push
/// batches and snapshot pulls. The hardened-defaults pipeline (TLS
/// enforced unless <see cref="AllowPlaintextEndpoints"/> opts out, the
/// shared-secret authenticator on every call, the
/// <c>x-lattice-replication-origin</c> header) applies uniformly.
/// </para>
/// </remarks>
public sealed class LatticeReplicationGrpcOptions
{
    /// <summary>
    /// Map of remote cluster id to the gRPC endpoint URI it accepts
    /// both push batches and snapshot pulls at. Each entry produces a
    /// long-lived <see cref="GrpcChannel"/> that is reused across
    /// every outbound call targeting that peer; HTTP/2 multiplexes
    /// concurrent batches and snapshot streams on the underlying TCP
    /// connection.
    /// <para>
    /// A live-push batch whose <c>TargetClusterId</c> is not present
    /// in this map causes the push transport's <c>SendAsync</c> to
    /// throw <see cref="InvalidOperationException"/>; a bootstrap
    /// drain whose <c>sourceClusterId</c> is not present in this map
    /// causes the snapshot transport to throw the same exception on
    /// the first dial. The host is expected to populate every peer it
    /// intends to interact with before the first call. The map is
    /// read once per peer (on the first dial); subsequent edits are
    /// not observed until the silo restarts.
    /// </para>
    /// </summary>
    public IDictionary<string, Uri> Peers { get; }
        = new Dictionary<string, Uri>(StringComparer.Ordinal);

    /// <summary>
    /// When <see langword="false"/> (the default), the binding
    /// refuses to construct a channel for any peer whose endpoint URI
    /// is not <c>https://</c>. Replication batches and snapshot
    /// streams carry cluster state and must travel over TLS; the gate
    /// fails closed during startup-style misconfiguration. Set to
    /// <see langword="true"/> only for loopback / diagnostic scenarios
    /// where the peer runs inside the same trust domain and the host
    /// has explicitly signed off on plaintext.
    /// </summary>
    public bool AllowPlaintextEndpoints { get; set; }

    /// <summary>
    /// Optional callback invoked when the binding constructs a
    /// <see cref="GrpcChannel"/> for a peer. Lets the host attach
    /// custom <c>HttpHandler</c>s, mTLS credentials, retry policies,
    /// and keep-alive options without the binding having to surface a
    /// pass-through option per setting. The callback runs after the
    /// package applies its hardened defaults (call credentials,
    /// secure-channel option), so a host that needs to replace the
    /// credential chain (e.g. mTLS only) can do so unconditionally.
    /// The default (<see langword="null"/>) leaves channel options at
    /// <c>Grpc.Net.Client</c> defaults plus the package's
    /// shared-secret call credentials.
    /// </summary>
    public Action<string, GrpcChannelOptions>? ConfigureChannel { get; set; }

    /// <summary>
    /// The local cluster id stamped on the
    /// <c>x-lattice-replication-origin</c> metadata header of every
    /// outbound call (live-push batch or snapshot RPC). When
    /// <see langword="null"/> or whitespace, the binding reads
    /// <see cref="LatticeReplicationOptions.ClusterId"/> from
    /// <c>IOptionsMonitor&lt;LatticeReplicationOptions&gt;</c> at
    /// channel-construction time.
    /// </summary>
    public string? LocalClusterId { get; set; }
}
