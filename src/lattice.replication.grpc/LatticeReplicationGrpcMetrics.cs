using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the
/// <c>Orleans.Lattice.Replication.Grpc</c> transports. Published on a single
/// <see cref="Meter"/> named <see cref="MeterName"/> so an OpenTelemetry pipeline
/// can subscribe once and receive every gRPC-transport instrument.
/// </summary>
internal static class LatticeReplicationGrpcMetrics
{
    /// <summary>
    /// The meter name for all <c>Orleans.Lattice.Replication.Grpc</c> transport
    /// telemetry. External subscribers reference this literal in their
    /// <c>MeterProviderBuilder.AddMeter(...)</c> call.
    /// </summary>
    public const string MeterName = "orleans.lattice.replication.grpc";

    /// <summary>Tag key for the remote peer cluster id.</summary>
    public const string TagPeer = "peer";

    /// <summary>
    /// Tag key for the transport that opened the channel (<c>push</c>,
    /// <c>saga_control</c>, or <c>snapshot</c>).
    /// </summary>
    public const string TagTransport = "transport";

    /// <summary>
    /// Counter name for insecure (plaintext) channel constructions.
    /// </summary>
    public const string InsecureChannelName = "orleans.lattice.replication.grpc.insecure_channel";

    /// <summary>
    /// The meter that owns every gRPC-transport instrument. Exposed to the test
    /// assembly (via <c>InternalsVisibleTo</c>) so the dashboard-coverage drift
    /// guard can enumerate its instruments.
    /// </summary>
    internal static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> InsecureChannel = Meter.CreateCounter<long>(
        InsecureChannelName,
        unit: "{channel}",
        description:
            "Number of gRPC replication channels constructed against a plaintext endpoint because AllowPlaintextEndpoints is enabled. A non-zero value means the cross-cluster shared secret is travelling unencrypted and is intended for local/dev only.");

    /// <summary>
    /// Records that a channel to <paramref name="peerClusterId"/> was constructed
    /// over an insecure plaintext endpoint by the named <paramref name="transport"/>.
    /// </summary>
    public static void RecordInsecureChannel(string peerClusterId, string transport)
    {
        InsecureChannel.Add(
            1,
            new KeyValuePair<string, object?>(TagPeer, peerClusterId),
            new KeyValuePair<string, object?>(TagTransport, transport));
    }
}
