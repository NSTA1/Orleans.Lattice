using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Sender-side defaults applied to every
/// <see cref="GrpcChannel"/> the
/// <see cref="GrpcPushTransport"/> constructs before the host's
/// <see cref="GrpcPushTransportOptions.ConfigureChannel"/> callback
/// gets to mutate it. The host's callback runs after these defaults
/// so it can override any of them, but the defaults make sure the
/// out-of-the-box experience is hardened.
/// </summary>
/// <remarks>
/// The two pieces of hardening:
/// <list type="bullet">
///   <item>
///     A scheme gate that rejects non-<c>https</c> endpoints unless
///     the host explicitly sets
///     <see cref="GrpcPushTransportOptions.AllowPlaintextEndpoints"/>
///     to <see langword="true"/>. The check runs against the endpoint
///     URI before <see cref="GrpcChannel.ForAddress(Uri, GrpcChannelOptions)"/>
///     is called.
///   </item>
///   <item>
///     <see cref="CallCredentials"/> that inject the
///     <c>x-lattice-replication-secret</c> metadata header from the
///     resolved <see cref="IReplicationSecretProvider"/>. When the
///     channel is HTTPS the credentials are attached as
///     <see cref="ChannelCredentials.Create(ChannelCredentials, CallCredentials)"/>
///     so the gRPC client uses TLS plus call-credentials together;
///     when the channel is plaintext (only possible with the
///     opt-in) the credentials are attached via
///     <see cref="GrpcChannelOptions.UnsafeUseInsecureChannelCallCredentials"/>
///     so the secret still travels with the call.
///   </item>
/// </list>
/// </remarks>
internal static class GrpcChannelHardening
{
    /// <summary>
    /// Validates the endpoint scheme and, when not the opt-in
    /// plaintext path, requires <c>https://</c>. Throws
    /// <see cref="InvalidOperationException"/> with operator guidance
    /// on a mismatch.
    /// </summary>
    public static void EnforceSchemeGate(Uri endpoint, bool allowPlaintext, string peerClusterId)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(peerClusterId);

        if (allowPlaintext)
        {
            return;
        }

        if (!string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"GrpcPushTransport refuses to ship to peer '{peerClusterId}' over a non-https endpoint ('{endpoint}'). "
                + "Replication batches carry secret-shaped CRDT mutations and must travel over TLS. "
                + $"Configure an https://... endpoint, or - only for loopback / diagnostic scenarios - set "
                + $"{nameof(GrpcPushTransportOptions)}.{nameof(GrpcPushTransportOptions.AllowPlaintextEndpoints)} = true.");
        }
    }

    /// <summary>
    /// Attaches the shared-secret <see cref="CallCredentials"/> to
    /// <paramref name="channelOptions"/>, selecting the TLS-composite
    /// credentials for an <c>https</c> endpoint and the insecure-composite
    /// credentials for the opt-in plaintext path. When the plaintext path is
    /// taken (because <paramref name="allowPlaintextEndpoints"/> is enabled and
    /// the endpoint is not <c>https</c>) this emits a <see cref="LogLevel.Warning"/>
    /// naming the peer cluster and endpoint, and increments the
    /// <see cref="LatticeReplicationGrpcMetrics.InsecureChannelName"/> counter, so
    /// an accidental production misconfiguration is visible rather than silent -
    /// the cross-cluster shared secret then travels unencrypted.
    /// </summary>
    /// <param name="channelOptions">The channel options being built; mutated in place.</param>
    /// <param name="endpoint">The resolved peer endpoint URI.</param>
    /// <param name="allowPlaintextEndpoints">Whether the host opted in to plaintext endpoints.</param>
    /// <param name="secrets">The shared-secret provider.</param>
    /// <param name="peerClusterId">The remote peer cluster id.</param>
    /// <param name="localClusterId">The local cluster id stamped on the origin header.</param>
    /// <param name="logger">The transport logger the insecure-channel warning is written to.</param>
    /// <param name="transport">The transport name for the metric tag and log field (e.g. <c>push</c>).</param>
    public static void ApplyCallCredentials(
        GrpcChannelOptions channelOptions,
        Uri endpoint,
        bool allowPlaintextEndpoints,
        IReplicationSecretProvider secrets,
        string peerClusterId,
        string localClusterId,
        ILogger logger,
        string transport)
    {
        ArgumentNullException.ThrowIfNull(channelOptions);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(logger);

        var callCreds = BuildCallCredentials(secrets, peerClusterId, localClusterId);
        var insecure = allowPlaintextEndpoints
            && !string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);
        if (insecure)
        {
            channelOptions.UnsafeUseInsecureChannelCallCredentials = true;
            channelOptions.Credentials = ChannelCredentials.Create(ChannelCredentials.Insecure, callCreds);
            LatticeReplicationGrpcMetrics.RecordInsecureChannel(peerClusterId, transport);
            logger.LogWarning(
                "Replication {Transport} to cluster '{PeerClusterId}' is using an INSECURE plaintext channel to '{Endpoint}' because AllowPlaintextEndpoints is enabled; the cross-cluster shared secret is transmitted UNENCRYPTED. Intended for local / dev / loopback only - do not use in production.",
                transport, peerClusterId, endpoint);
        }
        else
        {
            channelOptions.Credentials = ChannelCredentials.Create(ChannelCredentials.SecureSsl, callCreds);
        }
    }

    /// <summary>
    /// Builds the <see cref="CallCredentials"/> that inject the
    /// shared-secret metadata header on every outbound call. The
    /// returned credentials resolve the secret per call (the
    /// underlying provider is itself cache-fronted), so a rotation
    /// completes within
    /// <see cref="LatticeReplicationSecurityOptions.SecretRefreshInterval"/>
    /// without channel teardown.
    /// </summary>
    public static CallCredentials BuildCallCredentials(
        IReplicationSecretProvider secrets,
        string peerClusterId,
        string localClusterId)
    {
        ArgumentNullException.ThrowIfNull(secrets);
        ArgumentNullException.ThrowIfNull(peerClusterId);
        ArgumentNullException.ThrowIfNull(localClusterId);

        return CallCredentials.FromInterceptor((context, metadata) =>
            PopulateMetadataAsync(secrets, peerClusterId, localClusterId, metadata, context.CancellationToken));
    }

    /// <summary>
    /// Populates the supplied gRPC metadata collection with the
    /// shared-secret credential and origin-cluster-id header. Extracted
    /// from <see cref="BuildCallCredentials"/> so the
    /// metadata-population contract can be unit-tested without going
    /// through a live gRPC channel.
    /// </summary>
    internal static async Task PopulateMetadataAsync(
        IReplicationSecretProvider secrets,
        string peerClusterId,
        string localClusterId,
        global::Grpc.Core.Metadata metadata,
        CancellationToken cancellationToken)
    {
        var token = await secrets.GetOutboundSecretAsync(peerClusterId, cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrEmpty(token))
        {
            metadata.Add(LatticeReplicationGrpcMetadataNames.SecretHeader, token);
        }
        metadata.Add(LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, localClusterId);
    }
}
