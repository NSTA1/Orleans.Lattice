using Grpc.Core;
using Grpc.Net.Client;

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
