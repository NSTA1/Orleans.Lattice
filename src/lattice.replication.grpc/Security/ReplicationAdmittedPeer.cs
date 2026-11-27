using System.Collections.Frozen;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// The admitted-peer seam of the replication gRPC host: the one place that says how
/// every bound method admits a remote cluster, and the checks a method calls to bind
/// what its body declares about its caller to what the transport authenticated.
/// </summary>
/// <remarks>
/// <para>
/// Every method bound on the host is registered here with its
/// <see cref="ReplicationPeerAdmission"/>. <see cref="LatticeReplicationGrpcAuthInterceptor"/>
/// refuses a call to any method of the replication services that is not registered,
/// before it consults a credential, so a new method or service on the host cannot
/// ship without the gate: it is unreachable until it is registered, and registering
/// it means naming how it admits its caller.
/// </para>
/// <para>
/// An origin-bound method calls <see cref="EnsureDeclaredOriginIsCaller"/>; a
/// control method takes its caller from <see cref="CredentialBoundOrigin"/>, binds
/// every body-declared identity to it, and requires it to be an authorized peer of the
/// current topology (<see cref="ISagaPeerAuthorizer"/>). A new cross-cluster control
/// service calls these rather than re-implementing them.
/// </para>
/// </remarks>
internal static class ReplicationAdmittedPeer
{
    private static readonly FrozenDictionary<string, ReplicationPeerAdmission> Admissions =
        new Dictionary<string, ReplicationPeerAdmission>(StringComparer.Ordinal)
        {
            [Name(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.PushMethodName)] = ReplicationPeerAdmission.OriginBound,
            [Name(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.ExchangeContentManifestMethodName)] = ReplicationPeerAdmission.OriginBound,
            [Name(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.GetPeerHighWaterMarkMethodName)] = ReplicationPeerAdmission.OriginBound,
            [Name(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.ProbeDigestMethodName)] = ReplicationPeerAdmission.PeerRead,
            [Name(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.ProbeMerkleWalkMethodName)] = ReplicationPeerAdmission.PeerRead,
            [Name(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.PullCompressionDictionaryMethodName)] = ReplicationPeerAdmission.PeerRead,
            [Name(LatticeRemoteSnapshotGrpcMethods.ServiceName, LatticeRemoteSnapshotGrpcMethods.GetMetadataMethodName)] = ReplicationPeerAdmission.PeerRead,
            [Name(LatticeRemoteSnapshotGrpcMethods.ServiceName, LatticeRemoteSnapshotGrpcMethods.RequestSnapshotMethodName)] = ReplicationPeerAdmission.PeerRead,
            [Name(LatticeSagaGrpcMethods.ServiceName, LatticeSagaGrpcMethods.PrepareMethodName)] = ReplicationPeerAdmission.ControlPeer,
            [Name(LatticeSagaGrpcMethods.ServiceName, LatticeSagaGrpcMethods.CommitMethodName)] = ReplicationPeerAdmission.ControlPeer,
            [Name(LatticeSagaGrpcMethods.ServiceName, LatticeSagaGrpcMethods.AbortMethodName)] = ReplicationPeerAdmission.ControlPeer,
            [Name(LatticeSagaGrpcMethods.ServiceName, LatticeSagaGrpcMethods.GetStatusMethodName)] = ReplicationPeerAdmission.ControlPeer,
            [Name(LatticeSagaGrpcMethods.ServiceName, LatticeSagaGrpcMethods.GetDecisionMethodName)] = ReplicationPeerAdmission.ControlPeer,
        }.ToFrozenDictionary(StringComparer.Ordinal);

    /// <summary>The canonical <c>/{service}/{method}</c> names of every registered method.</summary>
    internal static IEnumerable<string> RegisteredMethods => Admissions.Keys;

    /// <summary>
    /// Reports how the method named <paramref name="fullMethodName"/> admits its caller.
    /// </summary>
    /// <param name="fullMethodName">The canonical <c>/{service}/{method}</c> name.</param>
    /// <param name="admission">The method's admission class, when registered.</param>
    /// <returns><see langword="true"/> when the method is registered with the seam.</returns>
    public static bool TryClassify(string fullMethodName, out ReplicationPeerAdmission admission) =>
        Admissions.TryGetValue(fullMethodName, out admission);

    /// <summary>
    /// Binds a body-declared origin cluster id to the origin the transport stamped on
    /// the call, and refuses the call when the stamped origin is absent or the two
    /// disagree. With <c>BindCredentialToOriginCluster</c> on the interceptor has already
    /// bound the stamped origin to the presented credential, so the body-declared origin
    /// is then an authenticated fact; with binding off this check is the only one the
    /// header meets. The header is required: an absent value refuses the call, because
    /// omitting a header is strictly easier than forging one.
    /// </summary>
    /// <param name="context">The server call context carrying the request headers.</param>
    /// <param name="declaredOrigin">The origin cluster id the request body declares.</param>
    /// <param name="rpc">The RPC name, for the refusal diagnostic.</param>
    /// <param name="logger">The calling service's logger.</param>
    /// <exception cref="RpcException">
    /// <see cref="StatusCode.PermissionDenied"/> when the stamped origin is absent, or
    /// names a different cluster than the body declares.
    /// </exception>
    public static void EnsureDeclaredOriginIsCaller(ServerCallContext context, string declaredOrigin, string rpc, ILogger logger)
    {
        var stamped = GrpcRequestHeaders.Read(context, LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader);
        if (string.IsNullOrWhiteSpace(stamped))
        {
            // Cold reject path only.
            logger.LogWarning(
                "Refusing replication {Rpc}: the request declares origin '{DeclaredOrigin}' but the call "
                + "carries no transport-stamped origin. A peer may only advance its own stream.",
                rpc,
                declaredOrigin);

            throw new RpcException(new Status(StatusCode.PermissionDenied,
                $"The {rpc} call carries no stamped origin cluster; the origin declared in the request "
                + "body is not an authorization input. A peer may only act on its own origin."));
        }

        if (string.Equals(stamped, declaredOrigin, StringComparison.Ordinal))
        {
            return;
        }

        // Cold reject path only.
        logger.LogWarning(
            "Refusing replication {Rpc}: the request declares origin '{DeclaredOrigin}' but the transport "
            + "stamped origin '{StampedOrigin}'. A peer may only advance its own stream.",
            rpc,
            declaredOrigin,
            stamped);

        throw new RpcException(new Status(StatusCode.PermissionDenied,
            $"The origin declared by {rpc} does not match the origin stamped on the call; "
            + "a peer may only act on its own origin."));
    }

    /// <summary>
    /// Binds every entry of a pushed batch to the batch's declared origin, which
    /// <see cref="EnsureDeclaredOriginIsCaller"/> has already bound to the caller. The
    /// receiver keys its per-origin high-water mark, dedupe and inbound contact on each
    /// entry's own origin, so an entry naming another cluster would advance or suppress
    /// that cluster's stream under this caller's credential. No conforming sender relays:
    /// a shipper ships only entries its own cluster authored, and the anti-entropy
    /// re-replay and bootstrap fallback stamp the shipping cluster's id, so the only
    /// origin the receiver expects from a sender is the sender itself. An entry with any
    /// other origin, including none, refuses the whole batch before anything applies.
    /// </summary>
    /// <param name="entries">The batch's entries.</param>
    /// <param name="declaredOrigin">The batch's declared origin, already bound to the caller.</param>
    /// <param name="rpc">The RPC name, for the refusal diagnostic.</param>
    /// <param name="logger">The calling service's logger.</param>
    /// <exception cref="RpcException">
    /// <see cref="StatusCode.PermissionDenied"/> when an entry names an origin other than
    /// <paramref name="declaredOrigin"/>.
    /// </exception>
    public static void EnsureEntriesFromCaller(IReadOnlyList<WalRecord>? entries, string declaredOrigin, string rpc, ILogger logger)
    {
        if (entries is null)
        {
            return;
        }

        for (var i = 0; i < entries.Count; i++)
        {
            var entryOrigin = entries[i].OriginClusterId;
            if (string.Equals(entryOrigin, declaredOrigin, StringComparison.Ordinal))
            {
                continue;
            }

            // Cold reject path only.
            logger.LogWarning(
                "Refusing replication {Rpc}: an entry names origin '{EntryOrigin}' but the batch is from '{DeclaredOrigin}'. "
                + "A peer may only ship entries it authored.",
                rpc,
                entryOrigin,
                declaredOrigin);

            throw new RpcException(new Status(StatusCode.PermissionDenied,
                $"An entry of the {rpc} batch names an origin other than the batch's; "
                + "a peer may only ship entries it authored."));
        }
    }

    /// <summary>
    /// The transport-stamped origin of the call when the interceptor bound it to the
    /// presented credential, or <see langword="null"/> when the header is absent or was
    /// not credential-bound (binding disabled, or a different value). A control method
    /// refuses a <see langword="null"/> result: a self-asserted origin cannot authorize
    /// an identity-sensitive operation.
    /// </summary>
    /// <param name="context">The server call context.</param>
    /// <returns>The credential-bound origin, or <see langword="null"/>.</returns>
    public static string? CredentialBoundOrigin(ServerCallContext context)
    {
        var origin = GrpcRequestHeaders.Read(context, LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader);
        return !string.IsNullOrWhiteSpace(origin)
            && context.UserState.TryGetValue(LatticeReplicationGrpcAuthInterceptor.AuthenticatedPeerKey, out var authenticatedPeer)
            && authenticatedPeer is string peer
            && string.Equals(origin, peer, StringComparison.Ordinal)
                ? origin
                : null;
    }

    private static string Name(string service, string method) => "/" + service + "/" + method;
}
