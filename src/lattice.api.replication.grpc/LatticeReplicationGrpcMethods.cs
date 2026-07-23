using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// replication control API. Each method is a unary RPC over an Orleans-serialized,
/// code-first contract. Constructed from DI-resolved serializers so both the
/// public client invoker and the server-side binder wire up identical
/// marshallers.
/// </summary>
/// <remarks>
/// The contract is a flat set of RPCs over the transport-agnostic
/// <see cref="Orleans.Lattice.Api.Replication.ILatticeReplicationControl"/>
/// facade: <c>EnableReplication</c>, <c>DisableReplication</c>,
/// <c>GetReplicationConfig</c>, and the unauthenticated discovery RPC
/// <c>GetAuthScheme</c>. Contract-versioning policy: fields on the wire messages
/// are additive-only (new <c>[Id(n)]</c>); aliases and field numbers are never
/// renumbered, so a newer response decodes cleanly under an older client.
/// </remarks>
internal sealed class LatticeReplicationGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.replication";

    /// <summary>The unary enable-replication RPC method name.</summary>
    public const string EnableReplicationMethodName = "EnableReplication";

    /// <summary>The unary disable-replication RPC method name.</summary>
    public const string DisableReplicationMethodName = "DisableReplication";

    /// <summary>The unary get-config RPC method name.</summary>
    public const string GetReplicationConfigMethodName = "GetReplicationConfig";

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC method name.</summary>
    public const string GetAuthSchemeMethodName = "GetAuthScheme";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeReplicationGrpcMethods(
        Serializer<ReplicationEnableRequestMessage> enableRequestSerializer,
        Serializer<ReplicationEnableResponse> enableResponseSerializer,
        Serializer<ReplicationDisableRequestMessage> disableRequestSerializer,
        Serializer<ReplicationDisableResponse> disableResponseSerializer,
        Serializer<ReplicationGetConfigRequest> getConfigRequestSerializer,
        Serializer<ReplicationConfigResponse> getConfigResponseSerializer,
        Serializer<AuthSchemeAdvertisementRequest> authSchemeRequestSerializer,
        Serializer<AuthSchemeAdvertisement> authSchemeAdvertisementSerializer)
    {
        ArgumentNullException.ThrowIfNull(enableRequestSerializer);
        ArgumentNullException.ThrowIfNull(enableResponseSerializer);
        ArgumentNullException.ThrowIfNull(disableRequestSerializer);
        ArgumentNullException.ThrowIfNull(disableResponseSerializer);
        ArgumentNullException.ThrowIfNull(getConfigRequestSerializer);
        ArgumentNullException.ThrowIfNull(getConfigResponseSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeRequestSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeAdvertisementSerializer);

        EnableReplication = new Method<ReplicationEnableRequestMessage, ReplicationEnableResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: EnableReplicationMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.Create(enableRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.Create(enableResponseSerializer));

        DisableReplication = new Method<ReplicationDisableRequestMessage, ReplicationDisableResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: DisableReplicationMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.Create(disableRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.Create(disableResponseSerializer));

        GetReplicationConfig = new Method<ReplicationGetConfigRequest, ReplicationConfigResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetReplicationConfigMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.Create(getConfigRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.Create(getConfigResponseSerializer));

        GetAuthScheme = new Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetAuthSchemeMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.Create(authSchemeRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.Create(authSchemeAdvertisementSerializer));
    }

    /// <summary>The unary <c>EnableReplication</c> RPC.</summary>
    public Method<ReplicationEnableRequestMessage, ReplicationEnableResponse> EnableReplication { get; }

    /// <summary>The unary <c>DisableReplication</c> RPC.</summary>
    public Method<ReplicationDisableRequestMessage, ReplicationDisableResponse> DisableReplication { get; }

    /// <summary>The unary <c>GetReplicationConfig</c> RPC.</summary>
    public Method<ReplicationGetConfigRequest, ReplicationConfigResponse> GetReplicationConfig { get; }

    /// <summary>The unary, unauthenticated <c>GetAuthScheme</c> advertisement RPC.</summary>
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out
    /// of <paramref name="serializerProvider"/>. Shared by the server-side DI
    /// factory and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeReplicationGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeReplicationGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<ReplicationEnableRequestMessage>>(),
            serializerProvider.GetRequiredService<Serializer<ReplicationEnableResponse>>(),
            serializerProvider.GetRequiredService<Serializer<ReplicationDisableRequestMessage>>(),
            serializerProvider.GetRequiredService<Serializer<ReplicationDisableResponse>>(),
            serializerProvider.GetRequiredService<Serializer<ReplicationGetConfigRequest>>(),
            serializerProvider.GetRequiredService<Serializer<ReplicationConfigResponse>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisementRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisement>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeReplicationGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI
/// dependencies directly). Setting it more than once is allowed: subsequent
/// registrations replace the prior instance, matching the "last-host-wins"
/// semantics integration-test fixtures rely on.
/// </summary>
internal static class LatticeReplicationGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeReplicationGrpcMethods? Current { get; set; }
}
