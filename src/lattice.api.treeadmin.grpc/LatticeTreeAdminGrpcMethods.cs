using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// tree-administration control API. Each method is a unary RPC over an
/// Orleans-serialized, code-first contract. Constructed from DI-resolved
/// serializers so both the public client invoker and the server-side binder wire
/// up identical marshallers.
/// </summary>
/// <remarks>
/// This foundation contract is a minimal set of RPCs over the transport-agnostic
/// <see cref="ILatticeTreeAdmin"/> facade: the capability probe
/// (<c>ProbeCapabilities</c>) and unauthenticated discovery (<c>GetAuthScheme</c>).
/// The whole-tree lifecycle operations land in later releases, each appending an
/// RPC here. Contract-versioning policy: fields on the wire messages are
/// additive-only (new <c>[Id(n)]</c>); aliases and field numbers are never
/// renumbered, so a newer response decodes cleanly under an older client, and new
/// RPCs are added without renaming or renumbering the existing ones.
/// </remarks>
internal sealed class LatticeTreeAdminGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.treeadmin";

    /// <summary>The unary capability-probe RPC method name.</summary>
    public const string ProbeCapabilitiesMethodName = "ProbeCapabilities";

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC method name.</summary>
    public const string GetAuthSchemeMethodName = "GetAuthScheme";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeTreeAdminGrpcMethods(
        Serializer<TreeAdminTreeRequest> treeRequestSerializer,
        Serializer<LatticeTreeAdminCapabilities> capabilitiesSerializer,
        Serializer<AuthSchemeAdvertisementRequest> authSchemeRequestSerializer,
        Serializer<AuthSchemeAdvertisement> authSchemeAdvertisementSerializer)
    {
        ArgumentNullException.ThrowIfNull(treeRequestSerializer);
        ArgumentNullException.ThrowIfNull(capabilitiesSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeRequestSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeAdvertisementSerializer);

        ProbeCapabilities = new Method<TreeAdminTreeRequest, LatticeTreeAdminCapabilities>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ProbeCapabilitiesMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(capabilitiesSerializer));

        GetAuthScheme = new Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetAuthSchemeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(authSchemeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(authSchemeAdvertisementSerializer));
    }

    /// <summary>The unary <c>ProbeCapabilities</c> capability-probe RPC.</summary>
    public Method<TreeAdminTreeRequest, LatticeTreeAdminCapabilities> ProbeCapabilities { get; }

    /// <summary>The unary, unauthenticated <c>GetAuthScheme</c> advertisement RPC.</summary>
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>. Shared by the server-side DI factory
    /// and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeTreeAdminGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTreeAdminGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<TreeAdminTreeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeTreeAdminCapabilities>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisementRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisement>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeTreeAdminGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI dependencies
/// directly). Setting it more than once is allowed: subsequent registrations
/// replace the prior instance, matching the "last-host-wins" semantics
/// integration-test fixtures rely on.
/// </summary>
internal static class LatticeTreeAdminGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeTreeAdminGrpcMethods? Current { get; set; }
}
