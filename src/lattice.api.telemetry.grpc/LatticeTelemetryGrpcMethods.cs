using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// telemetry control API. Each method is a unary RPC over an Orleans-serialized,
/// code-first contract. Constructed from DI-resolved serializers so both the
/// public client invoker and the server-side binder wire up identical
/// marshallers.
/// </summary>
/// <remarks>
/// <para>
/// The contract is the transport-agnostic <see cref="ILatticeTelemetry"/> facade
/// surface: the read-only catalogue discovery RPC (<c>GetCatalog</c>), the query
/// evaluation RPC (<c>Query</c>), and the unauthenticated auth-scheme discovery
/// RPC (<c>GetAuthScheme</c>).
/// </para>
/// <para>
/// <b>The query RPC carries the contract's own request type unchanged.</b> There
/// is deliberately no binding-specific query message: reusing
/// <see cref="TelemetryQueryRequest"/> means the wire can only ever carry what
/// the facade contract permits - a query id, bounded parameters, and a
/// <em>requested</em> visibility - and can never grow a query-text field or a
/// tenant assertion the facade would then have to defend against.
/// </para>
/// <para>
/// Contract-versioning policy: fields on the wire messages are additive-only (new
/// <c>[Id(n)]</c>); aliases and field numbers are never renumbered, so a newer
/// response decodes cleanly under an older client, and new RPCs are added without
/// renaming or renumbering the existing ones.
/// </para>
/// </remarks>
internal sealed class LatticeTelemetryGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.telemetry";

    /// <summary>The unary, read-only curated-catalogue discovery RPC method name.</summary>
    public const string GetCatalogMethodName = "GetCatalog";

    /// <summary>The unary, read-only curated-query evaluation RPC method name.</summary>
    public const string QueryMethodName = "Query";

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC method name.</summary>
    public const string GetAuthSchemeMethodName = "GetAuthScheme";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    /// <param name="catalogRequestSerializer">Serializer for the catalogue marker request.</param>
    /// <param name="catalogSerializer">Serializer for the curated catalogue response.</param>
    /// <param name="queryRequestSerializer">Serializer for the query selection request.</param>
    /// <param name="queryResponseSerializer">Serializer for the evaluated query response.</param>
    /// <param name="authSchemeRequestSerializer">Serializer for the auth-scheme probe request.</param>
    /// <param name="authSchemeAdvertisementSerializer">Serializer for the auth-scheme advertisement.</param>
    /// <exception cref="ArgumentNullException">Any serializer is <see langword="null"/>.</exception>
    public LatticeTelemetryGrpcMethods(
        Serializer<TelemetryCatalogRequest> catalogRequestSerializer,
        Serializer<TelemetryQueryCatalog> catalogSerializer,
        Serializer<TelemetryQueryRequest> queryRequestSerializer,
        Serializer<TelemetryQueryResponse> queryResponseSerializer,
        Serializer<AuthSchemeAdvertisementRequest> authSchemeRequestSerializer,
        Serializer<AuthSchemeAdvertisement> authSchemeAdvertisementSerializer)
    {
        ArgumentNullException.ThrowIfNull(catalogRequestSerializer);
        ArgumentNullException.ThrowIfNull(catalogSerializer);
        ArgumentNullException.ThrowIfNull(queryRequestSerializer);
        ArgumentNullException.ThrowIfNull(queryResponseSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeRequestSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeAdvertisementSerializer);

        GetCatalog = new Method<TelemetryCatalogRequest, TelemetryQueryCatalog>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetCatalogMethodName,
            requestMarshaller: LatticeTelemetryGrpcMarshallers.Create(catalogRequestSerializer),
            responseMarshaller: LatticeTelemetryGrpcMarshallers.Create(catalogSerializer));

        Query = new Method<TelemetryQueryRequest, TelemetryQueryResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: QueryMethodName,
            requestMarshaller: LatticeTelemetryGrpcMarshallers.Create(queryRequestSerializer),
            responseMarshaller: LatticeTelemetryGrpcMarshallers.Create(queryResponseSerializer));

        GetAuthScheme = new Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetAuthSchemeMethodName,
            requestMarshaller: LatticeTelemetryGrpcMarshallers.Create(authSchemeRequestSerializer),
            responseMarshaller: LatticeTelemetryGrpcMarshallers.Create(authSchemeAdvertisementSerializer));
    }

    /// <summary>The unary, read-only curated-catalogue discovery RPC.</summary>
    public Method<TelemetryCatalogRequest, TelemetryQueryCatalog> GetCatalog { get; }

    /// <summary>The unary, read-only curated-query evaluation RPC.</summary>
    public Method<TelemetryQueryRequest, TelemetryQueryResponse> Query { get; }

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC.</summary>
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    /// <summary>
    /// Builds the method definitions by resolving each per-message Orleans
    /// serializer out of <paramref name="serializerProvider"/>.
    /// </summary>
    /// <param name="serializerProvider">
    /// A service provider with Orleans serialization registered
    /// (<c>AddSerializer()</c>).
    /// </param>
    /// <returns>The resolved method definitions.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="serializerProvider"/> is <see langword="null"/>.</exception>
    public static LatticeTelemetryGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTelemetryGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<TelemetryCatalogRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TelemetryQueryCatalog>>(),
            serializerProvider.GetRequiredService<Serializer<TelemetryQueryRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TelemetryQueryResponse>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisementRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisement>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeTelemetryGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI dependencies
/// directly). Setting it more than once is allowed: subsequent registrations
/// replace the prior instance, matching the "last-host-wins" semantics
/// integration-test fixtures rely on.
/// </summary>
internal static class LatticeTelemetryGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeTelemetryGrpcMethods? Current { get; set; }
}
