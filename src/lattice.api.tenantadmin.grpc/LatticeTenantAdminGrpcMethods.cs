using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// tenant-administration control API. Each method is a unary RPC over an
/// Orleans-serialized, code-first contract. Constructed from DI-resolved
/// serializers so both the public client invoker and the server-side binder wire
/// up identical marshallers.
/// </summary>
/// <remarks>
/// The contract is the transport-agnostic <see cref="ILatticeTenantAdmin"/> facade
/// surface: the four tenant lifecycle RPCs (<c>CreateTenant</c>,
/// <c>SuspendTenant</c>, <c>ResumeTenant</c>, <c>DeleteTenant</c>) plus the
/// unauthenticated auth-scheme discovery RPC (<c>GetAuthScheme</c>).
/// Contract-versioning policy: fields on the wire messages are additive-only (new
/// <c>[Id(n)]</c>); aliases and field numbers are never renumbered, so a newer
/// response decodes cleanly under an older client, and new RPCs are added without
/// renaming or renumbering the existing ones.
/// </remarks>
internal sealed class LatticeTenantAdminGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.tenantadmin";

    /// <summary>The unary tenant-creation RPC method name.</summary>
    public const string CreateTenantMethodName = "CreateTenant";

    /// <summary>The unary tenant-suspend RPC method name.</summary>
    public const string SuspendTenantMethodName = "SuspendTenant";

    /// <summary>The unary tenant-resume RPC method name.</summary>
    public const string ResumeTenantMethodName = "ResumeTenant";

    /// <summary>The unary tenant-delete (with tree cascade) RPC method name.</summary>
    public const string DeleteTenantMethodName = "DeleteTenant";

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC method name.</summary>
    public const string GetAuthSchemeMethodName = "GetAuthScheme";

    /// <summary>The unary, read-only "current tenant" self-service RPC method name.</summary>
    public const string GetCurrentTenantMethodName = "GetCurrentTenant";

    /// <summary>The unary, read-only "list accessible tenants" self-service RPC method name.</summary>
    public const string ListAccessibleTenantsMethodName = "ListAccessibleTenants";

    /// <summary>The unary, read-only "get tenant status" self-service RPC method name.</summary>
    public const string GetTenantMethodName = "GetTenant";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeTenantAdminGrpcMethods(
        Serializer<TenantAdminTenantRequest> tenantRequestSerializer,
        Serializer<TenantCreationResult> creationResultSerializer,
        Serializer<TenantStatusChangeResult> statusChangeResultSerializer,
        Serializer<TenantDeletionResult> deletionResultSerializer,
        Serializer<AuthSchemeAdvertisementRequest> authSchemeRequestSerializer,
        Serializer<AuthSchemeAdvertisement> authSchemeAdvertisementSerializer,
        Serializer<TenantSelfCurrentRequest> selfCurrentRequestSerializer,
        Serializer<TenantSelfListRequest> selfListRequestSerializer,
        Serializer<TenantDescriptor> tenantDescriptorSerializer,
        Serializer<TenantSelfDescriptorList> selfDescriptorListSerializer,
        Serializer<TenantStatusReport> tenantStatusReportSerializer)
    {
        ArgumentNullException.ThrowIfNull(tenantRequestSerializer);
        ArgumentNullException.ThrowIfNull(creationResultSerializer);
        ArgumentNullException.ThrowIfNull(statusChangeResultSerializer);
        ArgumentNullException.ThrowIfNull(deletionResultSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeRequestSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeAdvertisementSerializer);
        ArgumentNullException.ThrowIfNull(selfCurrentRequestSerializer);
        ArgumentNullException.ThrowIfNull(selfListRequestSerializer);
        ArgumentNullException.ThrowIfNull(tenantDescriptorSerializer);
        ArgumentNullException.ThrowIfNull(selfDescriptorListSerializer);
        ArgumentNullException.ThrowIfNull(tenantStatusReportSerializer);

        CreateTenant = new Method<TenantAdminTenantRequest, TenantCreationResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CreateTenantMethodName,
            requestMarshaller: LatticeTenantAdminGrpcMarshallers.Create(tenantRequestSerializer),
            responseMarshaller: LatticeTenantAdminGrpcMarshallers.Create(creationResultSerializer));

        SuspendTenant = new Method<TenantAdminTenantRequest, TenantStatusChangeResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SuspendTenantMethodName,
            requestMarshaller: LatticeTenantAdminGrpcMarshallers.Create(tenantRequestSerializer),
            responseMarshaller: LatticeTenantAdminGrpcMarshallers.Create(statusChangeResultSerializer));

        ResumeTenant = new Method<TenantAdminTenantRequest, TenantStatusChangeResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ResumeTenantMethodName,
            requestMarshaller: LatticeTenantAdminGrpcMarshallers.Create(tenantRequestSerializer),
            responseMarshaller: LatticeTenantAdminGrpcMarshallers.Create(statusChangeResultSerializer));

        DeleteTenant = new Method<TenantAdminTenantRequest, TenantDeletionResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: DeleteTenantMethodName,
            requestMarshaller: LatticeTenantAdminGrpcMarshallers.Create(tenantRequestSerializer),
            responseMarshaller: LatticeTenantAdminGrpcMarshallers.Create(deletionResultSerializer));

        GetAuthScheme = new Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetAuthSchemeMethodName,
            requestMarshaller: LatticeTenantAdminGrpcMarshallers.Create(authSchemeRequestSerializer),
            responseMarshaller: LatticeTenantAdminGrpcMarshallers.Create(authSchemeAdvertisementSerializer));

        GetCurrentTenant = new Method<TenantSelfCurrentRequest, TenantDescriptor>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetCurrentTenantMethodName,
            requestMarshaller: LatticeTenantAdminGrpcMarshallers.Create(selfCurrentRequestSerializer),
            responseMarshaller: LatticeTenantAdminGrpcMarshallers.Create(tenantDescriptorSerializer));

        ListAccessibleTenants = new Method<TenantSelfListRequest, TenantSelfDescriptorList>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ListAccessibleTenantsMethodName,
            requestMarshaller: LatticeTenantAdminGrpcMarshallers.Create(selfListRequestSerializer),
            responseMarshaller: LatticeTenantAdminGrpcMarshallers.Create(selfDescriptorListSerializer));

        GetTenant = new Method<TenantAdminTenantRequest, TenantStatusReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTenantMethodName,
            requestMarshaller: LatticeTenantAdminGrpcMarshallers.Create(tenantRequestSerializer),
            responseMarshaller: LatticeTenantAdminGrpcMarshallers.Create(tenantStatusReportSerializer));
    }

    /// <summary>The unary tenant-creation RPC.</summary>
    public Method<TenantAdminTenantRequest, TenantCreationResult> CreateTenant { get; }

    /// <summary>The unary tenant-suspend RPC.</summary>
    public Method<TenantAdminTenantRequest, TenantStatusChangeResult> SuspendTenant { get; }

    /// <summary>The unary tenant-resume RPC.</summary>
    public Method<TenantAdminTenantRequest, TenantStatusChangeResult> ResumeTenant { get; }

    /// <summary>The unary tenant-delete (with tree cascade) RPC.</summary>
    public Method<TenantAdminTenantRequest, TenantDeletionResult> DeleteTenant { get; }

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC.</summary>
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    /// <summary>The unary, read-only "current tenant" self-service RPC.</summary>
    public Method<TenantSelfCurrentRequest, TenantDescriptor> GetCurrentTenant { get; }

    /// <summary>The unary, read-only "list accessible tenants" self-service RPC.</summary>
    public Method<TenantSelfListRequest, TenantSelfDescriptorList> ListAccessibleTenants { get; }

    /// <summary>The unary, read-only "get tenant status" self-service RPC.</summary>
    public Method<TenantAdminTenantRequest, TenantStatusReport> GetTenant { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>. Shared by the server-side DI factory
    /// and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeTenantAdminGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTenantAdminGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<TenantAdminTenantRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TenantCreationResult>>(),
            serializerProvider.GetRequiredService<Serializer<TenantStatusChangeResult>>(),
            serializerProvider.GetRequiredService<Serializer<TenantDeletionResult>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisementRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisement>>(),
            serializerProvider.GetRequiredService<Serializer<TenantSelfCurrentRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TenantSelfListRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TenantDescriptor>>(),
            serializerProvider.GetRequiredService<Serializer<TenantSelfDescriptorList>>(),
            serializerProvider.GetRequiredService<Serializer<TenantStatusReport>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeTenantAdminGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI dependencies
/// directly). Setting it more than once is allowed: subsequent registrations
/// replace the prior instance, matching the "last-host-wins" semantics
/// integration-test fixtures rely on.
/// </summary>
internal static class LatticeTenantAdminGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeTenantAdminGrpcMethods? Current { get; set; }
}
