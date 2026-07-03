using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// write-capable data API. Each method is a unary RPC over an Orleans-serialized,
/// code-first contract. Constructed from DI-resolved serializers so both the
/// client invoker and the server-side binder wire up identical marshallers.
/// </summary>
/// <remarks>
/// The contract is a flat set of unary RPCs: point writes (<c>Set</c> /
/// <c>Delete</c>), atomic batches (<c>SetManyAtomic</c> /
/// <c>SetManyAtomicCrossTree</c>), a point read (<c>Get</c>), and a single-page
/// bounded range read (<c>ReadRange</c>). A live streaming scan / change feed is
/// intentionally out of scope for v1. Contract-versioning policy: fields on the
/// wire messages are additive-only (new <c>[Id(n)]</c>); aliases and field
/// numbers are never renumbered, so a newer response decodes cleanly under an
/// older client.
/// </remarks>
internal sealed class LatticeDataApiGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.data";

    /// <summary>The unary point-set RPC method name.</summary>
    public const string SetMethodName = "Set";

    /// <summary>The unary point-delete RPC method name.</summary>
    public const string DeleteMethodName = "Delete";

    /// <summary>The unary single-tree atomic-batch RPC method name.</summary>
    public const string SetManyAtomicMethodName = "SetManyAtomic";

    /// <summary>The unary cross-tree atomic-batch RPC method name.</summary>
    public const string SetManyAtomicCrossTreeMethodName = "SetManyAtomicCrossTree";

    /// <summary>The unary point-get RPC method name.</summary>
    public const string GetMethodName = "Get";

    /// <summary>The unary bounded range-read RPC method name.</summary>
    public const string ReadRangeMethodName = "ReadRange";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeDataApiGrpcMethods(
        Serializer<DataSetRequest> setRequestSerializer,
        Serializer<DataSetResponse> setResponseSerializer,
        Serializer<DataDeleteRequest> deleteRequestSerializer,
        Serializer<DataDeleteResponse> deleteResponseSerializer,
        Serializer<DataAtomicRequest> atomicRequestSerializer,
        Serializer<DataAtomicResponse> atomicResponseSerializer,
        Serializer<DataCrossTreeRequest> crossTreeRequestSerializer,
        Serializer<DataCrossTreeResponse> crossTreeResponseSerializer,
        Serializer<DataGetRequest> getRequestSerializer,
        Serializer<DataReadResult> readResultSerializer,
        Serializer<DataRangeRequest> rangeRequestSerializer,
        Serializer<DataRangePage> rangePageSerializer)
    {
        ArgumentNullException.ThrowIfNull(setRequestSerializer);
        ArgumentNullException.ThrowIfNull(setResponseSerializer);
        ArgumentNullException.ThrowIfNull(deleteRequestSerializer);
        ArgumentNullException.ThrowIfNull(deleteResponseSerializer);
        ArgumentNullException.ThrowIfNull(atomicRequestSerializer);
        ArgumentNullException.ThrowIfNull(atomicResponseSerializer);
        ArgumentNullException.ThrowIfNull(crossTreeRequestSerializer);
        ArgumentNullException.ThrowIfNull(crossTreeResponseSerializer);
        ArgumentNullException.ThrowIfNull(getRequestSerializer);
        ArgumentNullException.ThrowIfNull(readResultSerializer);
        ArgumentNullException.ThrowIfNull(rangeRequestSerializer);
        ArgumentNullException.ThrowIfNull(rangePageSerializer);

        Set = new Method<DataSetRequest, DataSetResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetMethodName,
            requestMarshaller: LatticeDataApiGrpcMarshallers.Create(setRequestSerializer),
            responseMarshaller: LatticeDataApiGrpcMarshallers.Create(setResponseSerializer));

        Delete = new Method<DataDeleteRequest, DataDeleteResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: DeleteMethodName,
            requestMarshaller: LatticeDataApiGrpcMarshallers.Create(deleteRequestSerializer),
            responseMarshaller: LatticeDataApiGrpcMarshallers.Create(deleteResponseSerializer));

        SetManyAtomic = new Method<DataAtomicRequest, DataAtomicResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetManyAtomicMethodName,
            requestMarshaller: LatticeDataApiGrpcMarshallers.Create(atomicRequestSerializer),
            responseMarshaller: LatticeDataApiGrpcMarshallers.Create(atomicResponseSerializer));

        SetManyAtomicCrossTree = new Method<DataCrossTreeRequest, DataCrossTreeResponse>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetManyAtomicCrossTreeMethodName,
            requestMarshaller: LatticeDataApiGrpcMarshallers.Create(crossTreeRequestSerializer),
            responseMarshaller: LatticeDataApiGrpcMarshallers.Create(crossTreeResponseSerializer));

        Get = new Method<DataGetRequest, DataReadResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetMethodName,
            requestMarshaller: LatticeDataApiGrpcMarshallers.Create(getRequestSerializer),
            responseMarshaller: LatticeDataApiGrpcMarshallers.Create(readResultSerializer));

        ReadRange = new Method<DataRangeRequest, DataRangePage>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ReadRangeMethodName,
            requestMarshaller: LatticeDataApiGrpcMarshallers.Create(rangeRequestSerializer),
            responseMarshaller: LatticeDataApiGrpcMarshallers.Create(rangePageSerializer));
    }

    /// <summary>The unary <c>Set</c> point-write RPC.</summary>
    public Method<DataSetRequest, DataSetResponse> Set { get; }

    /// <summary>The unary <c>Delete</c> point-delete RPC.</summary>
    public Method<DataDeleteRequest, DataDeleteResponse> Delete { get; }

    /// <summary>The unary <c>SetManyAtomic</c> single-tree atomic-batch RPC.</summary>
    public Method<DataAtomicRequest, DataAtomicResponse> SetManyAtomic { get; }

    /// <summary>The unary <c>SetManyAtomicCrossTree</c> cross-tree atomic-batch RPC.</summary>
    public Method<DataCrossTreeRequest, DataCrossTreeResponse> SetManyAtomicCrossTree { get; }

    /// <summary>The unary <c>Get</c> point-read RPC.</summary>
    public Method<DataGetRequest, DataReadResult> Get { get; }

    /// <summary>The unary <c>ReadRange</c> bounded range-read RPC.</summary>
    public Method<DataRangeRequest, DataRangePage> ReadRange { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>. Shared by the server-side DI factory
    /// and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeDataApiGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeDataApiGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<DataSetRequest>>(),
            serializerProvider.GetRequiredService<Serializer<DataSetResponse>>(),
            serializerProvider.GetRequiredService<Serializer<DataDeleteRequest>>(),
            serializerProvider.GetRequiredService<Serializer<DataDeleteResponse>>(),
            serializerProvider.GetRequiredService<Serializer<DataAtomicRequest>>(),
            serializerProvider.GetRequiredService<Serializer<DataAtomicResponse>>(),
            serializerProvider.GetRequiredService<Serializer<DataCrossTreeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<DataCrossTreeResponse>>(),
            serializerProvider.GetRequiredService<Serializer<DataGetRequest>>(),
            serializerProvider.GetRequiredService<Serializer<DataReadResult>>(),
            serializerProvider.GetRequiredService<Serializer<DataRangeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<DataRangePage>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeDataApiGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI
/// dependencies directly). Setting it more than once is allowed: subsequent
/// registrations replace the prior instance, matching the "last-host-wins"
/// semantics integration-test fixtures rely on.
/// </summary>
internal static class LatticeDataApiGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeDataApiGrpcMethods? Current { get; set; }
}
