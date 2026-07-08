using Grpc.Core;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Code-first gRPC method holder for the cross-cluster saga control
/// RPCs. Hosts the four unary methods the coordinator-side
/// <see cref="GrpcSagaControlChannel"/> client and the participant-side
/// <see cref="LatticeSagaGrpcService"/> server share:
/// <c>Prepare</c>, <c>Commit</c>, <c>Abort</c>, and <c>GetStatus</c>.
/// All four are unary and share the
/// <see cref="SagaControlRequestBox"/>/<see cref="SagaControlResponseBox"/>
/// marshaller pair.
/// </summary>
/// <remarks>
/// The methods are lazily constructed from the Orleans-serialiser
/// dependencies via <see cref="LatticeSagaGrpcMarshallers"/>. Both the
/// client channel and the server-side service consume the same
/// <see cref="LatticeSagaGrpcMethods"/> singleton so they share a
/// single method instance across every call - a code-first gRPC
/// method's identity is its <c>FullName</c>, so a single registration
/// is enough for any number of channels and bind hooks.
/// </remarks>
internal sealed class LatticeSagaGrpcMethods
{
    /// <summary>
    /// Logical service name carried in the <c>ServiceName</c> slot of
    /// every saga control RPC. The receiver-side auth interceptor scopes
    /// the shared-secret check by this prefix, and the saga service
    /// applies the additional peer-authorization gate.
    /// </summary>
    public const string ServiceName = "orleans.lattice.replication.LatticeSaga";

    /// <summary>The <c>Prepare</c> unary RPC method name.</summary>
    public const string PrepareMethodName = "Prepare";

    /// <summary>The <c>Commit</c> unary RPC method name.</summary>
    public const string CommitMethodName = "Commit";

    /// <summary>The <c>Abort</c> unary RPC method name.</summary>
    public const string AbortMethodName = "Abort";

    /// <summary>The <c>GetStatus</c> unary RPC method name.</summary>
    public const string GetStatusMethodName = "GetStatus";

    private readonly Method<SagaControlRequestBox, SagaControlResponseBox> _prepare;
    private readonly Method<SagaControlRequestBox, SagaControlResponseBox> _commit;
    private readonly Method<SagaControlRequestBox, SagaControlResponseBox> _abort;
    private readonly Method<SagaControlRequestBox, SagaControlResponseBox> _getStatus;

    /// <summary>
    /// Initialises the method holder by composing the request and
    /// response marshallers from the supplied Orleans serialisers.
    /// </summary>
    public LatticeSagaGrpcMethods(
        Serializer<SagaControlRequest> requestSerializer,
        Serializer<SagaControlResponse> responseSerializer)
    {
        ArgumentNullException.ThrowIfNull(requestSerializer);
        ArgumentNullException.ThrowIfNull(responseSerializer);

        var requestMarshaller = LatticeSagaGrpcMarshallers.CreateRequestMarshaller(requestSerializer);
        var responseMarshaller = LatticeSagaGrpcMarshallers.CreateResponseMarshaller(responseSerializer);

        _prepare = Create(PrepareMethodName, requestMarshaller, responseMarshaller);
        _commit = Create(CommitMethodName, requestMarshaller, responseMarshaller);
        _abort = Create(AbortMethodName, requestMarshaller, responseMarshaller);
        _getStatus = Create(GetStatusMethodName, requestMarshaller, responseMarshaller);
    }

    private static Method<SagaControlRequestBox, SagaControlResponseBox> Create(
        string methodName,
        Marshaller<SagaControlRequestBox> requestMarshaller,
        Marshaller<SagaControlResponseBox> responseMarshaller)
        => new(
            MethodType.Unary,
            ServiceName,
            methodName,
            requestMarshaller,
            responseMarshaller);

    /// <summary>The unary <c>Prepare</c> RPC.</summary>
    public Method<SagaControlRequestBox, SagaControlResponseBox> Prepare => _prepare;

    /// <summary>The unary <c>Commit</c> RPC.</summary>
    public Method<SagaControlRequestBox, SagaControlResponseBox> Commit => _commit;

    /// <summary>The unary <c>Abort</c> RPC.</summary>
    public Method<SagaControlRequestBox, SagaControlResponseBox> Abort => _abort;

    /// <summary>The unary <c>GetStatus</c> RPC.</summary>
    public Method<SagaControlRequestBox, SagaControlResponseBox> GetStatus => _getStatus;
}
