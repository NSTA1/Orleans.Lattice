using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Pins the failure and cancellation arms of
/// <see cref="LatticeSagaGrpcService"/>, and the <c>serviceImpl</c>-bound
/// half of <see cref="LatticeSagaGrpcServiceBase.BindService"/>.
/// <para>
/// The saga control RPCs sit on the two-phase commit path, so how a
/// handler fault is classified matters: a caller-initiated cancellation
/// must stay an <see cref="OperationCanceledException"/>, an
/// <see cref="RpcException"/> the handler raised deliberately must pass
/// through with its own status intact, and only an unexpected fault may be
/// mapped to <see cref="StatusCode.Internal"/>. Collapsing those three into
/// one arm would make a deliberate refusal indistinguishable from a server
/// fault at the coordinator.
/// </para>
/// </summary>
[TestFixture]
public class LatticeSagaGrpcServiceFailureTests
{
    private const string Saga = "saga-1";
    private const string Tree = "tree";
    private const string Peer = "site-a";

    private static LatticeSagaGrpcMethods CreateMethods()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return new LatticeSagaGrpcMethods(
            sp.GetRequiredService<Serializer<SagaControlRequest>>(),
            sp.GetRequiredService<Serializer<SagaControlResponse>>());
    }

    private static ISagaPeerAuthorizer AllowAuthorizer()
    {
        var a = Substitute.For<ISagaPeerAuthorizer>();
        a.IsAuthorizedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        return a;
    }

    /// <summary>
    /// Saga control handler whose every entry point faults with the supplied
    /// exception, so the service's classification arms can be driven without
    /// a live saga coordinator behind them.
    /// </summary>
    private sealed class ThrowingHandler(Exception failure) : ILatticeSagaControlHandler
    {
        public Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => throw failure;

        public Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => throw failure;

        public Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => throw failure;

        public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => throw failure;
    }

    private sealed class EchoHandler(SagaControlResponse response) : ILatticeSagaControlHandler
    {
        public Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(response);

        public Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(response);

        public Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(response);

        public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(response);
    }

    private static LatticeSagaGrpcService CreateService(ILatticeSagaControlHandler handler)
        => new(CreateMethods(), handler, AllowAuthorizer(), NullLogger<LatticeSagaGrpcService>.Instance);

    private static SagaControlRequestBox Request()
        => new()
        {
            Value = new SagaControlRequest
            {
                SagaId = Saga,
                TargetTree = Tree,
                ManifestId = "m1",
                CoordinatorClusterId = Peer,
            },
        };

    [Test]
    public void Prepare_rethrows_cancellation_rather_than_mapping_it_to_internal()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var service = CreateService(new ThrowingHandler(new OperationCanceledException()));

        Assert.That(
            async () => await service.Prepare(Request(), Context(cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void Commit_maps_an_unexpected_handler_fault_to_internal()
    {
        var service = CreateService(new ThrowingHandler(new InvalidOperationException("coordinator unreachable")));

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.Commit(Request(), Context(CancellationToken.None)));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(rpc.Status.Detail, Does.Contain(Saga));
            Assert.That(rpc.Status.Detail, Does.Not.Contain("coordinator unreachable"));
        });
    }

    [Test]
    public void Abort_passes_a_deliberate_rpc_exception_through_with_its_own_status()
    {
        var service = CreateService(new ThrowingHandler(
            new RpcException(new Status(StatusCode.FailedPrecondition, "saga already committed"))));

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.Abort(Request(), Context(CancellationToken.None)));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(rpc.Status.Detail, Is.EqualTo("saga already committed"));
        });
    }

    [Test]
    public void GetStatus_maps_an_unexpected_handler_fault_to_internal()
    {
        var service = CreateService(new ThrowingHandler(new TimeoutException("registry timed out")));

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.GetStatus(Request(), Context(CancellationToken.None)));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.Internal));
    }

    [Test]
    public async Task BindService_binds_all_four_control_rpcs_to_the_supplied_service_instance()
    {
        var saved = LatticeSagaGrpcMethodsHolder.Current;
        LatticeSagaGrpcMethodsHolder.Current = CreateMethods();
        try
        {
            var service = CreateService(new EchoHandler(new SagaControlResponse
            {
                SagaId = Saga,
                Phase = SagaPhase.Prepared,
                Vote = SagaVote.Commit,
            }));
            var binder = new RecordingServiceBinder();

            LatticeSagaGrpcServiceBase.BindService(binder, service);

            Assert.That(binder.UnaryHandlers, Has.Count.EqualTo(4));

            // Every bound delegate must reach this instance, so drive each one
            // and require the stub handler's response back out.
            foreach (var bound in binder.UnaryHandlers)
            {
                var handler = (UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox>)bound;
                var response = await handler(Request(), Context(CancellationToken.None));
                Assert.That(response.Value.Phase, Is.EqualTo(SagaPhase.Prepared));
            }
        }
        finally
        {
            LatticeSagaGrpcMethodsHolder.Current = saved;
        }
    }

    private static ServerCallContext Context(CancellationToken cancellationToken)
    {
        var headers = new global::Grpc.Core.Metadata
        {
            { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, Peer },
        };
        return new FakeServerCallContext(headers, cancellationToken);
    }

    /// <summary>
    /// Minimal <see cref="ServiceBinderBase"/> that records the handler
    /// delegates gRPC's binding callback registers, so a test can invoke them
    /// directly instead of standing up a server.
    /// </summary>
    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public List<Delegate> UnaryHandlers { get; } = [];

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler)
        {
            if (handler is not null)
            {
                UnaryHandlers.Add(handler);
            }
        }
    }

    private sealed class FakeServerCallContext(global::Grpc.Core.Metadata headers, CancellationToken cancellationToken)
        : ServerCallContext
    {
        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = headers;
        protected override CancellationToken CancellationTokenCore => cancellationToken;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }
}
