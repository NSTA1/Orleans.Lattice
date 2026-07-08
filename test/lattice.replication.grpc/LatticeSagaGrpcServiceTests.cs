using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for the server-side <see cref="LatticeSagaGrpcService"/>.
/// Validates the request-validation surface, the peer-authorization gate
/// (an unauthorized origin cluster is rejected with
/// <see cref="StatusCode.PermissionDenied"/> before the handler runs),
/// and delegation to the underlying
/// <see cref="ILatticeSagaControlHandler"/>.
/// </summary>
[TestFixture]
public class LatticeSagaGrpcServiceTests
{
    private const string Saga = "saga-1";
    private const string Tree = "tree";
    private const string Peer = "site-a";

    private static LatticeSagaGrpcMethods CreateMethods()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        return new LatticeSagaGrpcMethods(
            sp.GetRequiredService<Serializer<SagaControlRequest>>(),
            sp.GetRequiredService<Serializer<SagaControlResponse>>());
    }

    private static LatticeSagaGrpcService CreateService(
        ILatticeSagaControlHandler handler,
        ISagaPeerAuthorizer authorizer)
        => new(CreateMethods(), handler, authorizer, NullLogger<LatticeSagaGrpcService>.Instance);

    private static SagaControlRequestBox Request(string coordinator = Peer)
        => new()
        {
            Value = new SagaControlRequest
            {
                SagaId = Saga,
                TargetTree = Tree,
                ManifestId = "m1",
                CoordinatorClusterId = coordinator,
            },
        };

    [Test]
    public async Task Prepare_delegates_to_handler_for_authorized_peer()
    {
        var handler = new RecordingHandler(new SagaControlResponse
        {
            SagaId = Saga,
            Phase = SagaPhase.Prepared,
            Vote = SagaVote.Commit,
        });
        var service = CreateService(handler, AllowAuthorizer());

        var response = await service.Prepare(Request(), ContextWithOrigin(Peer));

        Assert.Multiple(() =>
        {
            Assert.That(response.Value.Phase, Is.EqualTo(SagaPhase.Prepared));
            Assert.That(response.Value.Vote, Is.EqualTo(SagaVote.Commit));
            Assert.That(handler.PrepareCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Commit_abort_getStatus_delegate_to_handler_for_authorized_peer()
    {
        var handler = new RecordingHandler(new SagaControlResponse { SagaId = Saga, Phase = SagaPhase.Committed });
        var service = CreateService(handler, AllowAuthorizer());

        await service.Commit(Request(), ContextWithOrigin(Peer));
        await service.Abort(Request(), ContextWithOrigin(Peer));
        await service.GetStatus(Request(), ContextWithOrigin(Peer));

        Assert.Multiple(() =>
        {
            Assert.That(handler.CommitCalls, Is.EqualTo(1));
            Assert.That(handler.AbortCalls, Is.EqualTo(1));
            Assert.That(handler.GetStatusCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public void Prepare_rejects_unauthorized_origin_before_handler_runs()
    {
        var handler = new RecordingHandler(new SagaControlResponse { SagaId = Saga });
        var service = CreateService(handler, DenyAuthorizer());

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.Prepare(Request(), ContextWithOrigin("rogue")));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(handler.PrepareCalls, Is.EqualTo(0));
        });
    }

    [Test]
    public void Commit_rejects_unauthorized_origin_before_handler_runs()
    {
        var handler = new RecordingHandler(new SagaControlResponse { SagaId = Saga });
        var service = CreateService(handler, DenyAuthorizer());

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.Commit(Request(), ContextWithOrigin("rogue")));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(handler.CommitCalls, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Authorization_falls_back_to_coordinator_cluster_id_when_origin_header_absent()
    {
        var handler = new RecordingHandler(new SagaControlResponse { SagaId = Saga, Phase = SagaPhase.Prepared });
        var seen = new List<string?>();
        var authorizer = Substitute.For<ISagaPeerAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Do<string?>(seen.Add), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));
        var service = CreateService(handler, authorizer);

        await service.Prepare(Request(coordinator: Peer), ContextWithoutHeaders());

        Assert.That(seen, Is.EqualTo(new[] { Peer }));
    }

    [Test]
    public void Prepare_throws_invalid_argument_for_empty_saga_id()
    {
        var service = CreateService(new RecordingHandler(new SagaControlResponse()), AllowAuthorizer());

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.Prepare(
                new SagaControlRequestBox { Value = new SagaControlRequest { SagaId = " ", TargetTree = Tree } },
                ContextWithOrigin(Peer)));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void Prepare_throws_invalid_argument_for_empty_target_tree()
    {
        var service = CreateService(new RecordingHandler(new SagaControlResponse()), AllowAuthorizer());

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.Prepare(
                new SagaControlRequestBox { Value = new SagaControlRequest { SagaId = Saga, TargetTree = "  " } },
                ContextWithOrigin(Peer)));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void Constructor_throws_on_null_handler()
    {
        Assert.That(() => new LatticeSagaGrpcService(CreateMethods(), null!, AllowAuthorizer(), NullLogger<LatticeSagaGrpcService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_authorizer()
    {
        Assert.That(() => new LatticeSagaGrpcService(CreateMethods(), new RecordingHandler(new SagaControlResponse()), null!, NullLogger<LatticeSagaGrpcService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_methods()
    {
        Assert.That(() => new LatticeSagaGrpcService(null!, new RecordingHandler(new SagaControlResponse()), AllowAuthorizer(), NullLogger<LatticeSagaGrpcService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_throws_when_binder_null()
    {
        Assert.That(() => LatticeSagaGrpcServiceBase.BindService(null!, serviceImpl: null),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_throws_when_methods_holder_not_initialised()
    {
        var saved = LatticeSagaGrpcMethodsHolder.Current;
        LatticeSagaGrpcMethodsHolder.Current = null;
        try
        {
            Assert.That(
                () => LatticeSagaGrpcServiceBase.BindService(Substitute.For<ServiceBinderBase>(), serviceImpl: null),
                Throws.InvalidOperationException);
        }
        finally
        {
            LatticeSagaGrpcMethodsHolder.Current = saved;
        }
    }

    [Test]
    public void BindService_records_metadata_when_service_impl_null()
    {
        var saved = LatticeSagaGrpcMethodsHolder.Current;
        LatticeSagaGrpcMethodsHolder.Current = CreateMethods();
        try
        {
            Assert.That(
                () => LatticeSagaGrpcServiceBase.BindService(Substitute.For<ServiceBinderBase>(), serviceImpl: null),
                Throws.Nothing);
        }
        finally
        {
            LatticeSagaGrpcMethodsHolder.Current = saved;
        }
    }

    private static ISagaPeerAuthorizer AllowAuthorizer()
    {
        var a = Substitute.For<ISagaPeerAuthorizer>();
        a.IsAuthorizedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        return a;
    }

    private static ISagaPeerAuthorizer DenyAuthorizer()
    {
        var a = Substitute.For<ISagaPeerAuthorizer>();
        a.IsAuthorizedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(false));
        return a;
    }

    private static ServerCallContext ContextWithOrigin(string origin)
    {
        var headers = new global::Grpc.Core.Metadata
        {
            { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, origin },
        };
        return new FakeServerCallContext(headers);
    }

    private static ServerCallContext ContextWithoutHeaders() => new FakeServerCallContext(new global::Grpc.Core.Metadata());

    private sealed class RecordingHandler : ILatticeSagaControlHandler
    {
        private readonly SagaControlResponse _response;
        public int PrepareCalls;
        public int CommitCalls;
        public int AbortCalls;
        public int GetStatusCalls;

        public RecordingHandler(SagaControlResponse response) => _response = response;

        public Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            PrepareCalls++;
            return Task.FromResult(_response);
        }

        public Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            CommitCalls++;
            return Task.FromResult(_response);
        }

        public Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            AbortCalls++;
            return Task.FromResult(_response);
        }

        public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            GetStatusCalls++;
            return Task.FromResult(_response);
        }
    }

    private sealed class FakeServerCallContext : ServerCallContext
    {
        public FakeServerCallContext(global::Grpc.Core.Metadata headers) => RequestHeadersCore = headers;

        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; }
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
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
