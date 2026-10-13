using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grpc.Tests;

public partial class LatticeSagaGrpcServiceTests
{
    [TestCase("Prepare")]
    [TestCase("Commit")]
    [TestCase("Abort")]
    [TestCase("GetStatus")]
    [TestCase("GetDecision")]
    public async Task Saga_control_without_credential_binding_denies_claimed_origin(string operation)
    {
        var handler = Substitute.For<ILatticeSagaControlHandler>();
        var service = CreateService(handler, AllowAuthorizer());
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.IsAcceptedAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(true));
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationSecurityOptions { BindCredentialToOriginCluster = false });
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(secrets, options,
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);
        var headers = new global::Grpc.Core.Metadata
        {
            { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, Peer },
            { LatticeReplicationGrpcMetadataNames.SecretHeader, "some-accepted-secret" },
        };
        UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox> method = operation switch
        {
            "Prepare" => service.Prepare,
            "Commit" => service.Commit,
            "Abort" => service.Abort,
            "GetStatus" => service.GetStatus,
            _ => service.GetDecision,
        };
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await interceptor.UnaryServerHandler(Request(),
                new FakeServerCallContext(headers, "/" + LatticeSagaGrpcMethods.ServiceName + "/" + operation), method));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        await handler.DidNotReceiveWithAnyArgs().PrepareAsync(default, default);
        await handler.DidNotReceiveWithAnyArgs().CommitAsync(default, default);
        await handler.DidNotReceiveWithAnyArgs().AbortAsync(default, default);
        await handler.DidNotReceiveWithAnyArgs().GetStatusAsync(default, default);
        await handler.DidNotReceiveWithAnyArgs().GetDecisionAsync(default, default);
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Authenticated_peer_B_cannot_terminate_saga_prepared_by_peer_A(bool commit)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-participant", Saga));
        var persistent = Substitute.For<IPersistentState<CrossClusterSagaParticipantState>>();
        var state = new CrossClusterSagaParticipantState();
        persistent.State.Returns(state);
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.CurrentValue.Returns(new LatticeOptions());
        var participant = Substitute.For<ISagaParticipant>();
        participant.PrepareAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SagaParticipantPrepareResult { Vote = SagaVote.Commit }));
        var grain = new CrossClusterSagaParticipantGrain(context, [participant],
            Substitute.For<IReminderRegistry>(), options, NullLogger<CrossClusterSagaParticipantGrain>.Instance, persistent);
        var handler = Substitute.For<ILatticeSagaControlHandler>();
        handler.PrepareAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(c => grain.PrepareAsync(c.Arg<SagaControlRequest>()));
        handler.CommitAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(c => grain.CommitAsync(c.Arg<SagaControlRequest>()));
        handler.AbortAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(c => grain.AbortAsync(c.Arg<SagaControlRequest>()));
        var service = CreateService(handler, AllowAuthorizer());
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.IsAcceptedAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(true));
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(c => ValueTask.FromResult<string?>(c.Arg<string>() + "-credential"));
        var security = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        security.CurrentValue.Returns(new LatticeReplicationSecurityOptions());
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(secrets, security,
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

        Task<SagaControlResponseBox> Call(string peer, UnaryServerMethod<SagaControlRequestBox, SagaControlResponseBox> method)
        {
            var headers = new global::Grpc.Core.Metadata
            {
                { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, peer },
                { LatticeReplicationGrpcMetadataNames.SecretHeader, peer + "-credential" },
            };
            return interceptor.UnaryServerHandler(Request(peer),
                new FakeServerCallContext(headers, "/" + LatticeSagaGrpcMethods.ServiceName + "/Prepare"), method);
        }

        await Call("peer-a", service.Prepare);
        participant.ClearReceivedCalls();
        persistent.ClearReceivedCalls();
        var deadline = state.FenceDeadlineTicks;

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            if (commit) await Call("peer-b", service.Commit);
            else await Call("peer-b", service.Abort);
        });

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(state.Phase, Is.EqualTo(SagaPhase.Prepared));
        Assert.That(state.FenceDeadlineTicks, Is.EqualTo(deadline));
        await participant.DidNotReceiveWithAnyArgs().CommitAsync(default, default);
        await participant.DidNotReceiveWithAnyArgs().AbortAsync(default, default);
        await persistent.DidNotReceive().WriteStateAsync();
        if (commit) await Call("peer-a", service.Commit);
        else await Call("peer-a", service.Abort);
        Assert.That(state.Phase, Is.EqualTo(commit ? SagaPhase.Committed : SagaPhase.Aborted));
    }

    [TestCase(true)]
    [TestCase(false)]
    public void Terminal_identity_refusal_is_permission_denied(bool commit)
    {
        var handler = Substitute.For<ILatticeSagaControlHandler>();
        handler.CommitAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<SagaControlResponse>(new UnauthorizedAccessException("Identity mismatch.")));
        handler.AbortAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<SagaControlResponse>(new UnauthorizedAccessException("Identity mismatch.")));
        var service = CreateService(handler, AllowAuthorizer());

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            if (commit) await service.Commit(Request(), ContextWithOrigin(Peer));
            else await service.Abort(Request(), ContextWithOrigin(Peer));
        });

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task Prepare_without_body_coordinator_records_the_transport_origin()
    {
        var handler = Substitute.For<ILatticeSagaControlHandler>();
        var service = CreateService(handler, AllowAuthorizer());

        await service.Prepare(Request(coordinator: ""), ContextWithOrigin(Peer));

        await handler.Received(1).PrepareAsync(
            Arg.Is<SagaControlRequest>(r => r.CoordinatorClusterId == Peer), Arg.Any<CancellationToken>());
    }
}
