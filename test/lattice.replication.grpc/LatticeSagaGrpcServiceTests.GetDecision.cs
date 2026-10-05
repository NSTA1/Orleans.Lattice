using Grpc.Core;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Issue #4637: the <c>GetDecision</c> verb a prepared participant uses to ask
/// the saga's coordinator cluster for its decision. The caller is a participant,
/// not the coordinator, so the body's coordinator cluster names this cluster and
/// is not checked against the caller; the authorization input is still only the
/// transport-stamped origin, which must be an authorized peer and which
/// overwrites the requester the coordinator checks membership against.
/// </summary>
public partial class LatticeSagaGrpcServiceTests
{
    private const string Coordinator = "site-home";

    [Test]
    public async Task GetDecision_stamps_the_authenticated_origin_as_the_requester()
    {
        var handler = new RecordingHandler(new SagaControlResponse { SagaId = Saga, Phase = SagaPhase.Committed });
        var service = CreateService(handler, AllowAuthorizer());
        var request = new SagaControlRequestBox
        {
            Value = Request(coordinator: Coordinator).Value with { RequesterClusterId = "site-forged" },
        };

        var response = await service.GetDecision(request, ContextWithOrigin(Peer));

        Assert.Multiple(() =>
        {
            Assert.That(response.Value.Phase, Is.EqualTo(SagaPhase.Committed));
            Assert.That(handler.GetDecisionCalls, Is.EqualTo(1));
            Assert.That(handler.LastDecisionRequest?.RequesterClusterId, Is.EqualTo(Peer),
                "a body-supplied requester is overwritten with the stamped origin");
        });
    }

    [Test]
    public void GetDecision_refuses_an_unauthorized_origin_before_the_handler_runs()
    {
        var handler = new RecordingHandler(new SagaControlResponse());
        var service = CreateService(handler, DenyAuthorizer());

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.GetDecision(Request(coordinator: Coordinator), ContextWithOrigin(Peer)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(handler.GetDecisionCalls, Is.Zero);
    }

    [Test]
    public void GetDecision_refuses_an_unstamped_call()
    {
        var handler = new RecordingHandler(new SagaControlResponse());
        var service = CreateService(handler, AllowAuthorizer());

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.GetDecision(Request(coordinator: Coordinator), ContextWithoutHeaders()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(handler.GetDecisionCalls, Is.Zero);
    }

    [Test]
    public void A_coordinator_verb_still_refuses_a_body_coordinator_that_is_not_the_caller()
    {
        // The relaxation is the query's alone: a participant cannot use it to
        // drive a saga it does not coordinate through another verb.
        var handler = new RecordingHandler(new SagaControlResponse());
        var service = CreateService(handler, AllowAuthorizer());

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.Commit(Request(coordinator: Coordinator), ContextWithOrigin(Peer)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(handler.CommitCalls, Is.Zero);
    }
}
