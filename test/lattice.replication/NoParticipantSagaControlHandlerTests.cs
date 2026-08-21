using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Covers <see cref="NoParticipantSagaControlHandler"/>, the fail-safe default
/// control handler that votes <see cref="SagaVote.Abort"/> on prepare and holds
/// no participant state.
/// </summary>
[TestFixture]
public class NoParticipantSagaControlHandlerTests
{
    private static SagaControlRequest Request(string sagaId = "saga-1")
        => new()
        {
            SagaId = sagaId,
            TargetTree = "tree-1",
            ManifestId = "manifest-1",
            CoordinatorClusterId = "site-a",
        };

    [Test]
    public async Task PrepareAsync_votes_abort_and_reports_no_phase()
    {
        var handler = new NoParticipantSagaControlHandler();

        var response = await handler.PrepareAsync(Request());

        Assert.That(response.SagaId, Is.EqualTo("saga-1"));
        Assert.That(response.Phase, Is.EqualTo(SagaPhase.None));
        Assert.That(response.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(response.Detail, Is.Not.Empty);
    }

    [Test]
    public async Task CommitAsync_reports_no_phase_and_no_vote()
    {
        var handler = new NoParticipantSagaControlHandler();

        var response = await handler.CommitAsync(Request("saga-c"));

        Assert.That(response.SagaId, Is.EqualTo("saga-c"));
        Assert.That(response.Phase, Is.EqualTo(SagaPhase.None));
        Assert.That(response.Vote, Is.EqualTo(SagaVote.None));
    }

    [Test]
    public async Task AbortAsync_reports_no_phase_and_no_vote()
    {
        var handler = new NoParticipantSagaControlHandler();

        var response = await handler.AbortAsync(Request("saga-a"));

        Assert.That(response.SagaId, Is.EqualTo("saga-a"));
        Assert.That(response.Phase, Is.EqualTo(SagaPhase.None));
        Assert.That(response.Vote, Is.EqualTo(SagaVote.None));
    }

    [Test]
    public async Task GetStatusAsync_reports_no_phase_and_no_vote()
    {
        var handler = new NoParticipantSagaControlHandler();

        var response = await handler.GetStatusAsync(Request("saga-s"));

        Assert.That(response.SagaId, Is.EqualTo("saga-s"));
        Assert.That(response.Phase, Is.EqualTo(SagaPhase.None));
        Assert.That(response.Vote, Is.EqualTo(SagaVote.None));
    }

    [Test]
    public void PrepareAsync_honours_cancellation()
    {
        var handler = new NoParticipantSagaControlHandler();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await handler.PrepareAsync(Request(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void CommitAsync_honours_cancellation()
    {
        var handler = new NoParticipantSagaControlHandler();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await handler.CommitAsync(Request(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void AbortAsync_honours_cancellation()
    {
        var handler = new NoParticipantSagaControlHandler();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await handler.AbortAsync(Request(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void GetStatusAsync_honours_cancellation()
    {
        var handler = new NoParticipantSagaControlHandler();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await handler.GetStatusAsync(Request(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
