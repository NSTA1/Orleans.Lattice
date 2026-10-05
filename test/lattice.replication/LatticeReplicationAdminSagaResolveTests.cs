using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4637: <see cref="ILatticeReplicationAdmin.ResolveCrossClusterSagaParticipantAsync"/>
/// is the alarmed operator path for a prepared cross-cluster saga participant
/// whose coordinator is lost. It audits at Warning before dispatching, requires a
/// reason, fails closed without a grain factory, and is refused by an admin that
/// does not implement it. The participant-side rules - the coordinator's answer
/// wins, a request is applied only while it is unreachable - are covered by
/// <c>CrossClusterSagaFenceDecisionTests</c>. The seam-level default members
/// the verb and the decision query rely on are pinned here too.
/// </summary>
[TestFixture]
public class LatticeReplicationAdminSagaResolveTests
{
    private const string Saga = "saga-4637";

    private sealed class RecordingLogger : ILogger<LatticeReplicationAdmin>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }

    private static (LatticeReplicationAdmin Admin, ICrossClusterSagaParticipantGrain Grain, RecordingLogger Log) Create(bool resolved)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var grain = Substitute.For<ICrossClusterSagaParticipantGrain>();
        grain.OperatorResolveAsync(Arg.Any<bool>()).Returns(resolved);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ICrossClusterSagaParticipantGrain>(Saga, null).Returns(grain);
        var log = new RecordingLogger();
        var admin = new LatticeReplicationAdmin(
            Substitute.For<ILatticeBootstrapCoordinator>(), monitor, log, timeProvider: null, grainFactory: factory);
        return (admin, grain, log);
    }

    [Test]
    public async Task A_resolution_is_audited_at_warning_with_its_reason_and_dispatched()
    {
        var (admin, grain, log) = Create(resolved: true);

        var resolved = await admin.ResolveCrossClusterSagaParticipantAsync(Saga, commit: false, "coordinator cluster decommissioned");

        await grain.Received(1).OperatorResolveAsync(false);
        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(log.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("coordinator cluster decommissioned")), Is.EqualTo(1),
                "the request is audited at Warning, with its reason, before dispatch");
            Assert.That(log.Entries.Count(e => e.Level == LogLevel.Warning), Is.EqualTo(2), "the outcome is recorded at Warning too");
        });
    }

    [Test]
    public void A_refused_resolution_is_still_audited()
    {
        var (admin, grain, log) = Create(resolved: true);
        grain.OperatorResolveAsync(true).Returns<Task<bool>>(_ => throw new InvalidOperationException("coordinator decided abort"));

        Assert.ThrowsAsync<InvalidOperationException>(() => admin.ResolveCrossClusterSagaParticipantAsync(Saga, commit: true, "checking"));
        Assert.That(log.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("checking")), Is.EqualTo(1));
    }

    [Test]
    public void A_resolution_requires_a_saga_id_and_a_reason()
    {
        var (admin, _, _) = Create(resolved: true);

        Assert.ThrowsAsync<ArgumentException>(() => admin.ResolveCrossClusterSagaParticipantAsync(Saga, commit: true, ""));
        Assert.ThrowsAsync<ArgumentException>(() => admin.ResolveCrossClusterSagaParticipantAsync("", commit: true, "reason"));
    }

    [Test]
    public void A_resolution_fails_closed_without_a_grain_factory()
    {
        var admin = new LatticeReplicationAdmin(
            Substitute.For<ILatticeBootstrapCoordinator>(), Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(), new RecordingLogger());

        Assert.ThrowsAsync<InvalidOperationException>(() => admin.ResolveCrossClusterSagaParticipantAsync(Saga, commit: false, "reason"));
    }

    [Test]
    public void A_custom_admin_that_does_not_implement_the_resolution_refuses_it()
    {
        ILatticeReplicationAdmin admin = new MinimalAdmin();

        Assert.ThrowsAsync<NotSupportedException>(() => admin.ResolveCrossClusterSagaParticipantAsync(Saga, commit: false, "reason"));
    }

    [Test]
    public void A_transport_or_handler_that_predates_the_decision_query_refuses_it()
    {
        // The participant treats this as an unreachable coordinator and keeps its fence.
        ISagaControlChannel channel = new MinimalChannel();
        ILatticeSagaControlHandler handler = new NoParticipantSagaControlHandler();

        Assert.ThrowsAsync<NotSupportedException>(() => channel.GetDecisionAsync("site-home", new SagaControlRequest { SagaId = Saga }));
        Assert.ThrowsAsync<NotSupportedException>(() => handler.GetDecisionAsync(new SagaControlRequest { SagaId = Saga }));
    }

    private sealed class MinimalAdmin : ILatticeReplicationAdmin
    {
        public Task<OperatorReseedDecision> RequestSnapshotAsync(string treeName, string sourceClusterId, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public Task<OperatorReseedDecision> ForceRequestSnapshotAsync(string treeName, string sourceClusterId, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();
    }

    private sealed class MinimalChannel : ISagaControlChannel
    {
        public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    }
}
