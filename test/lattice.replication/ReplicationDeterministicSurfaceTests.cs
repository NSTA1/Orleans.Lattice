using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for the small deterministic replication surfaces that carry no
/// state of their own: the durable saga control handler's routing and
/// cancellation contract, the framework-contract constructors of the two typed
/// replication exceptions, the reflection-based gRPC status extraction in
/// <see cref="LatticeBootstrapTransientFaultClassifier"/>, and the default
/// <see cref="IReplicationApplier.ApplyBatchAsync"/> aggregation a legacy
/// applier inherits.
/// </summary>
/// <remarks>
/// Each of these is reached only from a path that is otherwise expensive to
/// stand up (a cross-cluster saga, a peer bootstrap over gRPC, a legacy applier
/// implementation), so each is asserted directly here instead. Deterministic -
/// substituted grains, no cluster, no transport.
/// </remarks>
[TestFixture]
public sealed class ReplicationDeterministicSurfaceTests
{
    // ---- LatticeSagaControlHandler -----------------------------------------

    private static (LatticeSagaControlHandler Handler, ICrossClusterSagaParticipantGrain Participant) Saga()
    {
        var participant = Substitute.For<ICrossClusterSagaParticipantGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ICrossClusterSagaParticipantGrain>("saga-1", null).Returns(participant);
        return (new LatticeSagaControlHandler(factory), participant);
    }

    private static SagaControlRequest Request() => new() { SagaId = "saga-1" };

    [Test]
    public void Saga_handler_rejects_a_null_grain_factory()
        => Assert.That(() => new LatticeSagaControlHandler(null!), Throws.ArgumentNullException);

    [Test]
    public async Task Saga_handler_routes_prepare_to_the_per_saga_participant_grain()
    {
        var (handler, participant) = Saga();
        var expected = new SagaControlResponse { SagaId = "saga-1" };
        participant.PrepareAsync(Arg.Any<SagaControlRequest>()).Returns(expected);

        var response = await handler.PrepareAsync(Request());

        Assert.That(response, Is.EqualTo(expected));
        await participant.Received(1).PrepareAsync(Arg.Is<SagaControlRequest>(r => r.SagaId == "saga-1"));
    }

    [Test]
    public async Task Saga_handler_routes_commit_to_the_per_saga_participant_grain()
    {
        var (handler, participant) = Saga();
        participant.CommitAsync(Arg.Any<SagaControlRequest>()).Returns(new SagaControlResponse());

        await handler.CommitAsync(Request());

        await participant.Received(1).CommitAsync(Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task Saga_handler_routes_abort_to_the_per_saga_participant_grain()
    {
        var (handler, participant) = Saga();
        participant.AbortAsync(Arg.Any<SagaControlRequest>()).Returns(new SagaControlResponse());

        await handler.AbortAsync(Request());

        await participant.Received(1).AbortAsync(Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task Saga_handler_routes_status_to_the_per_saga_participant_grain()
    {
        var (handler, participant) = Saga();
        participant.GetStatusAsync(Arg.Any<SagaControlRequest>()).Returns(new SagaControlResponse());

        await handler.GetStatusAsync(Request());

        await participant.Received(1).GetStatusAsync(Arg.Any<SagaControlRequest>());
    }

    [Test]
    public void Saga_handler_observes_cancellation_before_it_touches_a_grain()
    {
        var (handler, participant) = Saga();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await handler.PrepareAsync(Request(), cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
            Assert.That(async () => await handler.CommitAsync(Request(), cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
            Assert.That(async () => await handler.AbortAsync(Request(), cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
            Assert.That(async () => await handler.GetStatusAsync(Request(), cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
        });

        Assert.That(participant.ReceivedCalls(), Is.Empty,
            "A cancelled control RPC must never reach the participant grain, so no durable record is touched.");
    }

    // ---- typed replication exceptions --------------------------------------

    [Test]
    public void Precondition_failed_exception_default_constructor_leaves_empty_context()
    {
        var exception = new LatticeReplicationPreconditionFailedException();

        Assert.Multiple(() =>
        {
            Assert.That(exception.TreeId, Is.Empty);
            Assert.That(exception.RequestedMode, Is.EqualTo(default(LatticeMergeMode)));
        });
    }

    [Test]
    public void Precondition_failed_exception_message_constructor_leaves_empty_context()
    {
        var exception = new LatticeReplicationPreconditionFailedException("boom");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.TreeId, Is.Empty);
        });
    }

    [Test]
    public void Precondition_failed_exception_preserves_its_inner_exception()
    {
        var inner = new InvalidOperationException("cause");

        var exception = new LatticeReplicationPreconditionFailedException("boom", inner);

        Assert.Multiple(() =>
        {
            Assert.That(exception.InnerException, Is.SameAs(inner));
            Assert.That(exception.TreeId, Is.Empty);
        });
    }

    [Test]
    public void Precondition_failed_exception_carries_the_tree_and_requested_mode()
    {
        var exception = new LatticeReplicationPreconditionFailedException(
            "boom", "orders", LatticeMergeMode.OrSet);

        Assert.Multiple(() =>
        {
            Assert.That(exception.TreeId, Is.EqualTo("orders"));
            Assert.That(exception.RequestedMode, Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }

    [Test]
    public void Precondition_failed_exception_rejects_a_null_tree_id()
        => Assert.That(
            () => new LatticeReplicationPreconditionFailedException("boom", null!, LatticeMergeMode.OrSet),
            Throws.ArgumentNullException);

    [Test]
    public void Mode_change_rejected_exception_default_constructor_leaves_empty_context()
    {
        var exception = new LatticeReplicationModeChangeRejectedException();

        Assert.Multiple(() =>
        {
            Assert.That(exception.TreeId, Is.Empty);
            Assert.That(exception.CurrentModeAmbiguous, Is.False);
        });
    }

    [Test]
    public void Mode_change_rejected_exception_message_constructor_leaves_empty_context()
    {
        var exception = new LatticeReplicationModeChangeRejectedException("boom");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.TreeId, Is.Empty);
        });
    }

    [Test]
    public void Mode_change_rejected_exception_preserves_its_inner_exception()
    {
        var inner = new InvalidOperationException("cause");

        var exception = new LatticeReplicationModeChangeRejectedException("boom", inner);

        Assert.That(exception.InnerException, Is.SameAs(inner));
    }

    // ---- LatticeBootstrapTransientFaultClassifier ---------------------------

    [Test]
    public void Classifier_does_not_treat_a_type_merely_named_RpcException_as_transient()
    {
        // The classifier matches Grpc.Core.RpcException by *full* type name so the
        // replication package never links Grpc.Core. A host type that happens to
        // share the simple name must therefore not be mistaken for a transport
        // fault and retried.
        Assert.That(
            LatticeBootstrapTransientFaultClassifier.IsTransient(new RpcException("look-alike")),
            Is.False);
    }

    [Test]
    public void Classifier_does_not_treat_an_arbitrary_status_carrying_fault_as_transient()
    {
        Assert.That(
            LatticeBootstrapTransientFaultClassifier.IsTransient(new DirectStatusCodeException(14)),
            Is.False,
            "A status code alone does not make a fault transient; the type must be the real gRPC one.");
    }

    // ---- IReplicationApplier.ApplyBatchAsync default ------------------------

    [Test]
    public async Task Default_batch_apply_aggregates_the_highest_high_water_mark()
    {
        var applier = new RecordingApplier(
        [
            new ApplyResult { Applied = false, HighWaterMark = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 } },
            new ApplyResult { Applied = true, HighWaterMark = new HybridLogicalClock { WallClockTicks = 9, Counter = 0 } },
            new ApplyResult { Applied = false, HighWaterMark = new HybridLogicalClock { WallClockTicks = 7, Counter = 0 } },
        ]);

        var result = await ((IReplicationApplier)applier).ApplyBatchAsync([Record(), Record(), Record()]);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True, "One newly merged entry makes the batch applied.");
            Assert.That(result.HighWaterMark, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 9, Counter = 0 }),
                "The aggregate HWM is the pointwise maximum across every entry, not the last one seen.");
            Assert.That(applier.Applied, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public async Task Default_batch_apply_of_a_fully_deduped_batch_reports_not_applied()
    {
        var applier = new RecordingApplier(
        [
            new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero },
        ]);

        var result = await ((IReplicationApplier)applier).ApplyBatchAsync([Record()]);

        Assert.That(result.Applied, Is.False);
    }

    [Test]
    public async Task Default_batch_apply_of_an_empty_batch_is_a_no_op()
    {
        var applier = new RecordingApplier([]);

        var result = await ((IReplicationApplier)applier).ApplyBatchAsync([]);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void Default_batch_apply_rejects_a_null_entry_list()
        => Assert.That(
            async () => await ((IReplicationApplier)new RecordingApplier([])).ApplyBatchAsync(null!),
            Throws.ArgumentNullException);

    [Test]
    public void Default_batch_apply_observes_cancellation_between_entries()
    {
        var applier = new RecordingApplier(
        [
            new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero },
        ]);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await ((IReplicationApplier)applier).ApplyBatchAsync([Record()], cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(applier.Applied, Is.Empty,
            "Cancellation is observed before the first entry is applied, so nothing is half-installed.");
    }

    private static WalRecord Record()
        => new()
        {
            TreeId = "orders",
            Key = "k",
            Timestamp = HybridLogicalClock.Zero,
        };

    /// <summary>A legacy applier that implements only the single-entry seam, inheriting the batch default.</summary>
    private sealed class RecordingApplier(IReadOnlyList<ApplyResult> results) : IReplicationApplier
    {
        private int _index;

        public List<WalRecord> Applied { get; } = [];

        public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            Applied.Add(entry);
            return Task.FromResult(results[_index++]);
        }
    }

    /// <summary>A fault exposing a gRPC-looking status code but not the real gRPC type.</summary>
    private sealed class DirectStatusCodeException(int statusCode) : Exception("rpc")
    {
        public int StatusCode { get; } = statusCode;
    }

    /// <summary>A host type merely named RpcException, exposing no status at all.</summary>
    private sealed class RpcException(string message) : Exception(message);
}
