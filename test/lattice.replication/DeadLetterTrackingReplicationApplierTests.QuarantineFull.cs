using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4692: a saga that must be quarantined while the bounded quarantine set
/// is full is held, fail-closed. Poisoning it again instead would re-seed it,
/// and the re-seed cannot remove the cause, so the cycle quarantine exists to
/// end would return.
/// </summary>
public partial class DeadLetterTrackingReplicationApplierTests
{
    [Test]
    public async Task A_retired_saga_that_fails_again_while_the_quarantine_set_is_full_is_held_and_never_re_seeded()
    {
        var txid = Guid.NewGuid();
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        state.State.Retired.Add(new ReceiverSagaPoisonRecord("site-b", txid, "retired by a re-seed"));
        var oldest = Guid.NewGuid();
        state.State.Quarantined.Add(new ReceiverSagaPoisonRecord("site-b", oldest, "seed"));
        for (var i = 1; i < 4096; i++)
        {
            state.State.Quarantined.Add(new ReceiverSagaPoisonRecord("site-b", Guid.NewGuid(), "seed"));
        }

        var poison = new ReceiverSagaPoisonGrain(state);
        var inner = Substitute.For<IReplicationApplier>();
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("malformed terminal"));
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(TreeId).Returns(dlq);
        grainFactory.GetGrain<IReceiverSagaPoisonGrain>(TreeId).Returns(poison);
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaxApplyRetries = 1,
            SagaDeferralTimeout = TimeSpan.Zero,
        });
        var decorator = new DeadLetterTrackingReplicationApplier(
            inner, grainFactory, monitor, NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        var terminal = MakeEntry(op: MutationKind.TxCommit) with { TransactionId = txid, Key = "not-a-shard" };

        var outcomes = new ConcurrentBag<string>();
        ApplyResult held;
        using (MeterListening.StartForInstrument(LatticeReplicationMetrics.ReceiverSagaPoisoned, listener =>
            listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? tree = null;
                string? outcome = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagTree) tree = tag.Value as string;
                    if (tag.Key == LatticeReplicationMetrics.TagOutcome) outcome = tag.Value as string;
                }

                if (tree == TreeId && outcome is not null)
                {
                    outcomes.Add(outcome);
                }
            })))
        {
            held = await decorator.ApplyAsync(terminal, CancellationToken.None);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(held.Deferred, Is.True, "the record is held unacknowledged: the stream for this tree waits");
            Assert.That(await poison.GetPoisonedAsync("site-b"), Is.Empty, "the saga is not poisoned again");
            Assert.That(await poison.GetReseedOwedOriginsAsync(), Is.Empty, "so no re-seed is started or owed");
            Assert.That(await poison.GetQuarantinedAsync("site-b"), Does.Not.Contain(txid));
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.OutcomeReceiverSagaQuarantineFull), "the operator is alerted");
            Assert.That(outcomes, Does.Not.Contain(LatticeReplicationMetrics.OutcomeReceiverSagaPoisonedTerminalTimeout));
        });
        await dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());

        // Releasing a resolved quarantine frees capacity: the next attempt
        // quarantines the saga and parks its record, so the stream moves on.
        Assert.That(await poison.ReleaseQuarantineAsync("site-b", oldest), Is.True);
        var moved = await decorator.ApplyAsync(terminal, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(moved.Deferred, Is.False, "quarantined and parked, so acknowledged");
            Assert.That(await poison.GetQuarantinedAsync("site-b"), Does.Contain(txid));
            Assert.That(await poison.GetPoisonedAsync("site-b"), Is.Empty);
        });
        await dlq.Received(1).EnqueueAsync(
            terminal, Arg.Any<string>(), Arg.Any<int>(), LatticeReplicationMetrics.ReasonPoisonedSaga, Arg.Any<CancellationToken>());
    }
}
