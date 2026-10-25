using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using System.Collections;
using System.Reflection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

public partial class DeadLetterTrackingReplicationApplierTests
{
    [TestCase(true, false)]
    [TestCase(false, false)]
    [TestCase(false, true)]
    public async Task Failing_saga_retry_expires_to_poison_then_quarantine_without_reseed_loop(
        bool prepare, bool abort, Action<bool>? observeTerminalExpiry = null)
    {
        var (decorator, inner, dlq, _, options, factory) = BuildWithFactory(1);
        options.SagaDeferralTimeout = TimeSpan.FromDays(1);
        options.AutoBootstrapOnFallOffLog = false;
        var poisonState = new FakePersistentState<ReceiverSagaPoisonState>();
        var poison = new ReceiverSagaPoisonGrain(poisonState);
        factory.GetGrain<IReceiverSagaPoisonGrain>(TreeId).Returns(poison);
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(TxStatus.InFlight);
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);
        var tx = Guid.NewGuid();
        var entry = MakeEntry(op: prepare ? MutationKind.Set : abort ? MutationKind.TxAbort : MutationKind.TxCommit)
            with { TransactionId = tx, IsPrepared = prepare };
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("deterministic-failure"));

        var beforeExpiry = await decorator.ApplyAsync(entry);
        Assert.That(beforeExpiry.Deferred, Is.True);
        Assert.That(await poison.GetPoisonedAsync("site-b"), Is.Empty);
        await dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());

        // The expiry opportunity is independent of the retry count.
        options.SagaDeferralTimeout = TimeSpan.Zero;
        var expired = await decorator.ApplyAsync(entry);
        var poisoned = await poison.GetPoisonedAsync("site-b");
        observeTerminalExpiry?.Invoke(!prepare && expired.Deferred && poisoned.Contains(tx));
        Assert.That(poisoned, Does.Contain(tx));
        Assert.That(await poison.GetReseedOwedOriginsAsync(), Does.Contain("site-b"));
        Assert.That(expired.Deferred, Is.EqualTo(!prepare),
            "a poisoned prepare can park; a poisoned terminal stays withheld until reseed");

        await poison.RetireAsync("site-b", [tx]);
        options.SagaDeferralTimeout = TimeSpan.FromDays(1);
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        decorator = new DeadLetterTrackingReplicationApplier(
            inner, factory, monitor, NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.True,
            "retirement does not waive the newly observed failure's expiry threshold");
        Assert.That(await poison.GetQuarantinedAsync("site-b"), Is.Empty);
        options.SagaDeferralTimeout = TimeSpan.Zero;
        var repeated = await decorator.ApplyAsync(entry);
        Assert.Multiple(async () =>
        {
            Assert.That(repeated.Deferred, Is.False, "a repeated deterministic fault parks after quarantine");
            Assert.That(await poison.GetQuarantinedAsync("site-b"), Does.Contain(tx));
            Assert.That(await poison.GetPoisonedAsync("site-b"), Is.Empty);
            Assert.That(await poison.GetReseedOwedOriginsAsync(), Is.Empty, "never start the poison/reseed cycle again");
        });
        await dlq.Received(prepare ? 2 : 1).EnqueueAsync(
            entry, Arg.Any<string>(), Arg.Any<int>(), LatticeReplicationMetrics.ReasonPoisonedSaga,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Full_dead_letter_queue_preserves_unacknowledged_record_until_capacity_returns()
    {
        var (decorator, inner, dlq, hwm, _) = Build(1);
        var entry = MakeEntry();
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("invalid"));
        var full = true;
        dlq.EnqueueAsync(entry, Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => full
                ? Task.FromException<long>(new ReplicationDeadLetterQueueFullException(TreeId))
                : Task.FromResult(1L));

        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.True);
        await hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
        full = false;
        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.False);
        await hwm.Received(1).TryAdvanceAsync(entry.OriginClusterId!, entry.Timestamp, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Decided_prepare_at_expired_timeout_remains_unacknowledged_without_poisoning_a_decision()
    {
        var (decorator, inner, dlq, _, options, factory) = BuildWithFactory(1);
        options.SagaDeferralTimeout = TimeSpan.Zero;
        var poison = new ReceiverSagaPoisonGrain(new FakePersistentState<ReceiverSagaPoisonState>());
        factory.GetGrain<IReceiverSagaPoisonGrain>(TreeId).Returns(poison);
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(TxStatus.Committed);
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);
        var entry = MakeEntry() with { TransactionId = Guid.NewGuid(), IsPrepared = true };
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("failed decided prepare"));

        for (var retry = 0; retry < 3; retry++)
        {
            Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.True);
        }

        Assert.That(await poison.GetPoisonedAsync("site-b"), Is.Empty);
        await dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Decided_prepare_recovers_when_application_succeeds_without_rewriting_the_decision()
    {
        var (decorator, inner, _, _, options, factory) = BuildWithFactory(1);
        options.SagaDeferralTimeout = TimeSpan.Zero;
        var poison = new ReceiverSagaPoisonGrain(new FakePersistentState<ReceiverSagaPoisonState>());
        factory.GetGrain<IReceiverSagaPoisonGrain>(TreeId).Returns(poison);
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(TxStatus.Committed);
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);
        var entry = MakeEntry() with { TransactionId = Guid.NewGuid(), IsPrepared = true };
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("temporarily unavailable"));
        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.True);

        inner.ApplyAsync(entry, Arg.Any<CancellationToken>()).Returns(new ApplyResult { Applied = true });
        Assert.That((await decorator.ApplyAsync(entry)).Applied, Is.True);
        await inner.Received(2).ApplyAsync(entry, Arg.Any<CancellationToken>());
        Assert.That(await poison.GetPoisonedAsync("site-b"), Is.Empty);
        Assert.That(await registry.GetRecordedStatusAsync(entry.TransactionId), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task Receiver_restart_preserves_poison_and_requires_a_new_expiry_opportunity_for_fresh_deferrals()
    {
        var (decorator, inner, dlq, _, options, factory) = BuildWithFactory(1);
        options.SagaDeferralTimeout = TimeSpan.FromDays(1);
        options.AutoBootstrapOnFallOffLog = false;
        var poison = new ReceiverSagaPoisonGrain(new FakePersistentState<ReceiverSagaPoisonState>());
        factory.GetGrain<IReceiverSagaPoisonGrain>(TreeId).Returns(poison);
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(TxStatus.InFlight);
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);
        var entry = MakeEntry() with { TransactionId = Guid.NewGuid(), IsPrepared = true };
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("deterministic-failure"));
        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.True);
        var deferrals = typeof(DeadLetterTrackingReplicationApplier)
            .GetField("_firstDeferrals", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(decorator) as IDictionary;
        Assert.That(deferrals, Is.Not.Null);
        Assert.That(deferrals!.Count, Is.EqualTo(1));
        foreach (var key in deferrals.Keys.Cast<object>().ToArray())
        {
            deferrals[key] = DateTime.UtcNow.AddDays(-2);
        }

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        decorator = new DeadLetterTrackingReplicationApplier(
            inner, factory, monitor, NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.True);
        Assert.That(await poison.GetPoisonedAsync("site-b"), Is.Empty);

        options.SagaDeferralTimeout = TimeSpan.Zero;
        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.False);
        Assert.That(await poison.GetPoisonedAsync("site-b"), Does.Contain(entry.TransactionId));
        options.SagaDeferralTimeout = TimeSpan.FromDays(1);
        decorator = new DeadLetterTrackingReplicationApplier(
            inner, factory, monitor, NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.False,
            "durable poison survives a receiver restart; the timeout need not expire twice");
        await dlq.Received(2).EnqueueAsync(
            entry, Arg.Any<string>(), Arg.Any<int>(), LatticeReplicationMetrics.ReasonPoisonedSaga,
            Arg.Any<CancellationToken>());
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Full_poison_set_preserves_delivery_until_retirement_frees_capacity(bool prepare)
    {
        var (decorator, inner, dlq, _, options, factory) = BuildWithFactory(1);
        options.SagaDeferralTimeout = TimeSpan.Zero;
        options.AutoBootstrapOnFallOffLog = false;
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        for (var i = 0; i < 4096; i++)
        {
            state.State.Entries.Add(new ReceiverSagaPoisonRecord("site-b", Guid.NewGuid(), "seed"));
        }
        var oldest = state.State.Entries[0].TransactionId;
        var poison = new ReceiverSagaPoisonGrain(state);
        factory.GetGrain<IReceiverSagaPoisonGrain>(TreeId).Returns(poison);
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(TxStatus.InFlight);
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);
        var entry = MakeEntry(op: prepare ? MutationKind.Set : MutationKind.TxCommit)
            with { TransactionId = Guid.NewGuid(), IsPrepared = prepare };
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("deterministic-failure"));

        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.True);
        Assert.That(await poison.GetPoisonedAsync("site-b"), Does.Not.Contain(entry.TransactionId));
        Assert.That(await poison.GetReseedOwedOriginsAsync(), Is.Empty,
            "a failed poison admission cannot claim that this record started reseeding");
        await dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());

        await poison.RetireAsync("site-b", [oldest]);
        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.EqualTo(!prepare));
        Assert.That(await poison.GetPoisonedAsync("site-b"), Does.Contain(entry.TransactionId));
        Assert.That(state.State.Entries, Has.Count.EqualTo(4096), "capacity remains bounded after recovery");
        await dlq.Received(prepare ? 1 : 0).EnqueueAsync(
            entry, Arg.Any<string>(), Arg.Any<int>(), LatticeReplicationMetrics.ReasonPoisonedSaga,
            Arg.Any<CancellationToken>());
    }
}
