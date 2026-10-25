using System.Collections;
using System.Reflection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

public partial class DeadLetterTrackingReplicationApplierTests
{
    [TestCase(true, false)]
    [TestCase(false, false)]
    [TestCase(false, true)]
    public async Task Positive_timeout_expires_from_first_deferral_without_resetting_on_retry(bool prepare, bool abort)
    {
        var (decorator, inner, _, _, options, factory) = BuildWithFactory(1);
        options.SagaDeferralTimeout = TimeSpan.FromDays(1);
        options.AutoBootstrapOnFallOffLog = false;
        var poison = new ReceiverSagaPoisonGrain(new FakePersistentState<ReceiverSagaPoisonState>());
        factory.GetGrain<IReceiverSagaPoisonGrain>(TreeId).Returns(poison);
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(TxStatus.InFlight);
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);
        var entry = MakeEntry(op: prepare ? MutationKind.Set : abort ? MutationKind.TxAbort : MutationKind.TxCommit)
            with { TransactionId = Guid.NewGuid(), IsPrepared = prepare };
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("deterministic-failure"));

        Assert.That((await decorator.ApplyAsync(entry)).Deferred, Is.True);
        Assert.That(await poison.GetPoisonedAsync("site-b"), Is.Empty);

        var history = typeof(DeadLetterTrackingReplicationApplier)
            .GetField("_firstDeferrals", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(decorator) as IDictionary;
        Assert.That(history, Is.Not.Null);
        var keys = history!.Keys.Cast<object>().ToArray();
        Assert.That(keys, Has.Length.EqualTo(1));
        history[keys[0]] = DateTime.UtcNow.AddDays(-2);

        var expired = await decorator.ApplyAsync(entry);

        Assert.That(options.SagaDeferralTimeout, Is.EqualTo(TimeSpan.FromDays(1)));
        Assert.That(await poison.GetPoisonedAsync("site-b"), Does.Contain(entry.TransactionId),
            "retry must use the original elapsed clock, not renew it");
        Assert.That(expired.Deferred, Is.EqualTo(!prepare),
            "expired prepares park; expired commit/abort terminals await reseeding");
    }
}
