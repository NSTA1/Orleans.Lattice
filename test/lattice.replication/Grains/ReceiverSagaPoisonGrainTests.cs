using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public sealed class ReceiverSagaPoisonGrainTests
{
    [Test]
    public async Task PoisonAsync_records_reseed_owed_and_filter_returns_poisoned_subset()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        var grain = new ReceiverSagaPoisonGrain(state);
        var poisoned = Guid.NewGuid();
        var other = Guid.NewGuid();

        var recorded = await grain.PoisonAsync("site-a", poisoned, "reason");
        var filtered = await grain.FilterPoisonedAsync("site-a", new[] { poisoned, other });
        var owed = await grain.GetReseedOwedOriginsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Is.True);
            Assert.That(filtered, Is.EquivalentTo(new[] { poisoned }));
            Assert.That(owed, Is.EquivalentTo(new[] { "site-a" }));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RetireAsync_removes_only_selected_transactions_and_clears_owed_when_origin_empty()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        var grain = new ReceiverSagaPoisonGrain(state);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        Assert.That(await grain.PoisonAsync("site-a", first, "one"), Is.True);
        Assert.That(await grain.PoisonAsync("site-a", second, "two"), Is.True);

        await grain.RetireAsync("site-a", new[] { first });
        Assert.That(await grain.GetPoisonedAsync("site-a"), Is.EquivalentTo(new[] { second }));
        Assert.That(await grain.GetReseedOwedOriginsAsync(), Is.EquivalentTo(new[] { "site-a" }));

        await grain.RetireAsync("site-a", new[] { second });
        var remaining = await grain.GetPoisonedAsync("site-a");
        var owed = await grain.GetReseedOwedOriginsAsync();
        Assert.Multiple(() =>
        {
            Assert.That(remaining, Is.Empty);
            Assert.That(owed, Is.Empty);
        });
    }

    [Test]
    public async Task PoisonAsync_refuses_when_capacity_is_full()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        for (var i = 0; i < 4096; i++)
        {
            state.State.Entries.Add(new ReceiverSagaPoisonRecord("site-a", Guid.NewGuid(), "seed"));
        }

        var grain = new ReceiverSagaPoisonGrain(state);

        var recorded = await grain.PoisonAsync("site-a", Guid.NewGuid(), "overflow");

        Assert.That(recorded, Is.False);
    }
    [Test]
    public async Task RetireAsync_remembers_the_retired_sagas_durably_and_only_those()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        var grain = new ReceiverSagaPoisonGrain(state);
        var (retired, kept, other) = (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        await grain.PoisonAsync("site-a", retired, "one");
        await grain.PoisonAsync("site-a", kept, "two");

        await grain.RetireAsync("site-a", new[] { retired, other });
        var restarted = new ReceiverSagaPoisonGrain(state);

        Assert.Multiple(async () =>
        {
            Assert.That(await restarted.IsRetiredAsync("site-a", retired), Is.True, "issue #4692: a re-seed retired it");
            Assert.That(await restarted.IsRetiredAsync("site-a", kept), Is.False, "still poisoned, not retired");
            Assert.That(await restarted.IsRetiredAsync("site-a", other), Is.False, "never poisoned, so nothing to remember");
            Assert.That(await restarted.IsRetiredAsync("site-b", retired), Is.False, "per origin");
        });
    }

    [Test]
    public async Task QuarantineAsync_is_durable_idempotent_and_classified_apart_from_poison()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        var grain = new ReceiverSagaPoisonGrain(state);
        var (quarantined, poisoned, clean) = (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        await grain.PoisonAsync("site-a", poisoned, "poisoned");

        Assert.That(await grain.QuarantineAsync("site-a", quarantined, "integrity fault"), Is.True);
        var writes = state.WriteCount;
        Assert.That(await grain.QuarantineAsync("site-a", quarantined, "again"), Is.True);
        var restarted = new ReceiverSagaPoisonGrain(state);
        var classified = await restarted.ClassifyAsync("site-a", new[] { quarantined, poisoned, clean });

        Assert.Multiple(async () =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writes), "a repeat quarantine writes nothing");
            Assert.That(classified.Quarantined, Is.EquivalentTo(new[] { quarantined }));
            Assert.That(classified.Poisoned, Is.EquivalentTo(new[] { poisoned }));
            Assert.That(await restarted.GetQuarantinedAsync("site-a"), Is.EquivalentTo(new[] { quarantined }));
            Assert.That(await restarted.GetQuarantinedAsync("site-b"), Is.Empty);
            Assert.That((await restarted.ClassifyAsync("site-a", Array.Empty<Guid>())).Quarantined, Is.Empty);
        });
    }

    [Test]
    public void QuarantineAsync_rejects_an_empty_transaction_id() =>
        Assert.ThrowsAsync<ArgumentException>(() =>
            new ReceiverSagaPoisonGrain(new FakePersistentState<ReceiverSagaPoisonState>()).QuarantineAsync("site-a", Guid.Empty, "reason"));
}
