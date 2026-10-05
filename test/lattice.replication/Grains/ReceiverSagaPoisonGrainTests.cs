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
}
