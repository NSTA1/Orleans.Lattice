using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public sealed class ReceiverSagaPoisonGrainTests
{
    private static Serializer<ReceiverSagaPoisonState> StateSerializer { get; } =
        new ServiceCollection().AddSerializer().BuildServiceProvider().GetRequiredService<Serializer<ReceiverSagaPoisonState>>();

    /// <summary>
    /// A state whose every write is stored as serialized bytes, and a restart
    /// that activates a new grain over a fresh state loaded from those bytes
    /// only. A mutation that is never written is therefore lost on restart, as
    /// it would be against real storage; re-using the same in-memory state
    /// object would hide it.
    /// </summary>
    private static (FakePersistentState<ReceiverSagaPoisonState> State, Func<ReceiverSagaPoisonGrain> Restart) Persisted()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        byte[]? stored = null;
        state.OnAfterWrite = written => stored = StateSerializer.SerializeToArray(written);
        return (state, () => new ReceiverSagaPoisonGrain(new FakePersistentState<ReceiverSagaPoisonState>
        {
            State = stored is null ? new ReceiverSagaPoisonState() : StateSerializer.Deserialize(stored),
        }));
    }

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
        var (state, restart) = Persisted();
        var grain = new ReceiverSagaPoisonGrain(state);
        var (retired, kept, other) = (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        await grain.PoisonAsync("site-a", retired, "one");
        await grain.PoisonAsync("site-a", kept, "two");

        await grain.RetireAsync("site-a", new[] { retired, other });
        var restarted = restart();

        Assert.Multiple(async () =>
        {
            Assert.That(await restarted.IsRetiredAsync("site-a", retired), Is.True, "issue #4692: a re-seed retired it, and that was written");
            Assert.That(await restarted.IsRetiredAsync("site-a", kept), Is.False, "still poisoned, not retired");
            Assert.That(await restarted.IsRetiredAsync("site-a", other), Is.False, "never poisoned, so nothing to remember");
            Assert.That(await restarted.IsRetiredAsync("site-b", retired), Is.False, "per origin");
            Assert.That(await restarted.GetPoisonedAsync("site-a"), Is.EquivalentTo(new[] { kept }), "the retire itself was written");
        });
    }

    [Test]
    public async Task QuarantineAsync_is_durable_idempotent_and_classified_apart_from_poison()
    {
        var (state, restart) = Persisted();
        var grain = new ReceiverSagaPoisonGrain(state);
        var (quarantined, poisoned, clean) = (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        await grain.PoisonAsync("site-a", poisoned, "poisoned");

        Assert.That(await grain.QuarantineAsync("site-a", quarantined, "integrity fault"), Is.True);
        var writes = state.WriteCount;
        Assert.That(await grain.QuarantineAsync("site-a", quarantined, "again"), Is.True);
        var restarted = restart();
        var classified = await restarted.ClassifyAsync("site-a", new[] { quarantined, poisoned, clean });

        Assert.Multiple(async () =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writes), "a repeat quarantine writes nothing");
            Assert.That(classified.Quarantined, Is.EquivalentTo(new[] { quarantined }), "the quarantine survives a restart from storage");
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

    [Test]
    public async Task QuarantineAsync_refuses_when_the_quarantine_set_is_full()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        for (var i = 0; i < 4096; i++)
        {
            state.State.Quarantined.Add(new ReceiverSagaPoisonRecord("site-a", Guid.NewGuid(), "seed"));
        }

        var grain = new ReceiverSagaPoisonGrain(state);
        var overflow = Guid.NewGuid();

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.QuarantineAsync("site-a", overflow, "overflow"), Is.False);
            Assert.That(await grain.GetQuarantinedAsync("site-a"), Does.Not.Contain(overflow));
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task ReleaseQuarantineAsync_removes_only_that_saga_durably_and_keeps_it_retired()
    {
        var (state, restart) = Persisted();
        var grain = new ReceiverSagaPoisonGrain(state);
        var (released, kept) = (Guid.NewGuid(), Guid.NewGuid());
        await grain.PoisonAsync("site-a", released, "poisoned");
        await grain.RetireAsync("site-a", new[] { released });
        await grain.QuarantineAsync("site-a", released, "integrity fault");
        await grain.QuarantineAsync("site-a", kept, "integrity fault");
        await grain.QuarantineAsync("site-b", released, "integrity fault");

        Assert.That(await grain.ReleaseQuarantineAsync("site-a", released), Is.True);
        var restarted = restart();

        Assert.Multiple(async () =>
        {
            Assert.That(await restarted.GetQuarantinedAsync("site-a"), Is.EquivalentTo(new[] { kept }), "the release was written");
            Assert.That(await restarted.GetQuarantinedAsync("site-b"), Is.EquivalentTo(new[] { released }), "per origin");
            Assert.That(await restarted.IsRetiredAsync("site-a", released), Is.True,
                "a released saga stays retired, so a record of it that fails again is quarantined again, never re-seeded");
            Assert.That(await restarted.ReleaseQuarantineAsync("site-a", released), Is.False, "nothing left to release");
        });
    }

    [Test]
    public async Task ReleaseQuarantineAsync_frees_capacity_in_a_full_quarantine_set()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        var first = Guid.NewGuid();
        state.State.Quarantined.Add(new ReceiverSagaPoisonRecord("site-a", first, "seed"));
        for (var i = 1; i < 4096; i++)
        {
            state.State.Quarantined.Add(new ReceiverSagaPoisonRecord("site-a", Guid.NewGuid(), "seed"));
        }

        var grain = new ReceiverSagaPoisonGrain(state);
        var next = Guid.NewGuid();
        Assert.That(await grain.QuarantineAsync("site-a", next, "overflow"), Is.False, "precondition: the set is full");

        Assert.That(await grain.ReleaseQuarantineAsync("site-a", first), Is.True);
        Assert.That(await grain.QuarantineAsync("site-a", next, "now fits"), Is.True);
    }

    [Test]
    public async Task ReleaseQuarantineAsync_restores_the_entry_when_the_write_fails()
    {
        var state = new FakePersistentState<ReceiverSagaPoisonState>();
        var grain = new ReceiverSagaPoisonGrain(state);
        var txid = Guid.NewGuid();
        await grain.QuarantineAsync("site-a", txid, "integrity fault");
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ReleaseQuarantineAsync("site-a", txid));
        Assert.That(await grain.GetQuarantinedAsync("site-a"), Is.EquivalentTo(new[] { txid }), "fail closed: still quarantined");
    }

    [Test]
    public void ReleaseQuarantineAsync_rejects_an_empty_transaction_id() =>
        Assert.ThrowsAsync<ArgumentException>(() =>
            new ReceiverSagaPoisonGrain(new FakePersistentState<ReceiverSagaPoisonState>()).ReleaseQuarantineAsync("site-a", Guid.Empty));
}