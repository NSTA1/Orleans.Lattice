using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4385. A saga prepare that reaches a leaf after the leaf has already
/// applied that saga's terminal - a shard split's shadow-forward or
/// retroactive sweep trailing the terminal - is refused rather than bucketed.
/// A saga issues one terminal per transaction, so nothing would ever drain
/// such a bucket, and once the leaf no longer remembers the terminal (a
/// reactivation replaying the logged prepare, or a split stranding the key
/// outside the leaf's span) the orphan surfaces as the committed value: a
/// stale round over newer rows, or a key counted twice.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearLatePrepareRefusalAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
    }

    [Test]
    public async Task Prepared_set_trailing_its_sagas_terminal_is_refused()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.SetAsync("k", [99]);
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        await PreparedSetAsync(grain, txid, "k", [11]);
        await PreparedSetAsync(grain, txid, "fresh", [12]);
        var pendingKeys = await grain.GetPendingKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(grain.PendingTransactionCount, Is.Zero,
                "A prepare for a saga this leaf has already terminalled must not install a pending bucket.");
            Assert.That(pendingKeys, Is.Empty);
            Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 99 }));
            Assert.That(grain.EntriesForTest.ContainsKey("fresh"), Is.False);
        });
    }

    [Test]
    public async Task Prepared_set_many_trailing_its_sagas_terminal_is_refused()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.SetAsync("a", [1]);
        await grain.ApplyTxTerminalAsync(txid, committed: false, committedValues: null);

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetManyAsync([new("a", [7]), new("b", [8])]);
        }

        Assert.Multiple(() =>
        {
            Assert.That(grain.PendingTransactionCount, Is.Zero,
                "A batched prepare for a saga this leaf has already terminalled must not install a pending bucket.");
            Assert.That(grain.EntriesForTest["a"].Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(grain.EntriesForTest.ContainsKey("b"), Is.False);
        });
    }

    [Test]
    public async Task Prepared_delete_trailing_its_sagas_terminal_is_refused()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.SetAsync("k", [5]);
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.DeleteAsync("k");
        }

        LatticeTransactionContext.Set(Guid.Empty);
        var value = await grain.GetAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(grain.PendingTransactionCount, Is.Zero,
                "A prepared delete for a saga this leaf has already terminalled must not install a pending tombstone.");
            Assert.That(value, Is.EqualTo(new byte[] { 5 }));
        });
    }

    [Test]
    public async Task Prepared_set_for_a_saga_whose_terminal_has_not_landed_still_buckets()
    {
        var grain = CreateGrain();
        var terminalled = Guid.NewGuid();
        var inFlight = Guid.NewGuid();
        await grain.SetAsync("k", [99]);
        await grain.ApplyTxTerminalAsync(terminalled, committed: true, committedValues: null);

        await PreparedSetAsync(grain, inFlight, "k", [11]);
        var inFlightKeys = await grain.GetPendingKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(grain.PendingTransactionCount, Is.EqualTo(1),
                "Only a prepare for an already-terminalled saga is refused.");
            Assert.That(inFlightKeys, Is.EquivalentTo(new[] { "k" }));
        });
    }
}
