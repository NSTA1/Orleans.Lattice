using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// #4603 on the real site-B tree. Every dead-lettered entry was acknowledged
/// without being applied, so the dead-letter queue holds the only copy of its
/// write. A full queue must refuse and defer, never evict, and an operator
/// discard must leave a lost mark, so that an entry depending on the lost write
/// is dead-lettered instead of applied.
/// </summary>
public partial class ReplicationApplyIntegrationTests
{
    private static WalRecord ModeMismatched(string key, long ticks) =>
        LwwSet(TwoSiteClusterFixture.SmallDeadLetterQueueTree, key, new byte[] { 7 }, Hlc(ticks))
            with { Mode = LatticeMergeMode.PnCounter };

    [Test]
    public async Task A_full_dead_letter_queue_defers_instead_of_evicting_and_a_discarded_write_blocks_its_dependents()
    {
        const string tree = TwoSiteClusterFixture.SmallDeadLetterQueueTree;
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var dlq = _fixture.SiteB.Client.GetGrain<IReplicationDeadLetterGrain>(tree);
        var applier = CreateSiteBApplier();

        // Two mode-mismatched entries fill the queue (capacity 2). Each is
        // acknowledged to its sender; the queue is the only copy.
        var first = ModeMismatched("m1", 10);
        Assert.That((await applier.ApplyAsync(first)).Deferred, Is.False);
        Assert.That((await applier.ApplyAsync(ModeMismatched("m2", 11))).Deferred, Is.False);

        // A third cannot be parked: it is deferred (not acknowledged), and
        // nothing already parked is evicted.
        var third = await applier.ApplyAsync(ModeMismatched("m3", 12));
        var parked = await dlq.ListAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(third.Deferred, Is.True, "A full queue must defer, so the sender keeps the entry.");
            Assert.That(parked.Select(p => p.Entry.Key), Is.EqualTo(new[] { "m1", "m2" }), "Nothing was evicted.");
        });

        // The operator discards m1: its write is lost on this cluster for good.
        var m1Id = parked.Single(p => p.Entry.Key == "m1").EntryId;
        Assert.That(await dlq.DiscardAsync(m1Id, CancellationToken.None), Is.True);

        // A later write of the same origin applies and lifts its high-water
        // mark past the lost write, so a check against the mark alone would
        // wrongly release the dependent.
        Assert.That((await applier.ApplyAsync(LwwSet(tree, "later", new byte[] { 2 }, Hlc(30)))).Applied, Is.True);

        // An entry that depends on the lost write must never be applied: it is
        // dead-lettered as a terminal state.
        var dependency = new VersionVector();
        dependency.Entries[first.OriginClusterId!] = first.Timestamp;
        var dependent = LwwSet(tree, "dep", new byte[] { 1 }, Hlc(20)) with
        {
            OriginClusterId = "site-c",
            VectorClock = dependency,
        };
        var result = await applier.ApplyAsync(dependent);

        var afterDiscard = await dlq.ListAsync(CancellationToken.None);
        var depValue = (await lattice.GetWithVersionAsync("dep")).Value;
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(depValue, Is.Null, "Never applied before its lost dependency.");
            Assert.That(
                afterDiscard.Select(p => p.Entry.Key),
                Is.EquivalentTo(new[] { "m2", "dep" }),
                "The dependent is parked as dependency_lost.");
        });
    }

    [Test]
    public async Task A_dependent_of_a_discarded_write_is_dead_lettered_even_when_the_high_water_mark_passed_it()
    {
        const string tree = "ri-dlq-lost-dependent";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var dlq = _fixture.SiteB.Client.GetGrain<IReplicationDeadLetterGrain>(tree);
        var applier = CreateSiteBApplier();

        // A site-a write is dead-lettered, then discarded: lost here for good.
        var lostWrite = LwwSet(tree, "lost", new byte[] { 7 }, Hlc(10)) with { Mode = LatticeMergeMode.PnCounter };
        await applier.ApplyAsync(lostWrite);
        var parkedId = (await dlq.ListAsync(CancellationToken.None)).Single().EntryId;
        Assert.That(await dlq.DiscardAsync(parkedId, CancellationToken.None), Is.True);

        // A later site-a write lifts site-a's high-water mark past it.
        Assert.That((await applier.ApplyAsync(LwwSet(tree, "later", new byte[] { 2 }, Hlc(30)))).Applied, Is.True);

        var dependency = new VersionVector();
        dependency.Entries[lostWrite.OriginClusterId!] = lostWrite.Timestamp;
        var dependent = LwwSet(tree, "dep", new byte[] { 1 }, Hlc(20)) with { OriginClusterId = "site-c", VectorClock = dependency };
        var result = await applier.ApplyAsync(dependent);

        var parked = await dlq.ListAsync(CancellationToken.None);
        var depValue = (await lattice.GetWithVersionAsync("dep")).Value;
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(depValue, Is.Null, "Never applied before its lost dependency.");
            Assert.That(parked.Select(p => p.Entry.Key), Is.EqualTo(new[] { "dep" }), "Dead-lettered as dependency_lost.");
        });
    }
}
