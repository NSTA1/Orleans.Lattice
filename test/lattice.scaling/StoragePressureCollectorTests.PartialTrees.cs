namespace Orleans.Lattice.Scaling.Tests;

public sealed partial class StoragePressureCollectorTests
{
    [Test]
    public async Task CollectAsync_excludes_a_partial_tree_from_the_pressure_and_counts_it()
    {
        // Issue #6017: a partial tree's byte figure is a lower bound - a WAL
        // partition whose scan was refused contributed nothing - so weighing it
        // under-states pressure on exactly the account too busy to answer.
        var complete = Tree("complete", bytes: 400, ceiling: 1_000, partitions: (0, AcctA));
        var partial = Tree("partial", bytes: 100, ceiling: 1_000, partitions: (0, AcctA)) with { Partial = true };
        var collector = Collector(new FakeSource(Sample([AcctA], complete, partial)));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.ExcludedPartialTrees, Is.EqualTo(1));
            Assert.That(pressure.WalRetainedBytes, Is.EqualTo(400L),
                "the partial tree's lower-bound bytes are left out of the aggregate");
            Assert.That(Account(pressure, AcctA).WalRetainedBytes, Is.EqualTo(400L),
                "the partial tree's lower-bound bytes are left out of its account");
        });
    }

    [Test]
    public async Task CollectAsync_does_not_let_a_partial_trees_ceiling_hide_a_hot_account()
    {
        // The complete tree alone is over its own advisory ceiling. Counting the
        // partial tree's ceiling but only its lower-bound bytes would dilute the
        // account below the threshold.
        var complete = Tree("complete", bytes: 950, ceiling: 1_000, partitions: (0, AcctA));
        var partial = Tree("partial", bytes: 0, ceiling: 1_000, partitions: (0, AcctA)) with { Partial = true };
        var collector = Collector(new FakeSource(Sample([AcctA], complete, partial)));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Account(pressure, AcctA).OverThreshold, Is.True);
            Assert.That(pressure.OverThreshold, Is.True);
            Assert.That(pressure.ExcludedPartialTrees, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task CollectAsync_with_every_tree_complete_excludes_none()
    {
        var collector = Collector(new FakeSource(Sample([AcctA], Tree("t", bytes: 10, partitions: (0, AcctA)))));

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.That(pressure.ExcludedPartialTrees, Is.Zero);
    }
}
