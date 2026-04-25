using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplogShardPageTests
{
    [Test]
    public void Empty_returns_page_with_no_entries_and_supplied_next_sequence()
    {
        var page = ReplogShardPage.Empty(42);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(42L));
        });
    }

    [Test]
    public void Empty_at_zero_yields_empty_initial_cursor()
    {
        var page = ReplogShardPage.Empty(0);
        Assert.That(page.NextSequence, Is.EqualTo(0L));
    }

    [Test]
    public void With_initialiser_round_trips_entries_and_next_sequence()
    {
        var entries = new[]
        {
            new ReplogShardEntry { Sequence = 0, Entry = new ReplogEntry { TreeId = "t", Key = "a", Op = ReplogOp.Set } },
            new ReplogShardEntry { Sequence = 1, Entry = new ReplogEntry { TreeId = "t", Key = "b", Op = ReplogOp.Set } },
        };

        var page = new ReplogShardPage { Entries = entries, NextSequence = 2 };

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(entries));
            Assert.That(page.NextSequence, Is.EqualTo(2L));
        });
    }
}
