using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class WalShardPageTests
{
    [Test]
    public void Empty_returns_page_with_no_entries_and_supplied_next_sequence()
    {
        var page = WalShardPage.Empty(42);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(42L));
        });
    }

    [Test]
    public void Empty_at_zero_yields_empty_initial_cursor()
    {
        var page = WalShardPage.Empty(0);
        Assert.That(page.NextSequence, Is.EqualTo(0L));
    }

    [Test]
    public void With_initialiser_round_trips_entries_and_next_sequence()
    {
        var entries = new[]
        {
            new WalShardSequencedEntry { Sequence = 0, Entry = new WalRecord { TreeId = "t", Key = "a", Op = MutationKind.Set } },
            new WalShardSequencedEntry { Sequence = 1, Entry = new WalRecord { TreeId = "t", Key = "b", Op = MutationKind.Set } },
        };

        var page = new WalShardPage { Entries = entries, NextSequence = 2 };

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(entries));
            Assert.That(page.NextSequence, Is.EqualTo(2L));
        });
    }
}
