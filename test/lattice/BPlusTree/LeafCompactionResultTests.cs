using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Contract coverage for <see cref="LeafCompactionResult"/>, the value a
/// bounded tombstone-compaction turn reports itself with (issue 4135).
/// <para>
/// The two factories are the whole API, and the distinction between them is
/// load-bearing rather than cosmetic: a caller that reads
/// <see cref="LeafCompactionResult.Completed"/> as <see langword="true"/> will
/// drain the leaf's shard-root dirty mark. Getting the flag the wrong way round
/// on a truncated turn would drop a leaf from the dirty set with condemned
/// entries still in place, which is why it is asserted directly here as well as
/// through the grain.
/// </para>
/// </summary>
public class LeafCompactionResultTests
{
    [Test]
    public void Complete_reports_a_finished_turn_and_its_reaped_count()
    {
        var result = LeafCompactionResult.Complete(12);

        Assert.Multiple(() =>
        {
            Assert.That(result.Completed, Is.True);
            Assert.That(result.EntriesRemoved, Is.EqualTo(12));
        });
    }

    [Test]
    public void Truncated_reports_an_unfinished_turn_and_its_reaped_count()
    {
        var result = LeafCompactionResult.Truncated(5);

        Assert.Multiple(() =>
        {
            Assert.That(result.Completed, Is.False,
                "a truncated turn must never claim completeness - the caller drains the leaf's "
                + "dirty mark on that claim.");
            Assert.That(result.EntriesRemoved, Is.EqualTo(5),
                "a truncated turn still did work, and reports what it reaped.");
        });
    }

    [Test]
    public void A_turn_that_reaped_nothing_can_still_be_incomplete()
    {
        // Incomplete is the default and is independent of the count. A turn
        // whose budget was spent before it reached anything reaps zero and is
        // still outstanding work; conflating "reaped nothing" with "nothing to
        // do" is exactly the inference the Completed flag exists to prevent.
        var exhausted = LeafCompactionResult.Truncated(0);
        var cleanNoop = LeafCompactionResult.Complete(0);

        Assert.Multiple(() =>
        {
            Assert.That(exhausted.Completed, Is.False);
            Assert.That(cleanNoop.Completed, Is.True);
            Assert.That(exhausted, Is.Not.EqualTo(cleanNoop),
                "the two must be distinguishable by value, not only by the flag read in isolation.");
        });
    }

    [Test]
    public void Results_with_the_same_count_and_completeness_are_equal()
    {
        Assert.That(LeafCompactionResult.Complete(3), Is.EqualTo(LeafCompactionResult.Complete(3)));
    }
}
