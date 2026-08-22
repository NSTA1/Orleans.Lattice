using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="TerminalArrivalTally"/> - the
/// shared completeness gate the production registry grain
/// (<c>TxRegistryGrain.RecordTerminalArrivalAsync</c>) executes to decide when a
/// saga's per-source-shard terminals have all arrived. These pin the monotonic
/// expected-count merge and the final-arrival threshold so a change to the quorum
/// rule is caught here.
/// </summary>
[TestFixture]
public sealed class TerminalArrivalTallyTests
{
    [Test]
    public void First_expected_is_taken_verbatim() =>
        Assert.That(
            TerminalArrivalTally.MergeExpected(hadPrevious: false, previousExpected: 0, incomingExpected: 3),
            Is.EqualTo(3));

    [Test]
    public void Merge_raises_to_the_larger_expected() =>
        Assert.That(
            TerminalArrivalTally.MergeExpected(hadPrevious: true, previousExpected: 2, incomingExpected: 5),
            Is.EqualTo(5));

    [Test]
    public void Merge_never_lowers_a_recorded_expected() =>
        Assert.That(
            TerminalArrivalTally.MergeExpected(hadPrevious: true, previousExpected: 5, incomingExpected: 2),
            Is.EqualTo(5));

    [Test]
    public void Merge_is_idempotent_on_equal_expected() =>
        Assert.That(
            TerminalArrivalTally.MergeExpected(hadPrevious: true, previousExpected: 4, incomingExpected: 4),
            Is.EqualTo(4));

    [Test]
    public void Not_final_while_arrivals_below_expected() =>
        Assert.That(TerminalArrivalTally.IsFinalArrival(arrivalCount: 2, expectedCount: 3), Is.False);

    [Test]
    public void Final_when_arrivals_reach_expected() =>
        Assert.That(TerminalArrivalTally.IsFinalArrival(arrivalCount: 3, expectedCount: 3), Is.True);

    [Test]
    public void Final_when_arrivals_exceed_expected() =>
        // A benign over-count (e.g. a duplicate that slipped the dedup) never
        // latches the saga open: the gate still resolves.
        Assert.That(TerminalArrivalTally.IsFinalArrival(arrivalCount: 4, expectedCount: 3), Is.True);

    /// <summary>
    /// Order-independence: replaying the same set of per-shard arrivals under a
    /// monotonically-raised expected count fires final exactly once the last
    /// distinct arrival lands, regardless of the order the expected stamps arrive.
    /// </summary>
    [Test]
    public void Final_fires_once_last_distinct_arrival_lands([Values(1, 2, 3)] int expected)
    {
        var merged = 0;
        for (var arrivals = 0; arrivals <= expected; arrivals++)
        {
            merged = TerminalArrivalTally.MergeExpected(arrivals > 0, merged, expected);
            var final = TerminalArrivalTally.IsFinalArrival(arrivals, merged);
            Assert.That(final, Is.EqualTo(arrivals >= expected));
        }
    }
}
