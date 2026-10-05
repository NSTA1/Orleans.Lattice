namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// The read-side fall-off check (issue #4584): a trim that lands after the
/// pre-read tail probe must still be reported as a fall-off, never skipped over,
/// while a hole left by a failed flush (the tail has not moved past it) is not a
/// fall-off.
/// </summary>
public sealed partial class WalLogSubscriberTests
{
    [Test]
    public async Task DrainAsync_reports_fell_off_log_when_a_trim_lands_between_the_tail_probe_and_the_read()
    {
        var (subscriber, reader, _) = Create();
        for (var i = 0; i < 5; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }

        // The pre-read probe sees an intact log; the trim lands as the read starts.
        reader.BeforeRead = () => reader.TrimBefore(Tree, 0, 3);
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(
            Context(1, new Dictionary<int, long> { [0] = 0 }), handler, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.FellOffLog, Is.True, "offsets 1 and 2 were trimmed before the consumer read them");
            Assert.That(handler.Entries, Is.Empty, "nothing past the trimmed range may be surfaced");
        });
    }

    [Test]
    public async Task DrainAsync_reads_past_a_hole_the_tail_has_not_passed()
    {
        var (subscriber, reader, _) = Create();
        for (var i = 0; i < 4; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }

        reader.HoleAt(Tree, 0, 1);
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(
            Context(1, new Dictionary<int, long> { [0] = 0 }), handler, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.FellOffLog, Is.False, "an unacknowledged slot is not a trim");
            Assert.That(handler.Entries.Select(e => e.Offset), Is.EqualTo(new long[] { 2, 3 }));
        });
    }
}
