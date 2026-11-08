using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Issue #6036: once a later entry lands above a range a stale cached trim
/// watermark hides, a read starts above that range and the cursor used to jump
/// it with nothing recorded. A jump that is not a trim now refreshes the
/// partition's watermark and re-reads once: what reappears is surfaced, and only
/// what is still missing is read past, metered by
/// <see cref="LatticeMetrics.WalSubscriberHolesSkipped"/>.
/// </summary>
public sealed partial class WalLogSubscriberTests
{
    private static Dictionary<string, long> CountHoleOutcomes(Action body, string tree)
    {
        var outcomes = new Dictionary<string, long>();
        using var listener = Orleans.Lattice.Testing.MeterListening.StartForInstrument(
            LatticeMetrics.WalSubscriberHolesSkipped,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? treeTag = null;
                string? outcome = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) treeTag = tag.Value as string;
                    if (tag.Key == LatticeMetrics.TagOutcome) outcome = tag.Value as string;
                }

                if (treeTag == tree && outcome is not null && value != 0)
                {
                    lock (outcomes)
                    {
                        outcomes[outcome] = outcomes.GetValueOrDefault(outcome) + value;
                    }
                }
            }));
        body();
        return outcomes;
    }

    [Test]
    public void A_range_a_stale_watermark_hides_below_a_later_entry_is_recovered_not_jumped()
    {
        var (subscriber, reader, _) = Create();
        for (var i = 0; i < 5; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }

        // Offsets 0-3 are stored but hidden; offset 4 landed above them.
        reader.ClampReadsThrough(Tree, 0, 3);
        var handler = new CollectingHandler();
        WalDrainResult? result = null;

        var outcomes = CountHoleOutcomes(() =>
            result = subscriber.DrainAsync(
                Context(1, new Dictionary<int, long> { [0] = -1 }, pinWal: false), handler).GetAwaiter().GetResult(), Tree);

        Assert.Multiple(() =>
        {
            Assert.That(result!.FellOffLog, Is.False, "nothing was trimmed");
            Assert.That(handler.Entries.Select(e => e.Offset), Is.EqualTo(new long[] { 0, 1, 2, 3, 4 }),
                "the refresh and re-read must surface the hidden range before the entry above it");
            Assert.That(reader.WatermarkRefreshes, Is.EqualTo(new[] { (Tree, 0) }));
            Assert.That(outcomes, Is.EquivalentTo(new Dictionary<string, long> { ["recovered"] = 4 }),
                "the four hidden offsets are metered as recovered, and nothing as a hole");
        });
    }

    [Test]
    public void A_genuine_hole_is_read_past_after_one_refresh_and_metered()
    {
        var (subscriber, reader, _) = Create();
        for (var i = 0; i < 4; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }

        reader.HoleAt(Tree, 0, 1);
        var handler = new CollectingHandler();

        var outcomes = CountHoleOutcomes(() =>
            subscriber.DrainAsync(
                Context(1, new Dictionary<int, long> { [0] = 0 }, pinWal: false), handler).GetAwaiter().GetResult(), Tree);

        Assert.Multiple(() =>
        {
            Assert.That(handler.Entries.Select(e => e.Offset), Is.EqualTo(new long[] { 2, 3 }));
            Assert.That(reader.WatermarkRefreshes, Has.Count.EqualTo(1), "one refresh per jump, then the hole is read past");
            Assert.That(outcomes, Is.EquivalentTo(new Dictionary<string, long> { ["hole"] = 1 }),
                "the one-offset hole is metered by its width");
        });
    }
}
