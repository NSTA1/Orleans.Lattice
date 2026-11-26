using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// The append timeline's sorted ring (issue #6166) against the reference it replaced:
// a sorted map whose first record of an offset stands and whose smallest offset is
// evicted past capacity. The drain-lag sampler queries it once per fresh consumer per
// tick, under the lock every WAL append takes.
public partial class WalSaturationSamplerDrainLagTests
{
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    public void The_append_timeline_answers_as_a_bounded_sorted_map_under_out_of_order_acknowledgements(int seed)
    {
        var random = new Random(seed);
        var timeline = new WalAppendTimeline();
        var reference = new SortedDictionary<long, long>();
        var head = -1L;
        var next = 0L;
        for (var step = 0; step < 3 * WalAppendTimeline.Capacity; step++)
        {
            // Mostly in order, with late acknowledgements, duplicates and skipped (remote) offsets.
            var offset = random.Next(10) switch
            {
                0 => Math.Max(0, next - random.Next(1, 64)),
                1 => next += random.Next(2, 8),
                _ => next++,
            };
            var ticks = step + 1L;
            timeline.Record(offset, ticks);
            head = Math.Max(head, offset);
            reference.TryAdd(offset, ticks);
            while (reference.Count > WalAppendTimeline.Capacity)
                reference.Remove(reference.First().Key);

            if (step % 97 == 0)
            {
                for (var probe = 0; probe < 16; probe++)
                {
                    var checkpoint = random.Next(-1, (int)head + 3);
                    var lag = timeline.GetLagTicks(checkpoint, 1_000_000, out var incomplete);
                    var expected = ReferenceLag(reference, head, checkpoint, 1_000_000, out var expectedIncomplete);
                    Assert.That((lag, incomplete), Is.EqualTo((expected, expectedIncomplete)), $"step {step}, checkpoint {checkpoint}");
                }
            }
        }
    }

    [Test]
    public void The_append_timeline_records_and_answers_without_allocating_once_full()
    {
        var timeline = new WalAppendTimeline();
        var offset = 0L;
        for (; offset < 2 * WalAppendTimeline.Capacity; offset++)
            timeline.Record(offset, offset);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1_000; i++, offset++)
        {
            timeline.Record(offset, offset);
            timeline.GetLagTicks(offset - 100, offset + 1, out _);
            timeline.GetLagTicks(0, offset + 1, out _);
        }

        Assert.That(GC.GetAllocatedBytesForCurrentThread() - before, Is.Zero);
    }

    private static long? ReferenceLag(SortedDictionary<long, long> retained, long head, long checkpoint, long nowTicks, out bool incomplete)
    {
        incomplete = false;
        if (head < 0 || checkpoint > head)
        {
            incomplete = true;
            return null;
        }
        if (checkpoint == head)
            return 0;
        if (retained.TryGetValue(checkpoint + 1, out var appendedAt))
            return Math.Max(0, nowTicks - appendedAt);
        incomplete = true;
        foreach (var entry in retained)
        {
            if (entry.Key > checkpoint)
                return Math.Max(0, nowTicks - entry.Value);
        }
        return null;
    }
}
