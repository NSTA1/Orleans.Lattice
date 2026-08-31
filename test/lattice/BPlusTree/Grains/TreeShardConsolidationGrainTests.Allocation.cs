using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Allocation guards for the consolidation drain path.
/// <para>
/// The drain is the only part of a fold that scales with the donor's data, so
/// it is the only part where a per-entry allocation would matter: a
/// thousand-leaf vector shard holds hundreds of thousands of entries, and a
/// single stray allocation per entry would turn a background repair into GC
/// pressure on exactly the loaded deployment consolidation exists to heal.
/// </para>
/// <para>
/// The measurement is comparative rather than absolute, so it does not depend
/// on the allocation profile of the surrounding test harness or on GC timing:
/// two drains that differ only in how many entries the donor hands over -
/// with the batch large enough that both flush exactly once - must cost
/// essentially the same. A per-entry allocation would make the wide drain cost
/// hundreds of times more.
/// </para>
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    /// <summary>
    /// Slack allowed between the narrow and wide drains. A genuine per-entry
    /// allocation of even a single small object would add roughly two orders of
    /// magnitude more than this across the extra entries.
    /// </summary>
    private const long DrainAllocationSlackBytes = 8 * 1024;

    private static Dictionary<string, LwwValue<byte[]>> WideEntries(int count)
    {
        var entries = new Dictionary<string, LwwValue<byte[]>>(count);
        var wall = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        var payload = new byte[] { 1, 2, 3, 4 };
        for (var i = 0; i < count; i++)
        {
            var hlc = new HybridLogicalClock { WallClockTicks = wall, Counter = i };
            entries[string.Create(null, $"drain-key-{i:D6}")] = LwwValue<byte[]>.Create(payload, hlc);
        }
        return entries;
    }

    private static async Task<long> MeasureDrainAllocationAsync(int entryCount)
    {
        // The donor's delta is materialised before the measurement window so
        // only the coordinator's own work is counted; in production that
        // dictionary is the deserialised grain response, not the drain's doing.
        var entries = WideEntries(entryCount);

        var options = new LatticeOptions
        {
            // One flush for both sizes, so the comparison isolates per-entry
            // cost from per-batch cost.
            ConsolidationDrainBatchSize = 4096,
            ConsolidationDrainLeavesPerPass = 4,
        };

        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [entries],
            options: options);

        // Warm the path once so JIT and first-call substitute plumbing land
        // outside the measured window.
        await h.Grain.DrainAsync();

        var measured = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [entries],
            options: options);

        var before = GC.GetAllocatedBytesForCurrentThread();
        await measured.Grain.DrainAsync();
        var after = GC.GetAllocatedBytesForCurrentThread();

        Assert.That(measured.State.State.EntriesDrained, Is.EqualTo(entryCount),
            "The measured drain must actually have forwarded every entry.");

        return after - before;
    }

    [Test]
    public async Task Drain_does_not_allocate_per_entry()
    {
        const int narrowCount = 8;
        const int wideCount = 4000;

        var narrow = await MeasureDrainAllocationAsync(narrowCount);
        var wide = await MeasureDrainAllocationAsync(wideCount);

        var extraEntries = wideCount - narrowCount;
        var growth = wide - narrow;

        Assert.That(growth, Is.LessThan(DrainAllocationSlackBytes),
            $"Draining {extraEntries} extra entries allocated {growth} more bytes. The drain "
            + "must copy whole LwwValue records into a pre-sized batch dictionary that is "
            + "cleared rather than reallocated, so the marginal cost per entry is zero.");
    }

    [Test]
    public async Task Drain_batch_dictionary_is_reused_across_flushes()
    {
        // A batch dictionary reallocated per flush would show up as allocation
        // that scales with the flush count, so hold the entry count fixed and
        // vary only how many flushes it takes.
        var entries = WideEntries(2048);

        static async Task<long> MeasureAsync(Dictionary<string, LwwValue<byte[]>> entries, int batchSize)
        {
            var options = new LatticeOptions
            {
                ConsolidationDrainBatchSize = batchSize,
                ConsolidationDrainLeavesPerPass = 4,
            };

            var warm = CreateGrain(
                existingState: InFlightState(ShardConsolidationPhase.Drain),
                leafEntries: [entries], options: options);
            await warm.Grain.DrainAsync();

            var measured = CreateGrain(
                existingState: InFlightState(ShardConsolidationPhase.Drain),
                leafEntries: [entries], options: options);

            var before = GC.GetAllocatedBytesForCurrentThread();
            await measured.Grain.DrainAsync();
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        // A single flush at capacity 2048 versus 32 flushes at capacity 64.
        // The 64-capacity dictionary is far smaller, so if anything the
        // many-flush run should allocate less, never proportionally more.
        var oneFlush = await MeasureAsync(entries, 2048);
        var manyFlushes = await MeasureAsync(entries, 64);

        Assert.That(manyFlushes, Is.LessThan(oneFlush + DrainAllocationSlackBytes),
            $"32 flushes allocated {manyFlushes} bytes against {oneFlush} for one flush; "
            + "the batch dictionary must be cleared and reused, not rebuilt per flush.");
    }
}
