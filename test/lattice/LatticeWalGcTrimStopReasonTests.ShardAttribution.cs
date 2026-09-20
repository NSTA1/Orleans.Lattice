using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Shard attribution on the trim-stop arm (issue #3207).
/// <para>
/// The arm was tree-scoped, and that made it unable to answer the question it
/// exists for. A stop is not on its own an indictment: a perfectly healthy
/// shard releases every entry it may and then stops at the first one it must
/// retain, so it publishes <c>offset_floor</c> exactly like a shard that has
/// never released an entry in its life. Summed to the tree the two are the
/// same reading, which is why a live estate could carry one tree trimming
/// thousands of entries per pass and another trimming none while both reported
/// the same arm.
/// </para>
/// <para>
/// The separation that matters is <c>trim_stop{shard}</c> advancing while that
/// same shard's <c>entries_trimmed{shard}</c> stays flat: asked on every pass,
/// releases nothing. It has to be built from these two arms because every
/// other arm the shard publishes is derived from the provider's dead-byte
/// accounting, and dead bytes only rise as a consequence of the release that
/// is not happening - so the wedged shard reports zero dead bytes, a zero dead
/// ratio, zero compactions and zero reclaimed bytes, which is the best score
/// available on all four. Reaching the compaction evaluation (the repair this
/// issue first shipped) makes that shard <i>asked</i>; it cannot make it
/// <i>visible</i>, because the quantity every threshold reads is the one the
/// stop prevents from ever being written.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcTrimStopReasonTests
{
    private const string TrimStopInstrument = "orleans.lattice.wal.gc.trim_stop";
    private const string EntriesTrimmedInstrument = "orleans.lattice.wal.entries_trimmed";

    private sealed record ShardStop(int Shard, string Reason, long Value);

    private sealed record ShardTrimmed(int Shard, long Value);

    private static async Task<(List<ShardStop> Stops, List<ShardTrimmed> Trimmed)> RunShardScopedAsync(
        LatticeWalGc sut)
    {
        var stops = new List<ShardStop>();
        var trimmed = new List<ShardTrimmed>();

        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            new[] { TrimStopInstrument, EntriesTrimmedInstrument },
            l => l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            {
                string? reason = null;
                var shard = -1;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason)
                    {
                        reason = tag.Value as string;
                    }
                    else if (tag.Key == LatticeMetrics.TagShard && tag.Value is int index)
                    {
                        shard = index;
                    }
                }

                if (string.Equals(instrument.Name, TrimStopInstrument, StringComparison.Ordinal))
                {
                    stops.Add(new ShardStop(shard, reason ?? "<untagged>", measurement));
                }
                else
                {
                    trimmed.Add(new ShardTrimmed(shard, measurement));
                }
            }));

        await sut.RunOnceAsync(Tree);
        return (stops, trimmed);
    }

    private static List<string> AdvancedOn(IEnumerable<ShardStop> stops, int shard) =>
        stops.Where(s => s.Shard == shard && s.Value > 0)
            .Select(static s => s.Reason)
            .OrderBy(static r => r, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// The attribution itself. Two shards in different states, and the arm has
    /// to say which shard is in which - the sibling fixture
    /// <c>RunOnceAsync_records_one_stop_per_shard_so_the_arms_sum_to_the_shard_count</c>
    /// can only observe that both arms advanced somewhere on the tree, which
    /// is satisfied equally by attributing them the wrong way round.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_attributes_each_trim_stop_to_the_shard_whose_scan_stopped()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);
        await provider.AppendBatchAsync(
            Tree, 1, new[] { Entry(5, Hlc(10)) }, CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 2, partitions: 2);

        var (stops, _) = await RunShardScopedAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(AdvancedOn(stops, 0), Is.EqualTo(new[] { "exhausted" }),
                "Shard 0 sits below the floor at offset 2 and is fully reclaimable, so its own arm - not "
                + "merely some arm on the tree - must be the healthy one.");
            Assert.That(AdvancedOn(stops, 1), Is.EqualTo(new[] { "offset_floor" }),
                "Shard 1 begins at offset 5, above the stranded floor. Naming the shard is the whole "
                + "repair: an operator cannot act on 'one of this tree's eight shards is held'.");
            Assert.That(stops.Select(static s => s.Shard), Is.All.GreaterThanOrEqualTo(0),
                "Every measurement carries a shard, including the primed zeros, so a stranded shard is "
                + "never reduced to an absent dimension.");
        });
    }

    /// <summary>
    /// The decisive test for issue #3207, and the one that reproduces the live
    /// estate in a fixture. Both shards stop on <c>offset_floor</c>; only one
    /// of them is wedged. Tree-scoped, they are one indistinguishable reading.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_separates_a_shard_that_releases_nothing_from_a_healthy_shard_that_also_stops()
    {
        // Shard 0 is the healthy estate: offsets 0..4 against a floor of 2, so
        // it releases 0..2 and then stops at 3 - a real trim, followed by a
        // legitimate refusal. Shard 1 is the wedged estate: its first entry is
        // already above the floor, so it releases nothing and has never
        // released anything. Both report offset_floor.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(11)), Entry(2, Hlc(12)), Entry(3, Hlc(13)), Entry(4, Hlc(14)) },
            CancellationToken.None);
        await provider.AppendBatchAsync(
            Tree,
            1,
            new[] { Entry(5, Hlc(10)), Entry(6, Hlc(11)), Entry(7, Hlc(12)) },
            CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 2, partitions: 2);

        var (stops, trimmed) = await RunShardScopedAsync(sut);

        long ReleasedBy(int shard) => trimmed.Where(t => t.Shard == shard).Sum(static t => t.Value);

        Assert.Multiple(() =>
        {
            Assert.That(AdvancedOn(stops, 0), Is.EqualTo(new[] { "offset_floor" }));
            Assert.That(AdvancedOn(stops, 1), Is.EqualTo(new[] { "offset_floor" }),
                "Both shards stop for the same reason, which is exactly why the reason alone cannot "
                + "discriminate them and why the tree-scoped arm could not either.");

            Assert.That(ReleasedBy(0), Is.EqualTo(3),
                "The healthy shard released offsets 0..2 before meeting the floor.");
            Assert.That(ReleasedBy(1), Is.Zero,
                "The wedged shard released nothing, and its dead-byte accounting can therefore never "
                + "rise, so no compaction threshold will ever observe it.");

            var wedged = stops
                .Where(s => s.Value > 0 && ReleasedBy(s.Shard) == 0)
                .Select(static s => s.Shard)
                .Distinct()
                .ToList();
            Assert.That(wedged, Is.EqualTo(new[] { 1 }),
                "'Stop arm advancing while this shard's entries-trimmed stays flat' must select exactly "
                + "the wedged shard. That join is the signal which does not read through the dead-byte "
                + "accounting, and it is only computable because both arms carry the shard.");
        });
    }

    /// <summary>
    /// Priming has to carry the shard too. Without it the wedged shard's arms
    /// are absent rather than zero on every pass that returns early, which is
    /// the same false silence the instrument was added to remove, reproduced
    /// one dimension down.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_primes_every_arm_on_every_shard_so_an_absent_series_is_never_a_flat_one()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 10, partitions: 3);

        var (stops, _) = await RunShardScopedAsync(sut);

        var armsPerShard = stops
            .GroupBy(static s => s.Shard)
            .OrderBy(static g => g.Key)
            .Select(g => (g.Key, Arms: g.Select(static s => s.Reason)
                .Distinct()
                .OrderBy(static r => r, StringComparer.Ordinal)
                .ToArray()))
            .ToList();

        var expected = new[] { "block_pin", "causal_frontier", "cursor_floor", "empty", "exhausted", "offset_floor" };

        Assert.Multiple(() =>
        {
            Assert.That(armsPerShard.Select(static a => a.Key), Is.EqualTo(new[] { 0, 1, 2 }),
                "Every partition in the configured range is primed, including the ones this silo may not "
                + "resolve a provider for, so 'not scanned here' reads as flat zeros rather than silence.");
            foreach (var (shard, arms) in armsPerShard)
            {
                Assert.That(arms, Is.EqualTo(expected), $"Shard {shard} must carry all six arms.");
            }
        });
    }
}
