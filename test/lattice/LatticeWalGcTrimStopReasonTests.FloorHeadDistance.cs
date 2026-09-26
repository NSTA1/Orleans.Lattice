using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// The floor-to-head reclaimable distance, published per (tree, shard) by
/// <see cref="LatticeMetrics.WalGcFloorHeadDistance"/> (issue #3149).
/// <para>
/// <c>trim_stop{shard}</c> says a scan stopped and <c>entries_trimmed{shard}</c>
/// says what it released before stopping, but neither says how much of the
/// shard the floor it stopped at is holding. These tests pin that the distance
/// is the stopped scan's first retained offset through the shard's head, that
/// it is attributed to the shard which measured it, and that a shard with
/// nothing held publishes a measured zero rather than no series.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcTrimStopReasonTests
{
    private const string FloorHeadDistanceInstrument = "orleans.lattice.wal.gc.floor_head_distance";

    private sealed record ShardDistance(int Shard, long Value, string[] TagKeys);

    private static async Task<(List<ShardDistance> Distances, List<string[]> TrimmedTagKeys)> RunDistanceAsync(
        LatticeWalGc sut)
    {
        var distances = new List<ShardDistance>();
        var trimmedTagKeys = new List<string[]>();

        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            new[] { FloorHeadDistanceInstrument, EntriesTrimmedInstrument },
            l => l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            {
                var shard = -1;
                var keys = new string[tags.Length];
                for (var i = 0; i < tags.Length; i++)
                {
                    keys[i] = tags[i].Key;
                    if (tags[i].Key == LatticeMetrics.TagShard && tags[i].Value is int index)
                    {
                        shard = index;
                    }
                }

                Array.Sort(keys, StringComparer.Ordinal);
                if (string.Equals(instrument.Name, FloorHeadDistanceInstrument, StringComparison.Ordinal))
                {
                    distances.Add(new ShardDistance(shard, measurement, keys));
                }
                else
                {
                    trimmedTagKeys.Add(keys);
                }
            }));

        await sut.RunOnceAsync(Tree);
        return (distances, trimmedTagKeys);
    }

    /// <summary>
    /// The distance itself, on two shards held by the same floor to different
    /// depths. Shard 0 holds offsets 0..4 against a floor of 2, releases 0..2
    /// and retains 3..4; shard 1 begins at 5 and retains all of 5..7.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_publishes_the_retained_span_from_the_floor_to_the_head_on_each_stopped_shard()
    {
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

        var (distances, _) = await RunDistanceAsync(sut);

        Assert.That(
            distances.OrderBy(static d => d.Shard).Select(static d => (d.Shard, d.Value)),
            Is.EqualTo(new[] { (0, 2L), (1, 3L) }),
            "Each stopped shard publishes exactly one reading, and it is the span its floor holds: "
            + "offsets 3..4 on shard 0 and 5..7 on shard 1. Shard 1 has released nothing, so its "
            + "distance is its whole log - the reading that makes a stranded shard's size visible.");
    }

    /// <summary>
    /// A shard whose floor holds nothing publishes a measured zero - both when
    /// its scan released everything it was offered and when it was empty - so
    /// "the floor covers nothing" is a reading rather than an absent series.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_publishes_a_measured_zero_for_an_exhausted_shard_and_an_empty_shard()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(10)), Entry(1, Hlc(11)) }, CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 10, partitions: 2);

        var (distances, _) = await RunDistanceAsync(sut);

        Assert.That(
            distances.OrderBy(static d => d.Shard).Select(static d => (d.Shard, d.Value)),
            Is.EqualTo(new[] { (0, 0L), (1, 0L) }),
            "Shard 0 released its whole log and shard 1 has none, so neither floor holds anything; "
            + "both must still publish, or a quiet shard is indistinguishable from one not reporting.");
    }

    /// <summary>
    /// The distance carries the same tag set as <c>entries_trimmed</c>, so the
    /// retained side of a scan joins its released side shard for shard.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_tags_the_distance_with_the_same_set_as_entries_trimmed()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(3, Hlc(10)) }, CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 2, partitions: 1);

        var (distances, trimmedTagKeys) = await RunDistanceAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(distances, Has.Count.EqualTo(1));
            Assert.That(trimmedTagKeys, Is.Not.Empty);
            Assert.That(distances[0].TagKeys, Is.EqualTo(trimmedTagKeys[^1]),
                "A join on (tree, shard, tenant) needs both instruments to carry exactly that set.");
            Assert.That(distances[0].TagKeys,
                Is.EquivalentTo(new[] { LatticeMetrics.TagTree, LatticeMetrics.TagShard, LatticeTenantLabel.TagTenant }));
        });
    }
}
