using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for the per-origin causal-high-water accounting inside
/// <see cref="RawEntryCollector.StreamAsync"/>. These cover lines 113-120:
/// the tick-extraction and clamping path (negative ticks are silenced to zero),
/// the first-write path (no prior high-water for the origin), and the
/// non-update path (a lower-tick entry does not replace the existing high-water).
/// The tests create a real <see cref="Orleans.Serialization.Serializer"/> because
/// <see cref="RawEntryCollector"/> serializes entries through it; the cursor grain
/// is substituted.
/// </summary>
[TestFixture]
public sealed class RawEntryCollectorTests
{
    private ServiceProvider _services = null!;
    private Orleans.Serialization.Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Orleans.Serialization.Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    /// <summary>
    /// Drains one page (with the supplied entries) from a fake cursor, then a
    /// terminal empty page that ends the stream, and returns the collector.
    /// </summary>
    private async Task<RawEntryCollector> DrainAsync(params LwwEntry[] entries)
    {
        var page = new LatticeCursorRawEntriesPage
        {
            Entries = entries,
            HasMore = false,
        };

        var cursor = Substitute.For<ILatticeCursorGrain>();
        cursor.NextRawEntriesAsync(Arg.Any<int>()).Returns(Task.FromResult(page));

        var collector = new RawEntryCollector(_serializer, BackupKeyMergeMode.LastWriterWins);

        await foreach (var _ in collector.StreamAsync(cursor, 100, CancellationToken.None))
        {
            // Drain the stream to trigger all RecordEntry calls.
        }

        return collector;
    }

    [Test]
    public async Task StreamAsync_negative_origin_ticks_are_clamped_to_zero()
    {
        // Lines 113-116: when entry.Timestamp.WallClockTicks < 0, the ticks value
        // used for the high-water comparison is clamped to 0.
        var entry = new LwwEntry
        {
            Key = "k1",
            Timestamp = new HybridLogicalClock { WallClockTicks = -100L },
            OriginClusterId = "cluster-a",
        };

        var collector = await DrainAsync(entry);

        // Clamped to 0 and stored as the high-water.
        Assert.That(collector.PerOriginHighWater["cluster-a"], Is.EqualTo(0L));
    }

    [Test]
    public async Task StreamAsync_positive_ticks_are_stored_as_origin_high_water()
    {
        // Lines 113, 118-120: a positive tick value is stored directly as the
        // high-water mark for its origin cluster.
        var entry = new LwwEntry
        {
            Key = "k2",
            Timestamp = new HybridLogicalClock { WallClockTicks = 42L },
            OriginClusterId = "cluster-b",
        };

        var collector = await DrainAsync(entry);

        Assert.That(collector.PerOriginHighWater["cluster-b"], Is.EqualTo(42L));
    }

    [Test]
    public async Task StreamAsync_lower_ticks_do_not_replace_higher_high_water()
    {
        // Line 118 (else branch): when a later entry for the same origin has ticks
        // <= the stored high-water, the stored value must not be replaced.
        var high = new LwwEntry
        {
            Key = "k-high",
            Timestamp = new HybridLogicalClock { WallClockTicks = 100L },
            OriginClusterId = "cluster-c",
        };
        var low = new LwwEntry
        {
            Key = "k-low",
            Timestamp = new HybridLogicalClock { WallClockTicks = 10L },
            OriginClusterId = "cluster-c",
        };

        var collector = await DrainAsync(high, low);

        Assert.That(collector.PerOriginHighWater["cluster-c"], Is.EqualTo(100L));
    }

    [Test]
    public async Task StreamAsync_entry_without_origin_cluster_id_does_not_add_high_water()
    {
        // Line 111: entries with OriginClusterId == null are not added to the
        // per-origin high-water dictionary. Verify the positive case does not bleed
        // into an entry without an origin.
        var noOrigin = new LwwEntry
        {
            Key = "k-no-origin",
            Timestamp = new HybridLogicalClock { WallClockTicks = 999L },
            OriginClusterId = null,
        };

        var collector = await DrainAsync(noOrigin);

        Assert.That(collector.PerOriginHighWater, Is.Empty);
    }
}
