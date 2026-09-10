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

    // ---------------------------------------------------------------------------
    // #2621: an unstamped origin arrives as string.Empty, not null.
    //
    // DefaultLatticeOriginClusterIdResolver returns string.Empty for every tree on
    // a single-cluster host, so these are the values the DEFAULT local deployment
    // actually produces. Every pre-existing test above uses a non-empty cluster id
    // or null - never "" - which is why a deterministic production failure on the
    // common configuration passed a green suite.
    // ---------------------------------------------------------------------------

    [Test]
    public async Task StreamAsync_empty_origin_cluster_id_does_not_add_high_water()
    {
        // An empty origin must be treated as "unstamped", exactly as null is.
        // Before the fix the `is { }` guard admitted "", seeding a ""-keyed
        // high-water entry whose key later became BackupOriginProvenance.OriginId
        // and threw ArgumentException on every full capture of a local-only tree.
        var unstamped = new LwwEntry
        {
            Key = "k-empty-origin",
            Timestamp = new HybridLogicalClock { WallClockTicks = 999L },
            OriginClusterId = string.Empty,
        };

        var collector = await DrainAsync(unstamped);

        Assert.That(collector.PerOriginHighWater, Is.Empty);
    }

    [Test]
    public async Task StreamAsync_empty_origin_cluster_id_is_recorded_as_null_descriptor_origin()
    {
        // BackupKeyDescriptor.OriginId is documented as null for a single-origin
        // (local-only) tree. Before the fix the full-capture path wrote "" there
        // while the incremental path wrote null, so the same logical tree produced
        // two different manifests depending on which path captured it.
        var unstamped = new LwwEntry
        {
            Key = "k-empty-origin",
            Timestamp = new HybridLogicalClock { WallClockTicks = 5L },
            OriginClusterId = string.Empty,
        };

        var collector = await DrainAsync(unstamped);

        Assert.That(collector.KeyDescriptors, Has.Count.EqualTo(1));
        Assert.That(collector.KeyDescriptors[0].OriginId, Is.Null);
    }

    [Test]
    public async Task StreamAsync_counts_unstamped_origin_entries()
    {
        // The loud discriminator. An empty provenance is expected on a local-only
        // tree and alarming on a replicated one, and the manifest cannot tell them
        // apart. The count lets the capture state which case it observed instead of
        // emitting an unexplained absence.
        var empty = new LwwEntry
        {
            Key = "k-empty",
            Timestamp = new HybridLogicalClock { WallClockTicks = 1L },
            OriginClusterId = string.Empty,
        };
        var nul = new LwwEntry
        {
            Key = "k-null",
            Timestamp = new HybridLogicalClock { WallClockTicks = 2L },
            OriginClusterId = null,
        };
        var stamped = new LwwEntry
        {
            Key = "k-stamped",
            Timestamp = new HybridLogicalClock { WallClockTicks = 3L },
            OriginClusterId = "cluster-a",
        };

        var collector = await DrainAsync(empty, nul, stamped);

        Assert.Multiple(() =>
        {
            Assert.That(collector.UnstampedOriginEntryCount, Is.EqualTo(2));
            Assert.That(collector.PerOriginHighWater.Keys, Is.EquivalentTo(new[] { "cluster-a" }));
        });
    }

    [Test]
    public async Task Full_and_incremental_capture_agree_on_origin_normalization()
    {
        // The defect was not a wrong constant, it was a DIVERGENCE: the incremental
        // path already normalized empty to null and the full path did not, so only
        // the initial baseline failed. Pin the two paths together so they cannot
        // drift apart again silently.
        var full = await DrainAsync(new LwwEntry
        {
            Key = "k",
            Timestamp = new HybridLogicalClock { WallClockTicks = 7L },
            OriginClusterId = string.Empty,
        });

        var incremental = new IncrementalDeltaCollector(
            _serializer,
            Substitute.For<Orleans.Lattice.Wal.IWalSubscriber>(),
            treeId: "orders",
            consumerId: "test-consumer",
            partitions: 1,
            baseOffsets: new Dictionary<int, long>(),
            startInclusive: null,
            endExclusive: null,
            mergeMode: BackupKeyMergeMode.LastWriterWins,
            baseBackupId: "base-id",
            batchSize: 100);

        incremental.OnEntry(new Orleans.Lattice.Wal.WalSubscriptionEntry(0, 1L, new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = "k",
            Value = [1],
            Timestamp = new HybridLogicalClock { WallClockTicks = 7L },
            OriginClusterId = string.Empty,
        }));

        Assert.Multiple(() =>
        {
            Assert.That(
                full.PerOriginHighWater,
                Is.EquivalentTo(incremental.PerOriginHighWater),
                "full and incremental capture must agree on which origins are stamped");
            Assert.That(
                full.KeyDescriptors[0].OriginId,
                Is.EqualTo(incremental.KeyDescriptors[0].OriginId),
                "full and incremental capture must record the same descriptor origin");
        });
    }

    [Test]
    public void BuildProvenance_rejects_an_empty_origin_key_with_an_attributable_message()
    {
        // The guard that keeps this defect from ever recurring silently. Collectors
        // normalize, so an empty key can only mean a collector regression - and it
        // must NOT be skipped, because silently dropping a genuine origin from a
        // backup that then reports success is strictly worse than failing.
        //
        // Exercised by reflection because BuildProvenance is private and the guard
        // is unreachable through the public path by construction. An untested guard
        // is exactly the "measurand never exercised" shape this fixture exists to
        // close, so it is proved reachable here rather than assumed.
        var method = typeof(LatticeBackupCaptureService).GetMethod(
            "BuildProvenance",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        Assert.That(method, Is.Not.Null, "BuildProvenance was renamed; update this guard test.");

        var withEmptyKey = new Dictionary<string, long> { [string.Empty] = 5L };

        var ex = Assert.Throws<System.Reflection.TargetInvocationException>(
            () => method!.Invoke(null, [withEmptyKey]));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.InnerException, Is.TypeOf<InvalidOperationException>());
            Assert.That(
                ex.InnerException!.Message,
                Does.Contain("RawEntryCollector.RecordEntry").And.Contain("IncrementalDeltaCollector.OnEntry"),
                "the message must name the seams that must normalize, so a recurrence is attributable");
        });
    }

    [Test]
    public void BuildProvenance_accepts_a_normalized_non_empty_origin_key()
    {
        // Negative control: the guard must not reject the ordinary case. Without
        // this, a guard that threw unconditionally would still pass the test above.
        var method = typeof(LatticeBackupCaptureService).GetMethod(
            "BuildProvenance",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        var result = (IReadOnlyList<BackupOriginProvenance>)method!.Invoke(
            null,
            [new Dictionary<string, long> { ["cluster-a"] = 5L }])!;

        Assert.Multiple(() =>
        {
            Assert.That(result, Has.Count.EqualTo(1));
            Assert.That(result[0].OriginId, Is.EqualTo("cluster-a"));
            Assert.That(result[0].HighWaterSequence, Is.EqualTo(5L));
        });
    }
}
