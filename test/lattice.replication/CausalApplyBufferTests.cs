using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class CausalApplyBufferTests
{
    private const string Tree = "tree";
    private const string OriginA = "site-a";
    private const string OriginB = "site-b";
    private const string OriginC = "site-c";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static VersionVector Vc(params (string Origin, HybridLogicalClock Clock)[] entries)
    {
        var v = new VersionVector();
        foreach (var (o, c) in entries)
        {
            v.Entries[o] = c;
        }
        return v;
    }

    private static WalRecord Entry(
        string key,
        HybridLogicalClock ts,
        string origin = OriginB,
        VersionVector? vc = null,
        int valueSize = 1)
    {
        return new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[valueSize],
            Timestamp = ts,
            OriginClusterId = origin,
            VectorClock = vc,
        };
    }

    [Test]
    public void TryAdd_returns_added_when_buffer_empty()
    {
        var buffer = new CausalApplyBuffer();

        var outcome = buffer.TryAdd(Entry("k", Hlc(1)), maxEntries: 16, maxBytes: 1 << 20, out var evicted);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AddOutcome.Added));
            Assert.That(evicted, Is.Empty);
            Assert.That(buffer.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void TryAdd_returns_duplicate_for_same_identity_tuple()
    {
        var buffer = new CausalApplyBuffer();
        var entry = Entry("k", Hlc(1));
        buffer.TryAdd(entry, 16, 1 << 20, out _);

        var outcome = buffer.TryAdd(entry, 16, 1 << 20, out var evicted);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AddOutcome.Duplicate));
            Assert.That(evicted, Is.Empty);
            Assert.That(buffer.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void TryAdd_evicts_oldest_when_entry_cap_reached()
    {
        var buffer = new CausalApplyBuffer();
        buffer.TryAdd(Entry("a", Hlc(1)), 2, 1 << 20, out _);
        buffer.TryAdd(Entry("b", Hlc(2)), 2, 1 << 20, out _);

        var outcome = buffer.TryAdd(Entry("c", Hlc(3)), 2, 1 << 20, out var evicted);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AddOutcome.AddedWithEviction));
            Assert.That(evicted, Has.Count.EqualTo(1));
            Assert.That(evicted[0].Key, Is.EqualTo("a"));
            Assert.That(buffer.Count, Is.EqualTo(2));
        });
    }

    [Test]
    public void TryAdd_evicts_oldest_when_byte_cap_reached()
    {
        // Each entry is ~131 bytes (key=1ch×2 + value=1 + 128 overhead).
        // With a 300-byte cap, a+b coexist at 262, but adding c (which
        // would push to 393) must evict the head — a — first.
        var buffer = new CausalApplyBuffer();
        buffer.TryAdd(Entry("a", Hlc(1), valueSize: 1), 16, 300, out _);
        buffer.TryAdd(Entry("b", Hlc(2), valueSize: 1), 16, 300, out _);

        var outcome = buffer.TryAdd(Entry("c", Hlc(3), valueSize: 1), 16, 300, out var evicted);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AddOutcome.AddedWithEviction));
            Assert.That(evicted, Has.Count.EqualTo(1));
            Assert.That(evicted[0].Key, Is.EqualTo("a"));
            Assert.That(buffer.Count, Is.EqualTo(2));
        });
    }

    [Test]
    public void TryAdd_admits_oversize_entry_without_evicting_buffer()
    {
        var buffer = new CausalApplyBuffer();

        // A single entry larger than the cap is admitted as-is - the cap
        // is soft guidance, not a per-entry hard limit.
        var outcome = buffer.TryAdd(Entry("big", Hlc(1), valueSize: 4096), 16, maxBytes: 256, out var evicted);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AddOutcome.Added));
            Assert.That(evicted, Is.Empty);
            Assert.That(buffer.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void DrainSatisfied_returns_entries_in_fifo_order()
    {
        var buffer = new CausalApplyBuffer();
        buffer.TryAdd(Entry("first", Hlc(10), vc: Vc((OriginA, Hlc(5)))), 16, 1 << 20, out _);
        buffer.TryAdd(Entry("second", Hlc(11), vc: Vc((OriginA, Hlc(5)))), 16, 1 << 20, out _);

        var ready = buffer.DrainSatisfied(Vc((OriginA, Hlc(5))));

        Assert.Multiple(() =>
        {
            Assert.That(ready.Select(e => e.Key), Is.EqualTo(new[] { "first", "second" }));
            Assert.That(buffer.Count, Is.EqualTo(0));
        });
    }

    [Test]
    public void DrainSatisfied_leaves_unsatisfied_entries_parked()
    {
        var buffer = new CausalApplyBuffer();
        buffer.TryAdd(Entry("hi", Hlc(10), vc: Vc((OriginA, Hlc(5)))), 16, 1 << 20, out _);
        buffer.TryAdd(Entry("hi2", Hlc(11), vc: Vc((OriginA, Hlc(50)))), 16, 1 << 20, out _);

        var ready = buffer.DrainSatisfied(Vc((OriginA, Hlc(5))));

        Assert.Multiple(() =>
        {
            Assert.That(ready, Has.Count.EqualTo(1));
            Assert.That(ready[0].Key, Is.EqualTo("hi"));
            Assert.That(buffer.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void DependenciesSatisfied_returns_true_for_null_vector_clock()
    {
        var entry = Entry("k", Hlc(1), vc: null);

        Assert.That(CausalApplyBuffer.DependenciesSatisfied(entry, new VersionVector()), Is.True);
    }

    [Test]
    public void DependenciesSatisfied_returns_true_for_empty_vector_clock()
    {
        var entry = Entry("k", Hlc(1), vc: new VersionVector());

        Assert.That(CausalApplyBuffer.DependenciesSatisfied(entry, new VersionVector()), Is.True);
    }

    [Test]
    public void DependenciesSatisfied_skips_entrys_own_origin_diagonal()
    {
        // Entry's VC carries its own origin's HLC; the per-origin HWM
        // table is the authoritative dedup key for that component, so
        // the dep-check must not require localVc to dominate the
        // diagonal - that would deadlock the very entry we're applying.
        var entry = Entry("k", Hlc(100), origin: OriginB, vc: Vc((OriginB, Hlc(100))));

        Assert.That(CausalApplyBuffer.DependenciesSatisfied(entry, new VersionVector()), Is.True);
    }

    [Test]
    public void DependenciesSatisfied_returns_false_when_dep_origin_unknown_locally()
    {
        var entry = Entry("k", Hlc(1), origin: OriginB, vc: Vc((OriginC, Hlc(50))));

        Assert.That(CausalApplyBuffer.DependenciesSatisfied(entry, new VersionVector()), Is.False);
    }

    [Test]
    public void DependenciesSatisfied_returns_false_when_dep_origin_below_required_tick()
    {
        var entry = Entry("k", Hlc(1), origin: OriginB, vc: Vc((OriginC, Hlc(50))));
        var local = Vc((OriginC, Hlc(20)));

        Assert.That(CausalApplyBuffer.DependenciesSatisfied(entry, local), Is.False);
    }

    [Test]
    public void DependenciesSatisfied_returns_true_when_local_dominates()
    {
        var entry = Entry("k", Hlc(1), origin: OriginB, vc: Vc((OriginC, Hlc(50))));
        var local = Vc((OriginC, Hlc(100)));

        Assert.That(CausalApplyBuffer.DependenciesSatisfied(entry, local), Is.True);
    }

    [Test]
    public void DependenciesSatisfied_throws_when_local_vc_is_null()
    {
        var entry = Entry("k", Hlc(1), vc: Vc((OriginC, Hlc(50))));

        Assert.That(
            () => CausalApplyBuffer.DependenciesSatisfied(entry, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void TotalBytes_tracks_admitted_and_evicted_entries()
    {
        var buffer = new CausalApplyBuffer();
        buffer.TryAdd(Entry("a", Hlc(1), valueSize: 64), 16, 1 << 20, out _);
        var afterFirst = buffer.TotalBytes;
        buffer.TryAdd(Entry("b", Hlc(2), valueSize: 64), 16, 1 << 20, out _);
        var afterSecond = buffer.TotalBytes;

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.GreaterThan(0));
            Assert.That(afterSecond, Is.GreaterThan(afterFirst));
        });

        var ready = buffer.DrainSatisfied(new VersionVector());
        Assert.Multiple(() =>
        {
            Assert.That(ready, Has.Count.EqualTo(2));
            Assert.That(buffer.TotalBytes, Is.EqualTo(0));
        });
    }

    [Test]
    public void TryAdd_records_park_increments_violations_buffered_entries_and_buffer_bytes()
    {
        var buffer = new CausalApplyBuffer(Tree);

        using var blocked = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyCausalViolationsBlockedName);
        using var bufferedEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyBufferedEntriesName);
        using var bufferBytes = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyBufferBytesName);

        buffer.TryAdd(Entry("k", Hlc(1), valueSize: 10), 16, 1 << 20, out _);

        Assert.Multiple(() =>
        {
            Assert.That(blocked.Measurements.Sum(m => m.Value), Is.EqualTo(1L));
            Assert.That(bufferedEntries.Measurements.Sum(m => m.Value), Is.EqualTo(1L));
            Assert.That(bufferBytes.Measurements.Sum(m => m.Value), Is.GreaterThan(0L));
            Assert.That(blocked.Measurements.Single().Tags,
                Has.Some.Matches<KeyValuePair<string, object?>>(t => t.Key == "tree" && (string?)t.Value == Tree));
            Assert.That(bufferedEntries.Measurements.Single().Tags,
                Has.Some.Matches<KeyValuePair<string, object?>>(t => t.Key == "shard" && (string?)t.Value == "0"));
        });
    }

    [Test]
    public void TryAdd_eviction_decrements_buffered_entries_and_buffer_bytes()
    {
        var buffer = new CausalApplyBuffer(Tree);
        buffer.TryAdd(Entry("a", Hlc(1)), 2, 1 << 20, out _);
        buffer.TryAdd(Entry("b", Hlc(2)), 2, 1 << 20, out _);

        using var bufferedEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyBufferedEntriesName);
        using var bufferBytes = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyBufferBytesName);

        // Adding a third entry forces FIFO eviction of the first.
        buffer.TryAdd(Entry("c", Hlc(3)), 2, 1 << 20, out var evicted);

        // Net effect: +1 admit, -1 evict on entries → 0 net.
        Assert.Multiple(() =>
        {
            Assert.That(evicted, Has.Count.EqualTo(1));
            Assert.That(bufferedEntries.Measurements.Sum(m => m.Value), Is.EqualTo(0L));
            // Bytes net is non-zero (a's size != c's size in general but here both are similar).
            // We assert that *both* an admit increment and an evict decrement were emitted.
            Assert.That(bufferedEntries.Measurements.Any(m => m.Value > 0), Is.True);
            Assert.That(bufferedEntries.Measurements.Any(m => m.Value < 0), Is.True);
            Assert.That(bufferBytes.Measurements.Any(m => m.Value > 0), Is.True);
            Assert.That(bufferBytes.Measurements.Any(m => m.Value < 0), Is.True);
        });
    }

    [Test]
    public void TryAdd_duplicate_does_not_emit_violations_or_buffer_metrics()
    {
        var buffer = new CausalApplyBuffer(Tree);
        var entry = Entry("k", Hlc(1));
        buffer.TryAdd(entry, 16, 1 << 20, out _);

        using var blocked = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyCausalViolationsBlockedName);
        using var bufferedEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyBufferedEntriesName);

        var outcome = buffer.TryAdd(entry, 16, 1 << 20, out _);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AddOutcome.Duplicate));
            Assert.That(blocked.Measurements, Is.Empty);
            Assert.That(bufferedEntries.Measurements, Is.Empty);
        });
    }

    [Test]
    public void DrainSatisfied_records_dependency_wait_ms_and_decrements_gauges()
    {
        var buffer = new CausalApplyBuffer(Tree);
        buffer.TryAdd(Entry("first", Hlc(10), vc: Vc((OriginA, Hlc(5)))), 16, 1 << 20, out _);
        buffer.TryAdd(Entry("second", Hlc(11), vc: Vc((OriginA, Hlc(5)))), 16, 1 << 20, out _);

        using var bufferedEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyBufferedEntriesName);
        using var bufferBytes = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyBufferBytesName);
        using var waits = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDependencyWaitMsName);

        var ready = buffer.DrainSatisfied(Vc((OriginA, Hlc(5))));

        Assert.Multiple(() =>
        {
            Assert.That(ready, Has.Count.EqualTo(2));
            // One aggregate decrement for both drained entries: -2.
            Assert.That(bufferedEntries.Measurements.Sum(m => m.Value), Is.EqualTo(-2L));
            Assert.That(bufferBytes.Measurements.Sum(m => m.Value), Is.LessThan(0L));
            // Two wait samples (one per drained entry).
            Assert.That(waits.Measurements, Has.Count.EqualTo(2));
            Assert.That(waits.Measurements.All(m => m.Value >= 0.0), Is.True);
            Assert.That(waits.Measurements.First().Tags,
                Has.Some.Matches<KeyValuePair<string, object?>>(t => t.Key == "tree" && (string?)t.Value == Tree));
        });
    }

    [Test]
    public void DrainSatisfied_with_no_satisfied_entries_emits_no_metrics()
    {
        var buffer = new CausalApplyBuffer(Tree);
        buffer.TryAdd(Entry("blocked", Hlc(10), vc: Vc((OriginA, Hlc(50)))), 16, 1 << 20, out _);

        using var bufferedEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyBufferedEntriesName);
        using var waits = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDependencyWaitMsName);

        var ready = buffer.DrainSatisfied(Vc((OriginA, Hlc(5))));

        Assert.Multiple(() =>
        {
            Assert.That(ready, Is.Empty);
            Assert.That(bufferedEntries.Measurements, Is.Empty);
            Assert.That(waits.Measurements, Is.Empty);
        });
    }
}
