using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- CompactTombstonesAsync ---

    [Test]
    public async Task CompactTombstones_removes_old_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Insert a tombstone with a very old timestamp.
        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["dead"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Version.Tick("test"); // ensure version advances past LastCompactionVersion

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(1));
        Assert.That(state.State.Entries.ContainsKey("dead"), Is.False);
    }

    [Test]
    public async Task CompactTombstones_keeps_recent_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Insert a tombstone with a very recent timestamp.
        var recentClock = new HybridLogicalClock
        {
            WallClockTicks = DateTimeOffset.UtcNow.Ticks,
            Counter = 0
        };
        state.State.Entries["recent"] = LwwValue<byte[]>.Tombstone(recentClock);

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(0));
        Assert.That(state.State.Entries.ContainsKey("recent"), Is.True);
    }

    [Test]
    public async Task CompactTombstones_does_not_remove_live_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["alive"] = LwwValue<byte[]>.Create(
            Encoding.UTF8.GetBytes("value"), oldClock);

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(0));
        Assert.That(state.State.Entries.ContainsKey("alive"), Is.True);
    }

    [Test]
    public async Task CompactTombstones_tracks_LastCompactionVersion()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.That(state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version), Is.True);
    }

    [Test]
    public async Task CompactTombstones_skips_scan_when_nothing_changed()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        // First compaction removes the tombstone.
        var removed1 = await grain.CompactTombstonesAsync(TimeSpan.Zero);
        Assert.That(removed1, Is.EqualTo(1));

        // Second compaction should be a no-op (version hasn't changed).
        var removed2 = await grain.CompactTombstonesAsync(TimeSpan.Zero);
        Assert.That(removed2, Is.EqualTo(0));
    }

    // --- TTL-expiry metric ---

    [Test]
    public async Task CompactTombstones_emits_LeafTombstonesExpired_for_ttl_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "ttl-metric-tree";
        var grain = CreateGrain(state);

        // Seed a live entry with an already-elapsed expiry in the far past.
        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["ttl-key"] = LwwValue<byte[]>.CreateWithExpiry(
            Encoding.UTF8.GetBytes("v"), oldClock, expiresAtTicks: 2);
        state.State.Version.Tick("test"); // bump version past LastCompactionVersion.

        var records = new List<KeyValuePair<string, object?>[]>();
        using var listener = new System.Diagnostics.Metrics.MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == "orleans.lattice.leaf.tombstones.expired")
                    l.EnableMeasurementEvents(inst);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (value <= 0) return;
            records.Add(tags.ToArray());
        });
        listener.Start();

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.EqualTo(1));
            Assert.That(records, Has.Count.EqualTo(1));
            Assert.That(records[0].Any(t =>
                t.Key == LatticeMetrics.TagTree && (t.Value as string) == "ttl-metric-tree"), Is.True);
        });
    }

    [Test]
    public async Task CompactTombstones_separates_explicit_tombstones_from_ttl_expiries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "ttl-split-tree";
        var grain = CreateGrain(state);

        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["dead"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Entries["expired"] = LwwValue<byte[]>.CreateWithExpiry(
            Encoding.UTF8.GetBytes("v"), oldClock, expiresAtTicks: 2);
        state.State.Version.Tick("test");

        long reaped = 0, expired = 0;
        using var listener = new System.Diagnostics.Metrics.MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && (inst.Name == "orleans.lattice.leaf.tombstones.reaped"
                     || inst.Name == "orleans.lattice.leaf.tombstones.expired"))
                    l.EnableMeasurementEvents(inst);
            }
        };
        listener.SetMeasurementEventCallback<long>((inst, value, _, _) =>
        {
            if (inst.Name == "orleans.lattice.leaf.tombstones.reaped") reaped += value;
            else if (inst.Name == "orleans.lattice.leaf.tombstones.expired") expired += value;
        });
        listener.Start();

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.EqualTo(2));
            Assert.That(reaped, Is.EqualTo(1), "explicit tombstone should count on reaped");
            Assert.That(expired, Is.EqualTo(1), "TTL-expired live entry should count on expired");
        });
    }
}
