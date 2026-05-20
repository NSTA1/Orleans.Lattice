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

    // --- WAL routing: tombstone-reap envelopes ---

    [Test]
    public async Task CompactTombstones_appends_one_WAL_envelope_per_reaped_entry()
    {
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        // Seed two old tombstones and one expired live entry, all
        // beyond the grace cutoff.
        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["dead-a"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Entries["dead-b"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Entries["expired"] = LwwValue<byte[]>.CreateWithExpiry(
            Encoding.UTF8.GetBytes("v"), oldClock, expiresAtTicks: 2);
        state.State.Version.Tick("test");

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(3));
        Assert.That(commitLog.AppendCount, Is.EqualTo(3),
            "every reaped entry must emit a tombstone-reap WAL envelope so a reactivated leaf "
            + "observes the compacted state after replay returns.");
        Assert.That(commitLog.Appended.All(m => m.Op == MutationKind.Tombstone), Is.True);
        Assert.That(commitLog.Appended.All(m => m.IsMerge), Is.True,
            "tombstone-reap envelopes are tagged IsMerge=true to keep them out of ordinary "
            + "Set / Delete telemetry rollups");
        Assert.That(commitLog.Appended.All(m => m.IsTombstone), Is.True);
    }

    [Test]
    public async Task CompactTombstones_does_not_call_state_write_state_async()
    {
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["dead"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Version.Tick("test");
        var writeCountBefore = state.WriteCount;

        await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore),
            "CompactTombstonesAsync must route durability through the WAL; the legacy "
            + "state.WriteStateAsync() call site is gone now that compaction is WAL-routed.");
    }

    [Test]
    public async Task CompactTombstones_does_not_append_when_nothing_in_grace_window()
    {
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        // Recent tombstone within grace - must not be reaped or appended.
        var recentClock = new HybridLogicalClock
        {
            WallClockTicks = DateTimeOffset.UtcNow.Ticks,
            Counter = 0,
        };
        state.State.Entries["recent"] = LwwValue<byte[]>.Tombstone(recentClock);
        state.State.Version.Tick("test");

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(0));
        Assert.That(commitLog.AppendCount, Is.EqualTo(0),
            "a compaction pass that reaps nothing must not append a stray WAL envelope");
    }

    [Test]
    public async Task CompactTombstones_stamps_maintenance_category_on_emitted_envelopes()
    {
        // The reap envelope's `LatticeMutation.Category` must be
        // `MutationCategory.Maintenance` independently of whether the
        // caller wrapped the leaf invocation in a maintenance scope. The
        // grain opens its own `LatticeMaintenanceContext.BeginScope()`
        // so a direct test or any future leaf-level call site stamps
        // the WAL envelope correctly, which is the signal the producer-
        // side replication shipper uses to short-circuit shipping.
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["dead-a"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Entries["dead-b"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Version.Tick("test");

        await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(commitLog.AppendCount, Is.EqualTo(2));
        Assert.That(commitLog.Appended.All(m => m.Category == MutationCategory.Maintenance), Is.True,
            "compaction-emitted WAL envelopes must carry Category=Maintenance so "
            + "replication and observer paths can classify them as structural rewrites.");
    }

    [Test]
    public async Task CompactTombstones_uses_tombstone_kind_and_is_merge_flag()
    {
        // Hardens the wire-shape contract that the producer-side
        // tombstone filter in `ReplicationShipperGrain.ShouldShip` and
        // `ChangeFeed.Subscribe` keys on (`Op == MutationKind.Tombstone`).
        // A regression that flipped the kind back to `Delete` would
        // pass merge / delete tests but silently break the receiver-
        // side dead-letter avoidance for compaction.
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["dead"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Version.Tick("test");

        await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(commitLog.AppendCount, Is.EqualTo(1));
        Assert.That(commitLog.Appended[0].Op, Is.EqualTo(MutationKind.Tombstone));
        Assert.That(commitLog.Appended[0].IsMerge, Is.True);
        Assert.That(commitLog.Appended[0].IsTombstone, Is.True);
    }
}
