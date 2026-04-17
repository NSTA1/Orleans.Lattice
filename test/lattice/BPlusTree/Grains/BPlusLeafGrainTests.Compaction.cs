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

    [Test]
    public async Task CompactTombstones_returns_count_of_removed_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["a"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Entries["b"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Entries["c"] = LwwValue<byte[]>.Create(
            Encoding.UTF8.GetBytes("live"), oldClock);
        state.State.Version.Tick("test"); // ensure version advances past LastCompactionVersion

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(2));
        Assert.That(state.State.Entries, Has.Count.EqualTo(1));
        Assert.That(state.State.Entries.ContainsKey("c"), Is.True);
    }
}
