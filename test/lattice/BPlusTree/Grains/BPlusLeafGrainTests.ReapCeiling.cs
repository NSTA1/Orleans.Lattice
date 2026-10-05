using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- CompactTombstonesBelowAsync (issue #4615) ---

    [Test]
    public async Task CompactTombstonesBelow_reaps_only_entries_stamped_below_the_ceiling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var below = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };
        var ceiling = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };
        var atOrAbove = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };
        grain.EntriesForTest["below"] = LwwValue<byte[]>.Tombstone(below);
        grain.EntriesForTest["at-ceiling"] = LwwValue<byte[]>.Tombstone(atOrAbove);
        grain.EntriesForTest["expired-at-ceiling"] = LwwValue<byte[]>.CreateWithExpiry([1], atOrAbove, expiresAtTicks: 1);
        state.State.Version.Tick("test");

        var result = await grain.CompactTombstonesBelowAsync(TimeSpan.FromHours(1), ceiling);

        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesRemoved, Is.EqualTo(1));
            Assert.That(grain.EntriesForTest.ContainsKey("below"), Is.False, "past the grace period and below the ceiling");
            Assert.That(grain.EntriesForTest.ContainsKey("at-ceiling"), Is.True,
                "a write the tombstone beats may still be delivered, so it is kept");
            Assert.That(grain.EntriesForTest.ContainsKey("expired-at-ceiling"), Is.True,
                "an expired entry at the ceiling is kept for the same reason");
            Assert.That(state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version), Is.False,
                "a kept entry counts as inside the grace window, so a later pass re-scans the leaf");
        });
    }

    [Test]
    public async Task CompactTombstonesBelow_reaps_a_kept_tombstone_once_the_ceiling_passes_it()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var stamp = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };
        grain.EntriesForTest["dead"] = LwwValue<byte[]>.Tombstone(stamp);
        state.State.Version.Tick("test");

        var first = await grain.CompactTombstonesBelowAsync(TimeSpan.FromHours(1), HybridLogicalClock.Zero);
        var second = await grain.CompactTombstonesBelowAsync(
            TimeSpan.FromHours(1), new HybridLogicalClock { WallClockTicks = 11, Counter = 0 });

        Assert.Multiple(() =>
        {
            Assert.That(first.EntriesRemoved, Is.EqualTo(0), "a zero ceiling reaps nothing");
            Assert.That(second.EntriesRemoved, Is.EqualTo(1), "the re-scan reaps it once the ceiling passes it");
            Assert.That(grain.EntriesForTest.ContainsKey("dead"), Is.False);
        });
    }
}
