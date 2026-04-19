using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for audit findings:
/// <list type="bullet">
///   <item>Bug #3 — <see cref="BPlusTree.Grains.BPlusLeafGrain.MergeEntriesAsync"/> and
///   <see cref="BPlusTree.Grains.BPlusLeafGrain.MergeManyAsync"/> must advance the local
///   <see cref="LeafNodeState.Clock"/> past the highest incoming timestamp, otherwise
///   a subsequent local <c>SetAsync</c> produces a stamp that still loses LWW against
///   the just-merged (possibly future-dated) entry, causing silent write loss.</item>
///   <item>Bug #2 — <see cref="BPlusTree.Grains.BPlusLeafGrain.CompactTombstonesAsync"/>
///   must not mark itself as "up-to-date" when tombstones were left behind because they
///   were still within the grace period. Doing so causes every subsequent pass to be
///   short-circuited, and the tombstones are never swept.</item>
/// </list>
/// </summary>
public partial class BPlusLeafGrainTests
{
    // --- Bug #3: HLC advancement on merge ---

    [Test]
    public async Task MergeEntries_advances_local_clock_past_incoming_max()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Simulate a remote entry stamped one hour in the future.
        var futureClock = new HybridLogicalClock
        {
            WallClockTicks = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromHours(1).Ticks,
            Counter = 0
        };

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("remote"), futureClock)
        });

        Assert.That(state.State.Clock, Is.GreaterThanOrEqualTo(futureClock),
            "Local clock must be advanced past the highest incoming timestamp.");
    }

    [Test]
    public async Task Set_after_MergeEntries_with_future_timestamp_wins_LWW()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var futureClock = new HybridLogicalClock
        {
            WallClockTicks = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromHours(1).Ticks,
            Counter = 0
        };

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("remote"), futureClock)
        });

        // A subsequent local write should be the latest value — but without
        // clock advancement, HLC.Tick returns a stamp lower than futureClock
        // and the local write is silently dropped by LWW.Merge.
        await grain.SetAsync("k", Encoding.UTF8.GetBytes("local"));

        var result = await grain.GetAsync("k");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("local"),
            "Local write issued after merging a future-dated entry must win.");
    }

    [Test]
    public async Task MergeMany_advances_local_clock_past_incoming_max()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var futureClock = new HybridLogicalClock
        {
            WallClockTicks = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromHours(1).Ticks,
            Counter = 0
        };

        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("remote"), futureClock)
        });

        Assert.That(state.State.Clock, Is.GreaterThanOrEqualTo(futureClock));
    }

    [Test]
    public async Task Set_after_MergeMany_with_future_timestamp_wins_LWW()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var futureClock = new HybridLogicalClock
        {
            WallClockTicks = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromHours(1).Ticks,
            Counter = 0
        };

        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("remote"), futureClock)
        });

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("local"));

        var result = await grain.GetAsync("k");
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("local"));
    }

    // --- Bug #2: compaction must not short-circuit when tombstones remain in-grace ---

    [Test]
    public async Task CompactTombstones_does_not_block_future_passes_when_tombstones_remain_in_grace()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Insert a tombstone stamped "now" — within a 1h grace window.
        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("k");

        // First pass: grace not yet elapsed — nothing removed.
        var removed1 = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));
        Assert.That(removed1, Is.EqualTo(0));
        Assert.That(state.State.Entries.ContainsKey("k"), Is.True,
            "Tombstone should still be present after an in-grace pass.");

        // Second pass with zero grace: the tombstone is now eligible.
        // Without the fix, the first pass stamped LastCompactionVersion = Version,
        // so this pass short-circuits and the tombstone is never reclaimed.
        var removed2 = await grain.CompactTombstonesAsync(TimeSpan.Zero);
        Assert.That(removed2, Is.EqualTo(1),
            "An eligible tombstone must be swept even if a prior in-grace pass ran.");
        Assert.That(state.State.Entries.ContainsKey("k"), Is.False);
    }
}
