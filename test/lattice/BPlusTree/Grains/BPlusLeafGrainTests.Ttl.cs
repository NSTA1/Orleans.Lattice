using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- TTL / ExpiresAtTicks ---

    private static long FutureTicks(TimeSpan offset) => DateTimeOffset.UtcNow.Add(offset).UtcTicks;
    private static long PastTicks(TimeSpan offset) => DateTimeOffset.UtcNow.Subtract(offset).UtcTicks;

    [Test]
    public async Task Set_with_expiry_persists_ExpiresAtTicks()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var expiresAt = FutureTicks(TimeSpan.FromHours(1));

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"), expiresAt);

        Assert.That(state.State.Entries["k"].ExpiresAtTicks, Is.EqualTo(expiresAt));
    }

    [Test]
    public async Task Set_with_zero_expiry_is_equivalent_to_non_expiring_write()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"), expiresAtTicks: 0L);

        Assert.That(state.State.Entries["k"].ExpiresAtTicks, Is.EqualTo(0L));
        Assert.That(await grain.GetAsync("k"), Is.Not.Null);
    }

    [Test]
    public async Task Get_hides_expired_entry()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"), PastTicks(TimeSpan.FromMinutes(1)));

        Assert.That(await grain.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task GetWithVersion_hides_expired_entry()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"), PastTicks(TimeSpan.FromMinutes(1)));

        var result = await grain.GetWithVersionAsync("k");
        Assert.That(result.Value, Is.Null);
        Assert.That(result.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task Exists_returns_false_for_expired_entry()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"), PastTicks(TimeSpan.FromMinutes(1)));

        Assert.That(await grain.ExistsAsync("k"), Is.False);
    }

    [Test]
    public async Task GetMany_omits_expired_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("live", Encoding.UTF8.GetBytes("v1"), FutureTicks(TimeSpan.FromHours(1)));
        await grain.SetAsync("dead", Encoding.UTF8.GetBytes("v2"), PastTicks(TimeSpan.FromMinutes(1)));

        var result = await grain.GetManyAsync(["live", "dead"]);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result.ContainsKey("live"), Is.True);
        Assert.That(result.ContainsKey("dead"), Is.False);
    }

    [Test]
    public async Task Count_excludes_expired_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("v"), FutureTicks(TimeSpan.FromHours(1)));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("v"), PastTicks(TimeSpan.FromMinutes(1)));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("v"));

        Assert.That(await grain.CountAsync(), Is.EqualTo(2));
    }

    [Test]
    public async Task GetKeys_omits_expired_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("v"), PastTicks(TimeSpan.FromMinutes(1)));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("v"));

        var keys = await grain.GetKeysAsync();

        Assert.That(keys, Is.EqualTo(new[] { "a", "c" }));
    }

    [Test]
    public async Task GetEntries_omits_expired_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("va"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("vb"), PastTicks(TimeSpan.FromMinutes(1)));

        var entries = await grain.GetEntriesAsync();

        Assert.That(entries, Has.Count.EqualTo(1));
        Assert.That(entries[0].Key, Is.EqualTo("a"));
    }

    [Test]
    public async Task GetLiveEntries_omits_expired_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("va"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("vb"), PastTicks(TimeSpan.FromMinutes(1)));

        var live = await grain.GetLiveEntriesAsync();

        Assert.That(live, Has.Count.EqualTo(1));
        Assert.That(live.ContainsKey("a"), Is.True);
    }

    [Test]
    public async Task DeleteRange_skips_expired_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("va"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("vb"), PastTicks(TimeSpan.FromMinutes(1)));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("vc"));

        var result = await grain.DeleteRangeAsync("a", "z");

        Assert.That(result.Deleted, Is.EqualTo(2));
    }

    [Test]
    public async Task GetOrSet_overwrites_expired_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("old"), PastTicks(TimeSpan.FromMinutes(1)));

        var result = await grain.GetOrSetAsync("k", Encoding.UTF8.GetBytes("new"));

        Assert.That(result.ExistingValue, Is.Null);
        Assert.That(await grain.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("new")));
    }

    [Test]
    public async Task SetIfVersion_with_Zero_succeeds_on_expired_entry()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("old"), PastTicks(TimeSpan.FromMinutes(1)));

        var cas = await grain.SetIfVersionAsync("k", Encoding.UTF8.GetBytes("new"), HybridLogicalClock.Zero);

        Assert.That(cas.Success, Is.True);
        Assert.That(await grain.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("new")));
    }

    [Test]
    public async Task CompactTombstones_reaps_entries_expired_past_grace()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var expiredTicks = PastTicks(TimeSpan.FromHours(1));
        state.State.Entries["dead"] = LwwValue<byte[]>.CreateWithExpiry(
            Encoding.UTF8.GetBytes("v"),
            new HybridLogicalClock { WallClockTicks = expiredTicks, Counter = 0 },
            expiredTicks);
        state.State.Version.Tick("test");

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromMinutes(1));

        Assert.That(removed, Is.EqualTo(1));
        Assert.That(state.State.Entries.ContainsKey("dead"), Is.False);
    }

    [Test]
    public async Task CompactTombstones_keeps_expired_entry_within_grace()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var expiredTicks = PastTicks(TimeSpan.FromSeconds(10));
        state.State.Entries["recent"] = LwwValue<byte[]>.CreateWithExpiry(
            Encoding.UTF8.GetBytes("v"),
            new HybridLogicalClock { WallClockTicks = expiredTicks, Counter = 0 },
            expiredTicks);
        state.State.Version.Tick("test");

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(0));
        Assert.That(state.State.Entries.ContainsKey("recent"), Is.True);
    }

    [Test]
    public async Task CompactTombstones_keeps_unexpired_entry()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"), FutureTicks(TimeSpan.FromHours(1)));

        var removed = await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.That(removed, Is.EqualTo(0));
        Assert.That(state.State.Entries.ContainsKey("k"), Is.True);
    }
}
