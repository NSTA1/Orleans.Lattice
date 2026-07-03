using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the payload-eviction delegation path added for the bounded
/// read-through cache (<see cref="LatticeOptions.MaxCacheValueBytes"/>). When
/// the LRU budget evicts a value payload, the row's metadata envelope is
/// retained as a <c>Value == null &amp;&amp; !IsTombstone</c> sentinel. A value
/// read that lands on that sentinel must delegate to the primary leaf for the
/// authoritative payload rather than returning the null payload as a false
/// miss. Existence, by contrast, is answerable from the retained metadata and
/// must NOT pay a delegation RPC.
/// </summary>
public partial class LeafCacheGrainTests
{
    private static readonly byte[] TenBytesA = Encoding.UTF8.GetBytes("aaaaaaaaaa");
    private static readonly byte[] TenBytesB = Encoding.UTF8.GetBytes("bbbbbbbbbb");
    private static readonly byte[] TenBytesC = Encoding.UTF8.GetBytes("cccccccccc");

    private static LatticeOptions BudgetOptions(long maxBytes) =>
        new() { MaxCacheValueBytes = maxBytes };

    [Test]
    public async Task GetAsync_delegates_to_primary_when_payload_was_evicted()
    {
        // Budget fits two 10-byte payloads; a three-entry delta evicts the LRU
        // key ("e1") down to the metadata sentinel.
        var (grain, leaf) = CreateGrain(BudgetOptions(20));
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));
        leaf.GetAsync("e1").Returns(Encoding.UTF8.GetBytes("authoritative-e1"));

        // Prime: merges all three, evicts "e1"'s payload.
        await grain.GetAsync("e3");

        // The evicted payload is reflected in the footprint: two resident
        // 10-byte payloads remain.
        Assert.That(grain.DebugFootprint().EntryCount, Is.EqualTo(3));
        Assert.That(grain.DebugFootprint().ValueBytes, Is.EqualTo(20));

        // No further delta on the read of the evicted key.
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var result = await grain.GetAsync("e1");

        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("authoritative-e1"),
            "A read on an evicted payload must delegate to the primary leaf, not return the null sentinel.");
        await leaf.Received().GetAsync("e1");
    }

    [Test]
    public async Task GetAsync_resident_key_serves_from_cache_without_delegating()
    {
        var (grain, leaf) = CreateGrain(BudgetOptions(20));
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));

        await grain.GetAsync("e3");

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        leaf.ClearReceivedCalls();

        // "e3" is the most-recently-used key and stays resident.
        var result = await grain.GetAsync("e3");

        Assert.That(result, Is.Not.Null);
        Assert.That(result, Is.EqualTo(TenBytesC));
        await leaf.DidNotReceive().GetAsync("e3");
    }

    [Test]
    public async Task ExistsAsync_reports_true_for_evicted_payload_without_delegating()
    {
        var (grain, leaf) = CreateGrain(BudgetOptions(20));
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));

        await grain.GetAsync("e3");

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        leaf.ClearReceivedCalls();

        // "e1" was evicted to the metadata sentinel; existence is answerable
        // from the retained envelope with no leaf RPC.
        var exists = await grain.ExistsAsync("e1");

        Assert.That(exists, Is.True,
            "A payload-evicted entry still exists - ExistsAsync must answer from metadata without delegating.");
        await leaf.DidNotReceive().ExistsAsync("e1");
    }

    [Test]
    public async Task GetManyAsync_delegates_evicted_keys_and_serves_resident_keys()
    {
        var (grain, leaf) = CreateGrain(BudgetOptions(20));
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));
        leaf.GetManyAsync(Arg.Any<List<string>>())
            .Returns(_ => new Dictionary<string, byte[]>
            {
                ["e1"] = Encoding.UTF8.GetBytes("authoritative-e1")
            });

        await grain.GetManyAsync(new List<string> { "e1", "e2", "e3" });

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        leaf.ClearReceivedCalls();

        var result = await grain.GetManyAsync(new List<string> { "e1", "e3" });

        // "e1" was evicted -> delegated to the leaf for the authoritative value.
        Assert.That(result, Contains.Key("e1"));
        Assert.That(Encoding.UTF8.GetString(result["e1"]), Is.EqualTo("authoritative-e1"));
        // "e3" stayed resident -> served from the cache.
        Assert.That(result, Contains.Key("e3"));
        Assert.That(result["e3"], Is.EqualTo(TenBytesC));

        await leaf.Received().GetManyAsync(Arg.Is<List<string>>(list =>
            list.Contains("e1") && !list.Contains("e3")));
    }

    [Test]
    public async Task Evicted_key_stops_delegating_after_a_higher_hlc_rewrite_repopulates_it()
    {
        var (grain, leaf) = CreateGrain(BudgetOptions(20));
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));

        await grain.GetAsync("e3"); // evicts "e1"

        // A later write for "e1" (strictly higher HLC) re-ships the full value.
        // The merge restores the payload, so subsequent reads serve locally.
        // Keep the budget from evicting it again by giving room: the re-ship
        // delta carries only "e1".
        var clock = HybridLogicalClock.Tick(default);
        for (var i = 0; i < 100; i++) clock = HybridLogicalClock.Tick(clock);
        var version = new VersionVector();
        version.Tick("primary");
        var reship = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["e1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("rewritten"), clock)
            },
            Version = version
        };
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(reship);
        leaf.ClearReceivedCalls();

        var result = await grain.GetAsync("e1");

        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("rewritten"),
            "A higher-HLC re-ship repopulates the evicted payload and the cache serves it locally.");
        await leaf.DidNotReceive().GetAsync("e1");
    }

    [Test]
    public async Task Unbounded_cache_never_evicts_and_never_delegates_for_capacity()
    {
        // With no budget configured (the default), the mirror is a faithful 1:1
        // copy and no read delegates for capacity reasons.
        var (grain, leaf) = CreateGrain();
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));

        await grain.GetAsync("e1");

        Assert.That(grain.DebugFootprint().ValueBytes, Is.EqualTo(30),
            "Every payload stays resident when unbounded.");

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        leaf.ClearReceivedCalls();

        var result = await grain.GetAsync("e1");
        Assert.That(result, Is.EqualTo(TenBytesA));
        await leaf.DidNotReceive().GetAsync("e1");
    }
}
