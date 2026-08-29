using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <see cref="Orleans.Lattice.BPlusTree.Grains.LeafCacheGrain"/> honouring the per-tree runtime
/// <see cref="Orleans.Lattice.BPlusTree.State.TreeRegistryEntry.MaxCacheValueBytes"/> override resolved
/// through <see cref="LatticeOptionsResolver"/>. The cache resolves its payload
/// budget on each refresh, so a registry override caps the resident payload
/// bytes exactly as the static option would - and wins over the static option
/// when both are set. With no override pinned, the resolved budget equals the
/// static option byte-for-byte, so the pre-override behaviour is preserved.
/// <para>
/// These tests drive the same LRU eviction seam exercised by
/// <c>LeafCacheGrainTests.Eviction</c> (three 10-byte payloads against a
/// 20-byte cap evicts the LRU payload down to its metadata sentinel), but
/// source the cap from the registry override rather than the static option.
/// </para>
/// </summary>
public partial class LeafCacheGrainTests
{
    [Test]
    public async Task Registry_override_caps_the_budget_when_static_is_unbounded()
    {
        // Static option unbounded (null); the registry pins a 20-byte cap. The
        // resolved budget must come from the override, evicting the LRU payload.
        var (grain, leaf) = CreateGrain(options: null, registryOverrideMaxCacheValueBytes: 20);
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));

        await grain.GetAsync("e3");

        Assert.That(grain.DebugFootprint().EntryCount, Is.EqualTo(3));
        Assert.That(grain.DebugFootprint().ValueBytes, Is.EqualTo(20),
            "The runtime override must cap resident payload bytes even though the static option is unbounded.");
    }

    [Test]
    public async Task Registry_override_wins_over_a_larger_static_budget()
    {
        // Static option is generous (1000 bytes - all three payloads would fit);
        // the override pins a tighter 20-byte cap. The override must win, so one
        // payload is evicted.
        var (grain, leaf) = CreateGrain(BudgetOptions(1000), registryOverrideMaxCacheValueBytes: 20);
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));

        await grain.GetAsync("e3");

        Assert.That(grain.DebugFootprint().ValueBytes, Is.EqualTo(20),
            "The per-tree override must win over a larger silo-wide static budget.");
    }

    [Test]
    public async Task No_override_applies_static_budget_byte_for_byte()
    {
        // No registry override: the resolved budget must equal the static option
        // exactly (20 bytes), reproducing the pre-override eviction behaviour.
        var (grain, leaf) = CreateGrain(BudgetOptions(20), registryOverrideMaxCacheValueBytes: null);
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));

        await grain.GetAsync("e3");

        Assert.That(grain.DebugFootprint().ValueBytes, Is.EqualTo(20),
            "With no override, the resolved cap must equal the static option byte-for-byte.");
    }

    [Test]
    public async Task No_override_and_unbounded_static_never_evicts()
    {
        // No override and no static cap: the mirror stays a faithful 1:1 copy.
        var (grain, leaf) = CreateGrain(options: null, registryOverrideMaxCacheValueBytes: null);
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("e1", TenBytesA), ("e2", TenBytesB), ("e3", TenBytesC)));

        await grain.GetAsync("e1");

        Assert.That(grain.DebugFootprint().ValueBytes, Is.EqualTo(30),
            "Unbounded static option and no override leaves every payload resident.");
    }
}
