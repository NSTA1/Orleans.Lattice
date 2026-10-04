using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522, property H at a leaf split: every write acknowledged after a
/// prepare P must be stamped above P. A split sibling takes ownership of keys
/// whose prepares the donor minted, so it must start from the donor's clock;
/// otherwise a newborn sibling stamps a later write below a stranded prepare's P,
/// and a terminal applying the saga's value at P overwrites that later write.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Sibling_inherits_the_donor_clock_so_a_write_after_a_stranded_prepare_is_stamped_above_it()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateGrain(state);

        // A donor clock far ahead of wall time: the donor minted a prepare at it
        // just before the split, and the key now belongs to the sibling.
        var donorClock = new HybridLogicalClock
        {
            WallClockTicks = DateTimeOffset.UtcNow.Ticks + TimeSpan.TicksPerHour,
            Counter = 5,
        };

        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "tree-sibling-clock",
            ShardIndex = 0,
            LowKeyInclusive = "m",
            HighKeyExclusive = null,
            DonorClock = donorClock,
        });

        await sibling.SetAsync("p", [1]);

        Assert.That(sibling.EntriesForTest["p"].Timestamp.CompareTo(donorClock), Is.GreaterThan(0),
            "a write the sibling accepts after the split must be stamped above every stamp the donor minted");
    }

    [Test]
    public async Task Sibling_initialisation_without_a_donor_clock_leaves_the_sibling_clock_unchanged()
    {
        // An older donor sends no clock (the default); the sibling must not move.
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateGrain(state);
        var before = state.State.Clock;

        await sibling.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "tree-sibling-clock",
            ShardIndex = 0,
            LowKeyInclusive = "m",
        });

        Assert.That(state.State.Clock, Is.EqualTo(before));
    }
}
