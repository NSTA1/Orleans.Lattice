using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Proves every shared coordinator-stub builder in this fixture refuses an
/// unreachable WAL (issue #2680). The guard lives in
/// <see cref="ReachableWalFixture"/>; these tests pin that each builder actually
/// calls it, so deleting the call from a builder reddens the matching test here.
/// Each planted case carries its reachable neighbour, which must build.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static CommitLogSliceEntry PlantedEntry(long offset) => new(offset, default);

    [Test]
    public void BuildCoordinator_refuses_an_entry_at_the_exclusive_head()
    {
        Assert.Throws<InvalidOperationException>(() => BuildCoordinator(head: 1, PlantedEntry(1)));
        Assert.DoesNotThrow(() => BuildCoordinator(head: 2, PlantedEntry(1)));
    }

    [Test]
    public void BuildBudgetUnitsCoordinator_refuses_an_entry_at_the_exclusive_head()
    {
        Assert.Throws<InvalidOperationException>(() => BuildBudgetUnitsCoordinator(head: 1, [PlantedEntry(1)]));
        Assert.DoesNotThrow(() => BuildBudgetUnitsCoordinator(head: 2, [PlantedEntry(1)]));
    }

    [Test]
    public void BuildObservableCoordinator_refuses_an_entry_at_the_exclusive_head()
    {
        Assert.Throws<InvalidOperationException>(
            () => BuildObservableCoordinator(head: 1, sliceSize: 1, tail: 0, onRead: null, PlantedEntry(1)));
        Assert.DoesNotThrow(
            () => BuildObservableCoordinator(head: 2, sliceSize: 1, tail: 0, onRead: null, PlantedEntry(1)));
    }

    [Test]
    public void BuildChunkingCoordinator_refuses_an_entry_at_the_exclusive_head()
    {
        Assert.Throws<InvalidOperationException>(
            () => BuildChunkingCoordinator(head: 1, sliceSize: 1, tail: 0, PlantedEntry(1)));
        Assert.DoesNotThrow(
            () => BuildChunkingCoordinator(head: 2, sliceSize: 1, tail: 0, PlantedEntry(1)));
    }

    [Test]
    public void BuildCoordinatorWithLateAppends_refuses_a_persisted_entry_at_the_exclusive_head()
    {
        Assert.Throws<InvalidOperationException>(
            () => BuildCoordinatorWithLateAppends(head: 1, [PlantedEntry(1)], [PlantedEntry(2)]));
        Assert.Throws<InvalidOperationException>(
            () => BuildCoordinatorWithLateAppends(head: 2, [PlantedEntry(1)], [PlantedEntry(3)]));
        Assert.DoesNotThrow(
            () => BuildCoordinatorWithLateAppends(head: 2, [PlantedEntry(1)], [PlantedEntry(2)]));
    }
}
