using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- Compaction reminder registration tests ---

    private static ITombstoneCompactionGrain SetupCompactionGrain(IGrainFactory factory, string treeId)
    {
        var compaction = Substitute.For<ITombstoneCompactionGrain>();
        factory.GetGrain<ITombstoneCompactionGrain>(treeId, Arg.Any<string>())
            .Returns(compaction);
        return compaction;
    }

    [Test]
    public async Task SetAsync_CallsEnsureReminderAsync()
    {
        const string treeId = "compaction-set";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.SetAsync("k1", [1, 2, 3]);

        await compaction.Received(1).EnsureReminderAsync();
    }

    [Test]
    public async Task DeleteAsync_CallsEnsureReminderAsync()
    {
        const string treeId = "compaction-delete";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.DeleteAsync("k1");

        await compaction.Received(1).EnsureReminderAsync();
    }

    [Test]
    public async Task GetAsync_DoesNotCallEnsureReminderAsync()
    {
        const string treeId = "compaction-get";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.GetAsync("k1");

        await compaction.DidNotReceive().EnsureReminderAsync();
    }

    [Test]
    public async Task SetAsync_WhenCompactionDisabled_DoesNotCallEnsureReminderAsync()
    {
        const string treeId = "compaction-disabled";
        var (grain, factory) = CreateGrain(treeId, new LatticeOptions
        {
            TombstoneGracePeriod = Timeout.InfiniteTimeSpan
        });
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.SetAsync("k1", [1, 2, 3]);

        await compaction.DidNotReceive().EnsureReminderAsync();
    }

    [Test]
    public async Task SetAsync_SecondCall_DoesNotCallEnsureReminderAgain()
    {
        const string treeId = "compaction-dedup";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.SetAsync("k1", [1]);
        await grain.SetAsync("k2", [2]);

        await compaction.Received(1).EnsureReminderAsync();
    }
}
