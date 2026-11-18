using NSubstitute;
using NSubstitute.Extensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6083: healing an aliased tree must fold through the logical tree,
/// whose registry row is the map routing reads.
/// </summary>
public partial class ShardHealingOrchestratorGrainTests
{
    private const string AliasTarget = "healing-test-tree-copy";
    private const string AliasOwner = "healing-test-tree-owner";

    [Test]
    public async Task RunHealingPass_starts_an_aliased_tree_fold_on_the_coordinator_keyed_by_the_logical_tree()
    {
        var h = CreateGrain(shapeEntry: (id, row) => id == TreeId ? row with { PhysicalTreeId = AliasTarget } : row);

        await h.Grain.RunHealingPassAsync();

        Assert.That((await h.Grain.GetHealingReportAsync()).Decision, Is.EqualTo(ShardHealingDecision.Admitted));
        var donor = h.State.State.InFlightDonorShardIndices.Single();
        h.GrainFactory.Received().GetGrain<ITreeShardConsolidationGrain>($"{TreeId}/{donor}");
        h.GrainFactory.DidNotReceive().GetGrain<ITreeShardConsolidationGrain>($"{AliasTarget}/{donor}");
        await h.ConsolidationOf(donor).Received(1).StartAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunHealingPass_keeps_counting_a_fold_admitted_under_the_alias_target_id_until_it_ends()
    {
        // A fold admitted before the fix runs under the copy's id. Until it ends
        // it must keep counting against admission, or a second fold could start
        // beside it.
        var legacy = Substitute.For<ITreeShardConsolidationGrain>();
        legacy.GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = true, DonorShardIndex = 7, SurvivorShardIndex = 6, Phase = ShardConsolidationPhase.Drain,
        });
        var h = CreateGrain(
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] },
            shapeEntry: (id, row) => id == TreeId ? row with { PhysicalTreeId = AliasTarget } : row);
        h.GrainFactory.Configure().GetGrain<ITreeShardConsolidationGrain>($"{AliasTarget}/7").Returns(legacy);

        await h.Grain.RunHealingPassAsync();

        var report = await h.Grain.GetHealingReportAsync();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.EqualTo(new[] { 7 }));
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.AtCapacity));
        });
        await legacy.Received().RunConsolidationPassAsync();
    }

    [Test]
    public async Task RunHealingPass_stops_the_orchestrator_of_a_copy_its_owner_aliases()
    {
        var h = CreateGrain(shapeEntry: (id, row) => id switch
        {
            TreeId => row with { DerivedFrom = AliasOwner },
            AliasOwner => row with { PhysicalTreeId = TreeId },
            _ => row,
        });

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty);
        await h.ShardOf(0).DidNotReceive().GetSamplingStatusAsync();
        await h.Reminders.Received().GetReminder(Arg.Any<GrainId>(), Arg.Any<string>());
        for (var donor = 0; donor < 8; donor++)
            await h.ConsolidationOf(donor).DidNotReceive().StartAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunHealingPass_repairs_routing_a_fold_left_on_the_alias_target()
    {
        // The logical map still routes donor 7's slots to it; the copy's own map,
        // which the pre-fix fold rewrote, sends them to 6, and 7 is retired.
        var logicalMap = ShardMap.CreateDefault(64, 8);
        var copySlots = (int[])logicalMap.Slots.Clone();
        for (var i = 0; i < copySlots.Length; i++)
            if (copySlots[i] == 7) copySlots[i] = 6;
        var donorSlots = Enumerable.Range(0, 64).Where(i => logicalMap.Slots[i] == 7).ToArray();

        var h = CreateGrain(shapeEntry: (id, row) => id switch
        {
            TreeId => row with { PhysicalTreeId = AliasTarget, ShardMap = logicalMap },
            AliasTarget => row with { ShardMap = new ShardMap { Slots = copySlots } },
            _ => row,
        });
        h.ShardOf(7).IsRetiredAsync().Returns(true);

        await h.Grain.RunHealingPassAsync();

        await h.Registry.Received(1).ReassignSlotsAsync(
            TreeId,
            Arg.Is<int[]>(slots => slots.SequenceEqual(donorSlots)),
            6,
            Arg.Any<ShardMap>(),
            AliasTarget);
    }

    [Test]
    public async Task RunHealingPass_leaves_a_shard_the_alias_target_map_lacks_unless_it_is_retired()
    {
        // A split of the logical tree adds a shard only the logical map names.
        // It is live, so it is not a fold's donor and must not be re-pointed.
        var logicalMap = ShardMap.CreateDefault(64, 8);
        var copySlots = (int[])logicalMap.Slots.Clone();
        for (var i = 0; i < copySlots.Length; i++)
            if (copySlots[i] == 7) copySlots[i] = 6;

        var h = CreateGrain(shapeEntry: (id, row) => id switch
        {
            TreeId => row with { PhysicalTreeId = AliasTarget, ShardMap = logicalMap },
            AliasTarget => row with { ShardMap = new ShardMap { Slots = copySlots } },
            _ => row,
        });

        await h.Grain.RunHealingPassAsync();

        await h.Registry.DidNotReceive().ReassignSlotsAsync(
            Arg.Any<string>(), Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<ShardMap>(), Arg.Any<string>());
    }
}
