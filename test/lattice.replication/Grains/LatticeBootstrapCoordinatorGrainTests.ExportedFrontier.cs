using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #4586 part 2b-2: the coordinator keeps the export's applied frontier
/// only when the export's source generation held still under the lineage it was
/// read under, and pins what it kept on the tree frontier at the handoff.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    private static readonly SnapshotSourceGeneration StableGeneration = new()
    {
        PhysicalTreeId = "boot-physical",
        ShardMapVersion = 2,
        Lineage = Guid.Parse("33333333-3333-3333-3333-333333333333"),
        DeleteEpoch = 0,
        IsDeleted = false,
    };

    private static SnapshotSourceFrontier ExportedFrontier(Guid? lineage) => new()
    {
        Lineage = lineage,
        LowWatermarks = new Dictionary<string, HybridLogicalClock> { ["site-q"] = Hlc(40) },
        Held = new Dictionary<string, HybridLogicalClock[]> { ["site-q"] = [Hlc(12)] },
    };

    private static SnapshotStream FrontierStream(SnapshotSourceGeneration close, SnapshotSourceFrontier frontier)
    {
        SnapshotStream? stream = null;
        async IAsyncEnumerable<SnapshotEntry> Entries()
        {
            await Task.Yield();
            yield return new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(5) };
            stream!.CloseGeneration = close;
            stream.SourceFrontier = frontier;
        }

        stream = new SnapshotStream(Tree, HybridLogicalClock.Zero, new VersionVector(), Entries())
        {
            OpenGeneration = StableGeneration,
        };
        return stream;
    }

    [Test]
    public async Task A_drain_of_a_stable_export_keeps_its_frontier_and_an_unstable_one_keeps_none()
    {
        var stable = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(stable, LatticeBootstrapState.RequestingSnapshot);
        var (stableGrain, _, _, stableProvider, _, _, _, _) = Create(stable);
        var exported = ExportedFrontier(StableGeneration.Lineage);
        stableProvider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FrontierStream(StableGeneration, exported)));

        var unstable = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(unstable, LatticeBootstrapState.RequestingSnapshot);
        var (unstableGrain, _, _, unstableProvider, _, _, _, _) = Create(unstable);
        unstableProvider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FrontierStream(StableGeneration with { Lineage = Guid.NewGuid() }, ExportedFrontier(StableGeneration.Lineage))));

        await stableGrain.ProcessNextPhaseAsync();
        await unstableGrain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(stable.State.ExportedFrontier, Is.SameAs(exported));
            Assert.That(unstable.State.ExportedFrontier, Is.Null, "the source's contents were re-stamped during the export");
        });
    }

    [Test]
    public async Task The_handoff_pins_the_kept_frontier_under_the_epoch_captured_before_the_export()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        var epoch = Guid.NewGuid();
        var exported = ExportedFrontier(StableGeneration.Lineage);
        fake.State.FrontierEpoch = epoch;
        fake.State.ExportedFrontier = exported;
        var (grain, _, factory, _, reminders, _, _, _) = Create(fake);
        var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
        frontier.PinAsync(default, default!, default!, default).ReturnsForAnyArgs(true);
        factory.GetGrain<IReplicationTreeFrontierGrain>(Tree).Returns(frontier);
        factory.GetGrain<Orleans.Lattice.BPlusTree.ICrossTreeBarrierIndexGrain>(Arg.Any<string>()).Returns(_ => HighWaterMarkTestGrains.EmptyBarrierIndex());
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive").Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        await frontier.Received(1).PinAsync(epoch, exported.LowWatermarks, exported.Held, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_handoff_with_no_kept_frontier_pins_every_origin_from_zero()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        var epoch = Guid.NewGuid();
        fake.State.FrontierEpoch = epoch;
        var (grain, _, factory, _, reminders, _, _, _) = Create(fake);
        var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
        frontier.PinAsync(default, default!, default!, default).ReturnsForAnyArgs(true);
        factory.GetGrain<IReplicationTreeFrontierGrain>(Tree).Returns(frontier);
        factory.GetGrain<Orleans.Lattice.BPlusTree.ICrossTreeBarrierIndexGrain>(Arg.Any<string>()).Returns(_ => HighWaterMarkTestGrains.EmptyBarrierIndex());
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive").Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        await frontier.Received(1).PinAsync(
            epoch,
            Arg.Is<IReadOnlyDictionary<string, HybridLogicalClock>>(d => d.Count == 0),
            Arg.Is<IReadOnlyDictionary<string, HybridLogicalClock[]>>(d => d.Count == 0),
            Arg.Any<CancellationToken>());
    }
}
