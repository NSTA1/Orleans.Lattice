using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeWalFloorHolderProbeGrain"/> (issue #4195): it picks
/// the pin holding the tree's offset floor exactly as the WAL GC does (lowest usable
/// offset, never a <c>-1</c> while a usable one exists), classifies the leaf behind
/// it from durable state without activating it, and reports a wedge only for a usable
/// pin above a never-persisted checkpoint - so the benign <c>-1</c> sentinel in the
/// same state is not a wedge.
/// </summary>
[TestFixture]
public sealed class LatticeWalFloorHolderProbeGrainTests
{
    private const string Tree = "orders";

    private static string Consumer(string leafKey, int? partition = null) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{Tree}_{GrainId.Create("bplusleaf", leafKey)}"
        + (partition is { } p ? $"_{p}" : string.Empty);

    private static LeafNodeState Leaf(long checkpoint, int partition = 0, int partitions = 1, string? treeId = Tree)
    {
        var byPartition = new long[partitions];
        Array.Fill(byPartition, -1L);
        byPartition[partition] = checkpoint;
        return new LeafNodeState
        {
            TreeId = treeId,
            ProjectionCheckpointOffset = partition == 0 ? checkpoint : -1L,
            ProjectionCheckpointOffsetAssigned = partition == 0 && checkpoint >= 0 ? true : null,
            ProjectionCheckpointOffsetsByPartition = byPartition,
        };
    }

    private sealed class CannedLeafStorage(LeafNodeState? state, Exception? throws = null) : IGrainStorage
    {
        public int Reads { get; private set; }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Reads++;
            if (throws is not null)
            {
                throw throws;
            }

            if (state is not null && grainState is IGrainState<LeafNodeState> leafState)
            {
                leafState.State = state;
                leafState.RecordExists = true;
            }
            else
            {
                grainState.RecordExists = false;
            }

            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => Task.CompletedTask;

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => Task.CompletedTask;
    }

    private sealed record Harness(LatticeWalFloorHolderProbeGrain Grain, IWalMaterialiserPinGrain Pins);

    private static Harness Build(
        IReadOnlyDictionary<string, long>? offsets,
        IGrainStorage? storage,
        int walPartitions = 1,
        IReadOnlyDictionary<string, HybridLogicalClock>? frontiers = null,
        Exception? offsetsFault = null)
    {
        var options = new LatticeOptions { WalMaterialiserPinShards = 1, WalPartitions = walPartitions };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(null));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var pins = Substitute.For<IWalMaterialiserPinGrain>();
        if (offsetsFault is not null)
        {
            pins.GetPinOffsetsAsync().ThrowsAsync(offsetsFault);
        }
        else
        {
            pins.GetPinOffsetsAsync().Returns(offsets ?? new Dictionary<string, long>());
        }

        pins.GetPinsAsync().Returns(frontiers ?? new Dictionary<string, HybridLogicalClock>());
        factory.GetGrain<IWalMaterialiserPinGrain>(Tree, Arg.Any<string?>()).Returns(pins);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("latticewalfloorholderprobe", Tree));

        var grain = new LatticeWalFloorHolderProbeGrain(
            context,
            factory,
            monitor,
            new LatticeOptionsResolver(factory, monitor),
            NullLogger<LatticeWalFloorHolderProbeGrain>.Instance,
            storage);
        return new Harness(grain, pins);
    }

    [Test]
    public async Task ProbeAsync_reports_a_wedge_for_a_usable_pin_above_a_never_persisted_checkpoint()
    {
        var holder = Consumer("leaf-a");
        var harness = Build(
            new Dictionary<string, long> { [holder] = 42, [Consumer("leaf-b")] = 100, [Consumer("leaf-c")] = -1 },
            new CannedLeafStorage(Leaf(checkpoint: -1)));

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.PinStoreReadable, Is.True);
            Assert.That(report.PinCount, Is.EqualTo(3));
            Assert.That(report.PinsWithoutOffset, Is.EqualTo(1));
            Assert.That(report.ConsumerId, Is.EqualTo(holder));
            Assert.That(report.LeafId, Is.EqualTo(GrainId.Create("bplusleaf", "leaf-a").ToString()));
            Assert.That(report.Partition, Is.EqualTo(0));
            Assert.That(report.PinOffset, Is.EqualTo(42));
            Assert.That(report.PersistedCheckpoint, Is.EqualTo(-1));
            Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.NeverCheckpointed));
            Assert.That(report.IsWedged, Is.True);
        });
    }

    [Test]
    public async Task ProbeAsync_does_not_report_a_wedge_for_the_benign_minus_one_sentinel()
    {
        var harness = Build(
            new Dictionary<string, long> { [Consumer("leaf-z")] = -1, [Consumer("leaf-c")] = -1 },
            new CannedLeafStorage(Leaf(checkpoint: -1)));

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.ConsumerId, Is.EqualTo(Consumer("leaf-c")), "the ordinally first -1 pin when none is usable");
            Assert.That(report.PinOffset, Is.EqualTo(-1));
            Assert.That(report.PinsWithoutOffset, Is.EqualTo(2));
            Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.NeverCheckpointed));
            Assert.That(report.IsWedged, Is.False);
        });
    }

    [Test]
    public async Task ProbeAsync_reports_a_checkpointed_holder_with_a_live_frontier_as_coverage_unknown()
    {
        var holder = Consumer("leaf-a");
        var harness = Build(
            new Dictionary<string, long> { [holder] = 42 },
            new CannedLeafStorage(Leaf(checkpoint: 42)),
            frontiers: new Dictionary<string, HybridLogicalClock> { [holder] = new HybridLogicalClock { WallClockTicks = 10 } });

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.CheckpointedCoverageUnknown));
            Assert.That(report.PersistedCheckpoint, Is.EqualTo(42));
            Assert.That(report.IsWedged, Is.False);
        });
    }

    [Test]
    public async Task ProbeAsync_keeps_checkpointed_uncovered_only_for_a_pin_proven_unusable()
    {
        var holder = Consumer("leaf-a");
        var harness = Build(
            new Dictionary<string, long> { [holder] = 42 },
            new CannedLeafStorage(Leaf(checkpoint: 42)),
            frontiers: new Dictionary<string, HybridLogicalClock> { [holder] = HybridLogicalClock.Zero });

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.CheckpointedUncovered));
    }

    [Test]
    public async Task ProbeAsync_claims_no_coverage_hole_when_the_frontier_cannot_be_read()
    {
        var harness = Build(
            new Dictionary<string, long> { [Consumer("leaf-a")] = 42 },
            new CannedLeafStorage(Leaf(checkpoint: 42)));

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.CheckpointedCoverageUnknown));
    }

    [Test]
    public async Task ProbeAsync_reads_the_partition_from_a_partitioned_consumer_id()
    {
        var holder = Consumer("leaf-a", partition: 2);
        var harness = Build(
            new Dictionary<string, long> { [holder] = 7 },
            new CannedLeafStorage(Leaf(checkpoint: -1, partition: 2, partitions: 4)),
            walPartitions: 4);

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partition, Is.EqualTo(2));
            Assert.That(report.LeafId, Is.EqualTo(GrainId.Create("bplusleaf", "leaf-a").ToString()));
            Assert.That(report.IsWedged, Is.True);
        });
    }

    [Test]
    public async Task ProbeAsync_reports_an_empty_tree_as_holding_no_pin()
    {
        var storage = new CannedLeafStorage(Leaf(checkpoint: -1));
        var harness = Build(new Dictionary<string, long>(), storage);

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.PinStoreReadable, Is.True);
            Assert.That(report.ConsumerId, Is.Null);
            Assert.That(report.IsWedged, Is.False);
            Assert.That(storage.Reads, Is.Zero, "no holder, so no leaf is read");
        });
    }

    [Test]
    public async Task ProbeAsync_reports_an_unreadable_pin_store_rather_than_failing()
    {
        var harness = Build(null, new CannedLeafStorage(null), offsetsFault: new TimeoutException("pin store down"));

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.PinStoreReadable, Is.False);
            Assert.That(report.ConsumerId, Is.Null);
            Assert.That(report.IsWedged, Is.False);
        });
    }

    [Test]
    public async Task ProbeAsync_reports_a_leaf_read_fault_as_unreadable()
    {
        var harness = Build(
            new Dictionary<string, long> { [Consumer("leaf-a")] = 42 },
            new CannedLeafStorage(null, throws: new TimeoutException("storage down")));

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.Unreadable));
            Assert.That(report.PersistedCheckpoint, Is.Null);
            Assert.That(report.IsWedged, Is.False);
        });
    }

    [Test]
    public async Task ProbeAsync_without_a_storage_provider_reports_unreadable()
    {
        var harness = Build(new Dictionary<string, long> { [Consumer("leaf-a")] = 42 }, storage: null);

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.Unreadable));
    }

    [Test]
    public async Task ProbeAsync_reports_an_unparseable_consumer_id_as_unreadable_with_no_leaf()
    {
        var harness = Build(
            new Dictionary<string, long> { ["someone-else"] = 3 },
            new CannedLeafStorage(Leaf(checkpoint: -1)));

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.ConsumerId, Is.EqualTo("someone-else"));
            Assert.That(report.LeafId, Is.Null);
            Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.Unreadable));
        });
    }

    [Test]
    public async Task ProbeAsync_reports_a_husk_as_orphaned()
    {
        var harness = Build(
            new Dictionary<string, long> { [Consumer("leaf-a")] = 42 },
            new CannedLeafStorage(Leaf(checkpoint: 42, treeId: null)));

        var report = await harness.Grain.ProbeAsync(CancellationToken.None);

        Assert.That(report.State, Is.EqualTo(WalGcBlockingPinState.Orphaned));
    }

    [Test]
    public void ProbeAsync_honours_a_cancelled_token()
    {
        var harness = Build(new Dictionary<string, long>(), null);

        Assert.That(async () => await harness.Grain.ProbeAsync(new CancellationToken(canceled: true)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void SelectHolder_prefers_the_lowest_usable_offset_and_breaks_ties_ordinally()
    {
        var (holder, offset, without) = LatticeWalFloorHolderProbeGrain.SelectHolder(new Dictionary<string, long>
        {
            ["b"] = 5, ["a"] = 5, ["c"] = 9, ["0"] = -1, ["1"] = -1,
        });

        Assert.Multiple(() =>
        {
            Assert.That(holder, Is.EqualTo("a"));
            Assert.That(offset, Is.EqualTo(5));
            Assert.That(without, Is.EqualTo(2));
        });
    }

    [Test]
    public void SelectHolder_falls_back_to_the_ordinally_first_minus_one_pin()
    {
        var (holder, offset, without) = LatticeWalFloorHolderProbeGrain.SelectHolder(new Dictionary<string, long>
        {
            ["z"] = -1, ["m"] = -1,
        });

        Assert.Multiple(() =>
        {
            Assert.That(holder, Is.EqualTo("m"));
            Assert.That(offset, Is.EqualTo(-1));
            Assert.That(without, Is.EqualTo(2));
        });
    }

    [Test]
    public void SelectHolder_returns_no_holder_for_no_pins()
    {
        Assert.That(LatticeWalFloorHolderProbeGrain.SelectHolder(new Dictionary<string, long>()).Holder, Is.Null);
    }

    [Test]
    public void IsWedged_needs_both_a_usable_offset_and_a_never_checkpointed_holder()
    {
        var wedged = new WalFloorHolderProbeReport { ConsumerId = "c", PinOffset = 0, State = WalGcBlockingPinState.NeverCheckpointed };

        Assert.Multiple(() =>
        {
            Assert.That(wedged.IsWedged, Is.True);
            Assert.That((wedged with { PinOffset = -1 }).IsWedged, Is.False);
            Assert.That((wedged with { State = WalGcBlockingPinState.CheckpointedCoverageUnknown }).IsWedged, Is.False);
            Assert.That((wedged with { ConsumerId = null }).IsWedged, Is.False);
        });
    }
}
