using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the WAL append fence in pre-arm mode (#5833, revision 2),
/// the counterparts of <see cref="WalAppendFenceIntegrationTests"/>, which runs in eager
/// mode. A leaf claims nothing until it arms, so a write never meets a claim of its own:
/// another activation's arm does not refuse it, and is told of it through the leaf's
/// foreign high instead. The clear's retirement still covers a duplicate's epoch row.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalAppendFencePreArmIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
        await _cluster.WaitForAppendFenceGateAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainStorage Storage => SiloServices.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    private async Task<(ILattice Tree, Guid Leaf, int Partitions)> NewTreeAsync(string name)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(name, new TreeRegistryEntry
        {
            MaxLeafKeys = 1024,
            MaxInternalChildren = 16,
            ShardCount = 1,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(name);
        await tree.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{name}/0"));
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(name);
        return (tree, new Guid(hash.AsSpan(0, 16)), Math.Max(1, partitions));
    }

    [Test]
    public async Task The_pre_arm_class_is_open_and_a_leaf_s_first_writes_claim_nothing()
    {
        // Counterpart of The_gate_is_open_on_a_cluster_of_capable_silos.
        var gate = SiloServices.GetRequiredService<IWalAppendFenceGate>();
        const string Name = "prearm-gate";
        var (_, leaf, partitions) = await NewTreeAsync(Name);

        Assert.Multiple(async () =>
        {
            Assert.That(gate.IsOpen, Is.True);
            Assert.That(gate.IsPreArmOpen, Is.True);
            Assert.That((await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf)).RecordExists, Is.False, "an unarmed leaf persisted an epoch");
            for (var partition = 0; partition < partitions; partition++)
            {
                Assert.That((await WalAppendFenceRows.ReadEntryAsync(Storage, Name, partition, leaf)).RecordExists, Is.False,
                    $"an unarmed leaf claimed at partition {partition}");
            }
        });
    }

    [Test]
    public async Task A_write_after_another_activation_armed_is_admitted_and_reported_to_it()
    {
        // Counterpart of the superseded-activation tests: under pre-arm the live, unarmed
        // activation's write is not refused by another activation's arm (no entry is
        // checked); it is admitted, and the arming activation learns of it through its
        // foreign high, which it must replay past before it may advance live.
        const string Name = "prearm-duplicate";
        var (tree, leaf, partitions) = await NewTreeAsync(Name);
        var other = Guid.NewGuid();
        var before = new long[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            var shard = _cluster.Client.GetGrain<IWalShardGrain>($"{Name}/{partition}");
            var arm = await shard.ArmAppendEpochAsync(leaf, 1, other, WalRoutingInitiator.None, 0, CancellationToken.None);
            Assert.That(arm.Accepted, Is.True, $"precondition: the other activation armed at partition {partition}");
            before[partition] = await shard.GetNextSequenceAsync(CancellationToken.None);
        }

        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var reported = false;
        for (var partition = 0; partition < partitions; partition++)
        {
            var shard = _cluster.Client.GetGrain<IWalShardGrain>($"{Name}/{partition}");
            if (await shard.GetNextSequenceAsync(CancellationToken.None) == before[partition])
            {
                continue;
            }

            var rearm = await shard.ArmAppendEpochAsync(leaf, 1, other, WalRoutingInitiator.None, 0, CancellationToken.None);
            reported |= rearm.Accepted && rearm.ForeignHigh >= before[partition];
        }

        Assert.Multiple(async () =>
        {
            Assert.That(Encoding.UTF8.GetString((await tree.GetAsync("k"))!), Is.EqualTo("v"), "the write did not succeed");
            Assert.That(reported, Is.True, "the live activation's write was not attributed as foreign to the arming activation");
        });
    }

    [Test]
    public async Task Clearing_a_leaf_floors_every_partition_at_a_duplicate_s_epoch_row_and_a_never_armed_leaf_retires_nothing()
    {
        // Counterpart of Clearing_a_leaf_floors_every_partition_at_its_epoch_row_and_deletes_its_rows:
        // a duplicate that persisted an epoch row and armed nowhere is still floored at
        // every partition; a leaf that never armed writes no floor.
        const string Name = "prearm-clear";
        var (_, leaf, partitions) = await NewTreeAsync(Name);
        const string NeverArmed = "prearm-clear-never-armed";
        var (_, neverArmed, neverArmedPartitions) = await NewTreeAsync(NeverArmed);
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf);
        row.State.Epoch = 3;
        await WalAppendFenceRows.WriteAsync(Storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(leaf), row);

        await _cluster.Client.GetGrain<IBPlusLeafGrain>(leaf).ClearGrainStateAsync();
        await _cluster.Client.GetGrain<IBPlusLeafGrain>(neverArmed).ClearGrainStateAsync();

        for (var partition = 0; partition < partitions; partition++)
        {
            var floor = await WalAppendFenceRows.ReadFloorAsync(Storage, Name, partition);
            Assert.That(floor.State.Floor, Is.GreaterThanOrEqualTo(3), $"partition {partition}'s floor does not cover the duplicate's epoch row");
        }

        for (var partition = 0; partition < neverArmedPartitions; partition++)
        {
            var floor = await WalAppendFenceRows.ReadFloorAsync(Storage, NeverArmed, partition);
            Assert.That(floor.RecordExists, Is.False, $"a never-armed leaf's clear wrote a floor at partition {partition}");
        }

        Assert.That((await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf)).RecordExists, Is.False,
            "the epoch row goes once every partition has retired");
    }
}
