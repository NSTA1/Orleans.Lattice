using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Leaf-side routing of the sharded saga decision registry (issue #3501). A
/// leaf resolving a resident prepare must ask the registry shard its saga's
/// transaction id routes to, and a prepare written before the upgrade (a
/// version-4 id) must still resolve against the legacy single registry keyed by
/// the bare tree id. Each test stubs the registry at exactly one grain key, so a
/// leaf asking any other key reads the auto-substitute's default
/// <see cref="TxStatus.InFlight"/> and the prepare stays resident.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const int ShardedRegistryCount = 8;

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) BuildShardedRegistryLeaf(Guid txId, string registryKey)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusAsync(txId).Returns(TxStatus.Committed);
        return BuildSelfTerminaliseLeafCore(
            txId, registry, out _, persistedCheckpoint: 0,
            registryShardCount: ShardedRegistryCount,
            registryKey: registryKey);
    }

    [Test]
    public async Task Sharded_txid_prepare_resolves_against_its_owning_registry_shard()
    {
        var txId = TxRegistryRouting.MintTransactionId(ShardedRegistryCount);
        var shardKey = TxRegistryRouting.ShardKey(ResumableTreeId, txId, ShardedRegistryCount);
        Assume.That(shardKey, Is.Not.EqualTo(ResumableTreeId));
        var (grain, state) = BuildShardedRegistryLeaf(txId, shardKey);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.PendingTransactionCount, Is.EqualTo(0),
                "The leaf must read the committed verdict from the shard the txid routes to.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L));
        });
    }

    [Test]
    public async Task Legacy_v4_txid_prepare_resolves_against_the_legacy_registry_after_upgrade()
    {
        // A version-4 id is what every pre-sharding saga carries. Under a sharded
        // count it must still route to the bare-tree-id registry that recorded it.
        var txId = Guid.NewGuid();
        Assume.That(TxRegistryRouting.IsSharded(txId), Is.False);
        var (grain, state) = BuildShardedRegistryLeaf(txId, ResumableTreeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.PendingTransactionCount, Is.EqualTo(0),
                "A pre-upgrade prepare must resolve against the legacy registry row.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L));
        });
    }

    [Test]
    public async Task Sharded_txid_prepare_does_not_consult_the_legacy_registry()
    {
        // Guard: the legacy row does not hold a sharded saga's verdict, so a leaf
        // must not treat an answer from it as authoritative. Only the legacy key
        // is stubbed here, so the prepare must stay resident.
        var txId = TxRegistryRouting.MintTransactionId(ShardedRegistryCount);
        var (grain, _) = BuildShardedRegistryLeaf(txId, ResumableTreeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
    }
}
