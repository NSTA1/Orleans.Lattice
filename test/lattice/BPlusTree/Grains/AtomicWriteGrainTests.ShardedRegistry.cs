using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Saga-side routing of the sharded saga decision registry (issue #3501): the
/// transaction id minted at admission is the one Prepare persists, so the
/// admission check, every registry write, and the decision all land on the one
/// registry shard the id routes to.
/// </summary>
public partial class AtomicWriteGrainTests
{
    private const int ShardedCount = 8;

    private static Dictionary<string, ITxRegistryGrain> StubShardedRegistries(IGrainFactory factory, Action<ITxRegistryGrain>? configure = null)
    {
        var registries = new Dictionary<string, ITxRegistryGrain>(StringComparer.Ordinal);
        foreach (var key in TxRegistryRouting.EnumerateKeys(TreeId, ShardedCount))
        {
            var registry = Substitute.For<ITxRegistryGrain>();
            configure?.Invoke(registry);
            registries[key] = registry;
            factory.GetGrain<ITxRegistryGrain>(key).Returns(registry);
        }

        return registries;
    }

    private static string[] KeysCalled(Dictionary<string, ITxRegistryGrain> registries) =>
        registries.Where(kv => kv.Value.ReceivedCalls().Any()).Select(kv => kv.Key).ToArray();

    [Test]
    public async Task ExecuteAsync_under_a_sharded_registry_admits_and_decides_on_the_txids_owning_shard()
    {
        Dictionary<string, ITxRegistryGrain> registries = null!;
        var (grain, state, _, _, _) = CreateGrain(
            options: new LatticeOptions { TxRegistryShardCount = ShardedCount },
            configureFactory: f => registries = StubShardedRegistries(f));

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));

        var txid = state.State.TransactionId;
        var owner = TxRegistryRouting.ShardKey(TreeId, txid);
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
            Assert.That(TxRegistryRouting.IsSharded(txid), Is.True, "A sharded tree mints a shard-stamped txid.");
            Assert.That(KeysCalled(registries), Is.EqualTo(new[] { owner }),
                "Admission, participant registration and the decision must all reach the one owning shard.");
        });
        await registries[owner].Received(1).EnsureSagaAdmissionAsync();
        await registries[owner].Received(1).MarkCommittedAsync(txid);
    }

    [Test]
    public async Task ExecuteAsync_admission_refusal_leaves_the_saga_unstarted_and_a_retry_stays_on_one_shard()
    {
        var refuse = true;
        Dictionary<string, ITxRegistryGrain> registries = null!;
        var (grain, state, _, _, _) = CreateGrain(
            options: new LatticeOptions { TxRegistryShardCount = ShardedCount },
            configureFactory: f => registries = StubShardedRegistries(f, r =>
                r.EnsureSagaAdmissionAsync().Returns(_ => refuse
                    ? Task.FromException(new LatticeSaturatedException("full", TreeId, LatticeSaturationSource.TxRegistryCapacity))
                    : Task.CompletedTask)));

        Assert.ThrowsAsync<LatticeSaturatedException>(() => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.NotStarted));
            Assert.That(state.State.TransactionId, Is.EqualTo(Guid.Empty), "A refused saga must not persist a txid.");
            Assert.That(state.WriteCount, Is.Zero, "A refused saga must not persist anything.");
        });

        refuse = false;
        foreach (var registry in registries.Values)
        {
            registry.ClearReceivedCalls();
        }

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        var owner = TxRegistryRouting.ShardKey(TreeId, state.State.TransactionId);
        Assert.That(KeysCalled(registries), Is.EqualTo(new[] { owner }),
            "The retried saga's admission and decision must land on the shard its persisted txid routes to.");
    }
}
