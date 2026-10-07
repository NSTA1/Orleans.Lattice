using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class TxRegistryCaptureGateFanOutTests
{
    [Test]
    public async Task AcquireCaptureGateAsync_partial_failure_releases_every_attempted_key()
    {
        const string tree = "partial-acquire";
        var factory = Substitute.For<IGrainFactory>();
        var mark = Substitute.For<ITxRegistryHighWaterGrain>();
        mark.GetShardHighWaterAsync().Returns(Task.FromResult(1));
        factory.GetGrain<ITxRegistryHighWaterGrain>(tree).Returns(mark);
        var legacy = Substitute.For<ITxRegistryGrain>();
        var shard = Substitute.For<ITxRegistryGrain>();
        factory.GetGrain<ITxRegistryGrain>(tree).Returns(legacy);
        factory.GetGrain<ITxRegistryGrain>(TxRegistryRouting.ShardKeyAt(tree, 0)).Returns(shard);
        var token = Guid.NewGuid();
        shard.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, Arg.Any<TimeSpan>())
            .Returns(Task.FromException(new IOException("injected partial acquisition failure")));

        Assert.ThrowsAsync<IOException>(() => TxRegistryFanOut.AcquireCaptureGateAsync(
            factory, tree, token, TxRegistryCaptureGateMode.Gate, TimeSpan.FromSeconds(30)));

        await legacy.Received().ReleaseCaptureGateAsync(token);
        await shard.Received(1).ReleaseCaptureGateAsync(token);
    }

    [Test]
    public async Task GetCaptureGateSnapshotAsync_unions_legacy_and_sharded_D0_under_the_covered_mark()
    {
        const string tree = "gate-snapshot-union";
        var factory = Substitute.For<IGrainFactory>();
        var mark = Substitute.For<ITxRegistryHighWaterGrain>();
        mark.GetShardHighWaterAsync().Returns(Task.FromResult(2));
        factory.GetGrain<ITxRegistryHighWaterGrain>(tree).Returns(mark);
        var token = Guid.NewGuid();
        var expected = new Dictionary<Guid, TxStatus>();
        foreach (var key in TxRegistryRouting.EnumerateKeys(tree, 2))
        {
            var registry = Substitute.For<ITxRegistryGrain>();
            factory.GetGrain<ITxRegistryGrain>(key).Returns(registry);
            var txid = Guid.NewGuid();
            expected[txid] = TxStatus.Committed;
            registry.GetCaptureGateSnapshotAsync(token).Returns(
                Task.FromResult(new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed }));
        }

        var snapshot = await TxRegistryFanOut.GetCaptureGateSnapshotAsync(factory, tree, token);

        Assert.That(snapshot, Is.EquivalentTo(expected));
    }
}
