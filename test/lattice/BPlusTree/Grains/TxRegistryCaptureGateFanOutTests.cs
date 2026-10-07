using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class TxRegistryCaptureGateFanOutTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task AcquireCaptureGateAsync_read_holds_acquire_legacy_first_then_ascending_shards(bool warmCache)
    {
        const string tree = "ordered-read-acquire";
        var factory = Substitute.For<IGrainFactory>();
        var mark = Substitute.For<ITxRegistryHighWaterGrain>();
        mark.GetShardHighWaterAsync().Returns(Task.FromResult(2));
        factory.GetGrain<ITxRegistryHighWaterGrain>(tree).Returns(mark);
        if (warmCache) TxRegistryHighWaterCache.Observe(factory, tree, 2);
        var token = Guid.NewGuid();
        var observed = new List<string>();
        var releaseLegacy = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var keys = TxRegistryRouting.EnumerateKeys(tree, 2);
        foreach (var key in keys)
        {
            var registry = Substitute.For<ITxRegistryGrain>();
            factory.GetGrain<ITxRegistryGrain>(key).Returns(registry);
            registry.AcquireReadCaptureGateAsync(token, Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
                .Returns(_ =>
                {
                    observed.Add(key);
                    return key == tree ? releaseLegacy.Task : Task.CompletedTask;
                });
        }

        var acquisition = TxRegistryFanOut.AcquireCaptureGateAsync(
            factory, tree, token, TxRegistryCaptureGateMode.Gate, TimeSpan.FromSeconds(30), readGate: true);
        Assert.That(observed, Is.EqualTo(new[] { tree }));
        releaseLegacy.SetResult();
        Assert.That(await acquisition, Is.EqualTo(2));
        Assert.That(observed, Is.EqualTo(warmCache
            ? new[] { tree, keys[0], keys[1] }
            : new[] { tree, tree, keys[0], keys[1] }));
    }

    [Test]
    public async Task AcquireCaptureGateAsync_read_partial_failure_releases_every_attempted_key()
    {
        const string tree = "read-partial-acquire";
        var factory = Substitute.For<IGrainFactory>();
        var mark = Substitute.For<ITxRegistryHighWaterGrain>();
        mark.GetShardHighWaterAsync().Returns(Task.FromResult(1));
        factory.GetGrain<ITxRegistryHighWaterGrain>(tree).Returns(mark);
        var legacy = Substitute.For<ITxRegistryGrain>();
        var shard = Substitute.For<ITxRegistryGrain>();
        factory.GetGrain<ITxRegistryGrain>(tree).Returns(legacy);
        factory.GetGrain<ITxRegistryGrain>(TxRegistryRouting.ShardKeyAt(tree, 0)).Returns(shard);
        var token = Guid.NewGuid();
        shard.AcquireReadCaptureGateAsync(token, Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new IOException("injected partial read acquisition failure")));

        Assert.ThrowsAsync<IOException>(() => TxRegistryFanOut.AcquireCaptureGateAsync(
            factory, tree, token, TxRegistryCaptureGateMode.Gate, TimeSpan.FromSeconds(30), readGate: true));
        await legacy.Received(1).ReleaseCaptureGateAsync(token);
        await shard.Received(1).ReleaseCaptureGateAsync(token);
    }

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
