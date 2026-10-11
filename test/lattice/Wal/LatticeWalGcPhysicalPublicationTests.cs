using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Wal;

[TestFixture]
public sealed class LatticeWalGcPhysicalPublicationTests
{
    [Test]
    public async Task RunOnceAsync_sink_available_after_first_pass_recovers_publication()
    {
        var tree = $"gc-publication-{Guid.NewGuid():N}";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(tree, 0,
        [
            new WalEntry
            {
                Offset = 0,
                Mutation = new LatticeMutation { TreeId = tree, Key = "key", Value = new byte[100] },
            },
        ], CancellationToken.None);
        using var inner = new ServiceCollection().AddSingleton<IWalStorageProvider>(provider).BuildServiceProvider();
        using var sink = new LatticeStorageUsageMetrics();
        var services = new DelayedSinkProvider(inner);
        var options = new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 1, WalDurabilityHoldCeilingBytes = 0 };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        var gc = new LatticeWalGc(services, new InMemoryWalCursorRegistry(), monitor);

        var first = await gc.RunOnceAsync(tree);
        Assert.That(first.BytePressureOverThreshold, Is.True);
        Assert.That(ReadPolicy(tree), Is.Null);
        services.Sink = sink;
        var second = await gc.RunOnceAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(second.BytePressureOverThreshold, Is.True);
            Assert.That(ReadPolicy(tree), Is.EqualTo(1),
                "a null first resolution must not disable publication for the silo lifetime");
        });
        services.Sink = null;
        await gc.RunOnceAsync(tree);
        Assert.That(services.SinkResolutions, Is.EqualTo(2),
            "once present, the singleton sink must be cached instead of resolved per pass");
    }

    private static long? ReadPolicy(string tree)
        => GaugeScrape.ReadSingle(LatticeMetrics.Meter, LatticeMetrics.StoragePolicyOverThresholdName,
            LatticeMetrics.TagTree, tree);

    private sealed class DelayedSinkProvider(IServiceProvider inner) : IServiceProvider
    {
        public LatticeStorageUsageMetrics? Sink { get; set; }
        public int SinkResolutions { get; private set; }

        public object? GetService(Type serviceType)
        {
            if (serviceType != typeof(LatticeStorageUsageMetrics))
            {
                return inner.GetService(serviceType);
            }
            SinkResolutions++;
            return Sink;
        }
    }
}
