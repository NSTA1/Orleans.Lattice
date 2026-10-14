using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public sealed class LatticeOptionsResolverCompactionScheduleTests
{
    [Test]
    public void GetCompactionSchedule_clamps_warns_once_and_observes_configuration_reload_without_io()
    {
        var treeId = $"schedule-{Guid.NewGuid():N}";
        var options = new LatticeOptions
        {
            CompactionLeafBatchSize = 0,
            CompactionShardTickInterval = TimeSpan.Zero,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(treeId).Returns(_ => options);
        var factory = Substitute.For<IGrainFactory>();
        var logger = Substitute.For<ILogger<LatticeOptionsResolver>>();
        var resolver = new LatticeOptionsResolver(factory, monitor, logger);

        var first = resolver.GetCompactionSchedule(treeId);
        var repeated = resolver.GetCompactionSchedule(treeId);
        var full = resolver.Resolve(treeId, entry: null);

        Assert.Multiple(() =>
        {
            Assert.That(first.TickInterval, Is.EqualTo(LatticeOptions.MinCompactionShardTickInterval));
            Assert.That(first.LeafBatchSize, Is.EqualTo(LatticeOptions.MinCompactionLeafBatchSize));
            Assert.That(repeated, Is.EqualTo(first));
            Assert.That(full.CompactionShardTickInterval, Is.EqualTo(first.TickInterval));
            Assert.That(full.CompactionLeafBatchSize, Is.EqualTo(first.LeafBatchSize));
            Assert.That(logger.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILogger.Log)
                && c.GetArguments()[0] is LogLevel.Warning), Is.EqualTo(2));
        });

        options = new LatticeOptions
        {
            CompactionLeafBatchSize = 17,
            CompactionShardTickInterval = TimeSpan.FromSeconds(3),
        };
        Assert.That(resolver.GetCompactionSchedule(treeId), Is.EqualTo((TimeSpan.FromSeconds(3), 17)));
        Assert.That(factory.ReceivedCalls(), Is.Empty);
    }
}
