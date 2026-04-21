using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class LatticeEventPublisherTests
{
    private static LatticeTreeEvent MakeEvent(string treeId = "t1") => new()
    {
        Kind = LatticeTreeEventKind.Set,
        TreeId = treeId,
        Key = "k",
        ShardIndex = 0,
        OperationId = null,
        AtUtc = DateTimeOffset.UtcNow,
    };

    [Test]
    public void PublishAsync_noops_when_publishing_disabled()
    {
        // A service provider with no stream provider registered would throw if
        // PublishAsync reached the lookup, so DoesNotThrow proves the early-exit
        // branch taken when PublishEvents is false.
        var services = new ServiceCollection().BuildServiceProvider();
        var options = new LatticeOptions { PublishEvents = false };

        Assert.DoesNotThrowAsync(async () =>
            await LatticeEventPublisher.PublishAsync(services, options, MakeEvent(), NullLogger.Instance));
    }

    [Test]
    public void PublishAsync_swallows_missing_provider_and_does_not_throw()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var options = new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" };

        Assert.DoesNotThrowAsync(async () =>
            await LatticeEventPublisher.PublishAsync(services, options, MakeEvent(), NullLogger.Instance));
    }

    [Test]
    public void CreateEvent_reads_operationId_from_request_context()
    {
        const string opId = "op-abc";
        Orleans.Runtime.RequestContext.Set(LatticeEventConstants.OperationIdRequestContextKey, opId);
        try
        {
            var evt = LatticeEventPublisher.CreateEvent(
                LatticeTreeEventKind.Set, treeId: "tree-a", key: "k", shardIndex: 2);

            Assert.That(evt.Kind, Is.EqualTo(LatticeTreeEventKind.Set));
            Assert.That(evt.TreeId, Is.EqualTo("tree-a"));
            Assert.That(evt.Key, Is.EqualTo("k"));
            Assert.That(evt.ShardIndex, Is.EqualTo(2));
            Assert.That(evt.OperationId, Is.EqualTo(opId));
            Assert.That(evt.AtUtc, Is.GreaterThan(DateTimeOffset.MinValue));
        }
        finally
        {
            Orleans.Runtime.RequestContext.Remove(LatticeEventConstants.OperationIdRequestContextKey);
        }
    }

    [Test]
    public void CreateEvent_leaves_operationId_null_when_context_absent()
    {
        Orleans.Runtime.RequestContext.Remove(LatticeEventConstants.OperationIdRequestContextKey);
        var evt = LatticeEventPublisher.CreateEvent(
            LatticeTreeEventKind.Delete, treeId: "t", key: "k", shardIndex: null);

        Assert.That(evt.OperationId, Is.Null);
        Assert.That(evt.Kind, Is.EqualTo(LatticeTreeEventKind.Delete));
    }
}
