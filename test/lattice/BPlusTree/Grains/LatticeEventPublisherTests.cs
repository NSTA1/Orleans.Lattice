using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Streams;

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

    /// <summary>
    /// Records every <c>orleans.lattice.events.*</c> measurement so a test can prove
    /// which counter fired and with which <c>reason</c> tag, rather than only proving
    /// that publication did not throw.
    /// </summary>
    private sealed class EventCounterRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Instrument, long Value, string? Reason, string? Kind)> _measurements = [];

        public EventCounterRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == LatticeMetrics.MeterName &&
                        instrument.Name.StartsWith("orleans.lattice.events.", StringComparison.Ordinal))
                    {
                        listener.EnableMeasurementEvents(instrument);
                    }
                },
            };

            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                string? reason = null;
                string? kind = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason) reason = tag.Value as string;
                    if (tag.Key == LatticeMetrics.TagKind) kind = tag.Value as string;
                }

                lock (_measurements)
                {
                    _measurements.Add((instrument.Name, value, reason, kind));
                }
            });

            _listener.Start();
        }

        public long Dropped(string reason)
        {
            _listener.RecordObservableInstruments();
            lock (_measurements)
            {
                return _measurements
                    .Where(m => m.Instrument == "orleans.lattice.events.dropped" && m.Reason == reason)
                    .Sum(m => m.Value);
            }
        }

        public long Published()
        {
            _listener.RecordObservableInstruments();
            lock (_measurements)
            {
                return _measurements
                    .Where(m => m.Instrument == "orleans.lattice.events.published")
                    .Sum(m => m.Value);
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    private static ServiceProvider ServicesWith(IStreamProvider streamProvider, string providerName)
        => new ServiceCollection()
            .AddKeyedSingleton(providerName, streamProvider)
            .BuildServiceProvider();

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
        using var counters = new EventCounterRecorder();

        Assert.DoesNotThrowAsync(async () =>
            await LatticeEventPublisher.PublishAsync(services, options, MakeEvent(), NullLogger.Instance));

        Assert.That(counters.Dropped("missing_provider"), Is.EqualTo(1),
            "A missing provider must be counted as a drop, not silently ignored.");
    }

    [Test]
    public async Task PublishAsync_counts_a_successful_dispatch_as_published()
    {
        // The success arm is the baseline the two failure arms below are contrasted
        // against: exactly one published measurement, tagged with the event kind, and
        // no drop at all.
        var evt = MakeEvent();
        var stream = Substitute.For<IAsyncStream<LatticeTreeEvent>>();
        stream.OnNextAsync(Arg.Any<LatticeTreeEvent>(), Arg.Any<StreamSequenceToken?>())
            .Returns(Task.CompletedTask);
        var provider = Substitute.For<IStreamProvider>();
        provider.GetStream<LatticeTreeEvent>(Arg.Any<StreamId>()).Returns(stream);

        using var services = ServicesWith(provider, "Default");
        var options = new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" };
        using var counters = new EventCounterRecorder();

        await LatticeEventPublisher.PublishAsync(services, options, evt, NullLogger.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(counters.Published(), Is.EqualTo(1));
            Assert.That(counters.Dropped("publish_error"), Is.Zero);
        });
    }

    [Test]
    public async Task PublishAsync_swallows_a_synchronous_provider_fault_and_counts_a_publish_error()
    {
        // The write path must never fail because the stream provider threw while the
        // stream was being resolved - before any await. This is the synchronous catch
        // arm: it has to swallow, log, and count the drop.
        var provider = Substitute.For<IStreamProvider>();
        provider.GetStream<LatticeTreeEvent>(Arg.Any<StreamId>())
            .Throws(new InvalidOperationException("stream provider is not initialised"));

        using var services = ServicesWith(provider, "Default");
        var options = new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" };
        using var counters = new EventCounterRecorder();

        Assert.DoesNotThrowAsync(async () =>
            await LatticeEventPublisher.PublishAsync(services, options, MakeEvent("tree-sync"), NullLogger.Instance));

        await Task.CompletedTask;
        Assert.Multiple(() =>
        {
            Assert.That(counters.Dropped("publish_error"), Is.EqualTo(1),
                "A synchronous provider fault is a publish error, distinct from a missing provider.");
            Assert.That(counters.Dropped("missing_provider"), Is.Zero);
            Assert.That(counters.Published(), Is.Zero);
        });
    }

    [Test]
    public async Task PublishAsync_swallows_an_asynchronous_dispatch_fault_and_counts_a_publish_error()
    {
        // The downstream queue rejecting the event is the common real failure. It
        // surfaces from the awaited OnNextAsync, so it is caught by the async arm -
        // and must not fault the returned task, because the caller is on the write path.
        var stream = Substitute.For<IAsyncStream<LatticeTreeEvent>>();
        stream.OnNextAsync(Arg.Any<LatticeTreeEvent>(), Arg.Any<StreamSequenceToken?>())
            .Returns(_ => Task.FromException(new TimeoutException("queue write timed out")));
        var provider = Substitute.For<IStreamProvider>();
        provider.GetStream<LatticeTreeEvent>(Arg.Any<StreamId>()).Returns(stream);

        using var services = ServicesWith(provider, "Default");
        var options = new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" };
        using var counters = new EventCounterRecorder();

        var publish = LatticeEventPublisher.PublishAsync(
            services, options, MakeEvent("tree-async"), NullLogger.Instance);

        Assert.DoesNotThrowAsync(async () => await publish,
            "The returned task must never fault - the write path does not fail because nobody is listening.");
        await publish;

        Assert.Multiple(() =>
        {
            Assert.That(counters.Dropped("publish_error"), Is.EqualTo(1));
            Assert.That(counters.Published(), Is.Zero,
                "A faulted dispatch must not also be counted as published.");
        });
    }

    [Test]
    public void PublishAsync_swallows_a_provider_resolution_fault()
    {
        // A container whose keyed resolution itself throws (a broken registration, a
        // disposed provider) still must not propagate into the write path.
        var keyed = Substitute.For<IKeyedServiceProvider>();
        keyed.GetKeyedService(Arg.Any<Type>(), Arg.Any<object?>())
            .Throws(new ObjectDisposedException("ServiceProvider"));
        using var counters = new EventCounterRecorder();

        var options = new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" };

        Assert.DoesNotThrowAsync(async () =>
            await LatticeEventPublisher.PublishAsync(keyed, options, MakeEvent("tree-resolve"), NullLogger.Instance));

        Assert.That(counters.Dropped("publish_error"), Is.EqualTo(1));
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
