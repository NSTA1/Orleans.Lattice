using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the bounded-concurrency contract of
/// <see cref="LatticeEventPublisher.PublishManyAsync"/>.
/// <para>
/// The property under test is <b>peak simultaneous in-flight publishes</b>, not
/// the publish COUNT. A count assertion cannot discriminate this change at all:
/// the sequential loop this replaced issued exactly the same number of
/// publishes, one at a time, so a count test passes identically before and
/// after and proves nothing. Only the peak tells the two shapes apart.
/// </para>
/// </summary>
[TestFixture]
public class LatticeEventPublisherPublishManyTests
{
    /// <summary>
    /// How long a gated publish waits for its peers before declaring the gate
    /// abandoned. Only ever paid once per test (see <see cref="ConcurrencyProbe"/>).
    /// </summary>
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Rendezvous gate that measures the largest number of publishes ever
    /// simultaneously in flight.
    /// <para>
    /// The abandoned-flag design is load-bearing. A naive "block until N have
    /// arrived" gate still passes under a SEQUENTIAL publisher: each call blocks
    /// alone, times out in turn, and the arrival counter still climbs to N - so
    /// the fixture reports success after N timeouts and asserts nothing. Here
    /// the FIRST timeout releases the gate permanently, so a sequential
    /// implementation costs one timeout rather than N, and the recorded peak
    /// stays at 1 and fails the assertion.
    /// </para>
    /// </summary>
    private sealed class ConcurrencyProbe
    {
        private readonly int _gateWidth;
        private readonly TaskCompletionSource _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _inFlight;
        private int _peak;
        private int _total;

        public ConcurrencyProbe(int gateWidth) => _gateWidth = gateWidth;

        /// <summary>Largest number of publishes observed in flight at once.</summary>
        public int Peak => Volatile.Read(ref _peak);

        /// <summary>Total publishes that entered the probe.</summary>
        public int Total => Volatile.Read(ref _total);

        /// <summary>True once a publish gave up waiting for peers that never came.</summary>
        public bool Abandoned { get; private set; }

        public async Task EnterAsync()
        {
            Interlocked.Increment(ref _total);
            var now = Interlocked.Increment(ref _inFlight);

            int seen;
            while (now > (seen = Volatile.Read(ref _peak)) &&
                   Interlocked.CompareExchange(ref _peak, now, seen) != seen)
            {
                // Lost the race; re-read and retry.
            }

            if (now >= _gateWidth)
            {
                _release.TrySetResult();
            }

            var timeout = Task.Delay(GateTimeout);
            if (await Task.WhenAny(_release.Task, timeout).ConfigureAwait(false) == timeout)
            {
                // Nobody else is coming. Release every future arrival too, so a
                // sequential publisher fails in one timeout instead of N.
                Abandoned = true;
                _release.TrySetResult();
            }

            Interlocked.Decrement(ref _inFlight);
        }
    }

    private static ServiceProvider ServicesWith(IStreamProvider streamProvider, string providerName)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(providerName, streamProvider);
        return services.BuildServiceProvider();
    }

    private static (LatticeEventPublisher.BatchPublisher Batch, ConcurrencyProbe Probe, ServiceProvider Services)
        BatchWithProbe(int gateWidth)
    {
        var probe = new ConcurrencyProbe(gateWidth);
        var stream = Substitute.For<IAsyncStream<LatticeTreeEvent>>();
        stream.OnNextAsync(Arg.Any<LatticeTreeEvent>(), Arg.Any<StreamSequenceToken?>())
            .Returns(_ => probe.EnterAsync());
        var provider = Substitute.For<IStreamProvider>();
        provider.GetStream<LatticeTreeEvent>(Arg.Any<StreamId>()).Returns(stream);

        var services = ServicesWith(provider, "Default");
        var options = new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" };
        var batch = LatticeEventPublisher.CreateBatch(services, options, "tree-fanout", NullLogger.Instance);
        return (batch, probe, services);
    }

    [Test]
    public async Task PublishManyAsync_keeps_a_full_window_of_publishes_in_flight_at_once()
    {
        // The whole point of the change: a wave of entries must overlap its
        // publishes rather than paying one stream round trip at a time. The
        // gate only opens once PublishWindow publishes have arrived together,
        // so completing at all is itself proof of the concurrency - and the
        // peak assertion below states the property explicitly.
        const int entries = LatticeEventPublisher.PublishWindow * 2;
        var (batch, probe, services) = BatchWithProbe(LatticeEventPublisher.PublishWindow);
        using var _ = services;

        var keys = Enumerable.Range(0, entries).Select(i => $"k{i}").ToList();
        await LatticeEventPublisher.PublishManyAsync(
            batch, LatticeTreeEventKind.Set, keys, static k => k);

        Assert.Multiple(() =>
        {
            Assert.That(probe.Abandoned, Is.False,
                "The gate was abandoned, which means a full window never assembled - " +
                "publication is running sequentially.");
            Assert.That(probe.Peak, Is.EqualTo(LatticeEventPublisher.PublishWindow),
                "Peak simultaneous publishes must reach exactly the window width.");
            Assert.That(probe.Total, Is.EqualTo(entries),
                "Every entry must still be published exactly once.");
        });
    }

    [Test]
    public async Task PublishManyAsync_never_exceeds_the_window_however_large_the_batch()
    {
        // The fan-out is bounded by REQUEST SIZE, not by a routing constant, so
        // the throttle is the only thing standing between a large SetManyAsync
        // and handing the stream provider the whole batch at once. This lane
        // proves the ceiling holds when the batch is far wider than the window.
        const int entries = LatticeEventPublisher.PublishWindow * 8;
        var (batch, probe, services) = BatchWithProbe(LatticeEventPublisher.PublishWindow);
        using var _ = services;

        var keys = Enumerable.Range(0, entries).Select(i => $"k{i}").ToList();
        await LatticeEventPublisher.PublishManyAsync(
            batch, LatticeTreeEventKind.Set, keys, static k => k);

        Assert.Multiple(() =>
        {
            Assert.That(probe.Peak, Is.LessThanOrEqualTo(LatticeEventPublisher.PublishWindow),
                "An unthrottled fan-out would let peak in-flight track the batch size.");
            Assert.That(probe.Total, Is.EqualTo(entries));
        });
    }

    [Test]
    public async Task PublishManyAsync_publishes_a_single_entry_without_building_a_window()
    {
        // The dominant single-entry case must keep its original one-await shape.
        // A gate width of 1 opens on the first arrival, so this completes
        // without ever touching the abandoned path.
        var (batch, probe, services) = BatchWithProbe(gateWidth: 1);
        using var _ = services;

        await LatticeEventPublisher.PublishManyAsync(
            batch, LatticeTreeEventKind.Set, new[] { "only" }, static k => k);

        Assert.Multiple(() =>
        {
            Assert.That(probe.Total, Is.EqualTo(1));
            Assert.That(probe.Peak, Is.EqualTo(1));
            Assert.That(probe.Abandoned, Is.False);
        });
    }

    [Test]
    public async Task PublishManyAsync_publishes_nothing_for_an_empty_batch()
    {
        var (batch, probe, services) = BatchWithProbe(gateWidth: 1);
        using var _ = services;

        await LatticeEventPublisher.PublishManyAsync(
            batch, LatticeTreeEventKind.Set, Array.Empty<string>(), static k => k);

        Assert.That(probe.Total, Is.Zero);
    }

    [Test]
    public async Task PublishManyAsync_stamps_every_entry_with_its_own_key()
    {
        // Concurrency must not smear the per-entry payload: each publish still
        // carries the key its own item projected to.
        const int entries = 40;
        var captured = new List<LatticeTreeEvent>();
        var stream = Substitute.For<IAsyncStream<LatticeTreeEvent>>();
        stream.OnNextAsync(Arg.Any<LatticeTreeEvent>(), Arg.Any<StreamSequenceToken?>())
            .Returns(call =>
            {
                lock (captured) captured.Add(call.Arg<LatticeTreeEvent>());
                return Task.CompletedTask;
            });
        var provider = Substitute.For<IStreamProvider>();
        provider.GetStream<LatticeTreeEvent>(Arg.Any<StreamId>()).Returns(stream);

        using var services = ServicesWith(provider, "Default");
        var options = new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" };
        var batch = LatticeEventPublisher.CreateBatch(services, options, "tree-keys", NullLogger.Instance);

        var keys = Enumerable.Range(0, entries).Select(i => $"key-{i}").ToList();
        await LatticeEventPublisher.PublishManyAsync(
            batch, LatticeTreeEventKind.Set, keys, static k => k);

        Assert.Multiple(() =>
        {
            Assert.That(captured, Has.Count.EqualTo(entries));
            Assert.That(captured.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(keys.OrderBy(k => k, StringComparer.Ordinal)),
                "Publication order is not guaranteed, but the key SET must be exact.");
            Assert.That(captured.Select(e => e.Kind), Is.All.EqualTo(LatticeTreeEventKind.Set));
        });
    }

    [Test]
    public async Task PublishManyAsync_does_not_fault_when_the_stream_throws_for_every_entry()
    {
        // Per-entry independence. The sequential loop swallowed a throwing
        // publish per iteration; the concurrent window must not let one failure
        // fault the WhenAll and strand the rest of the wave.
        var attempts = 0;
        var stream = Substitute.For<IAsyncStream<LatticeTreeEvent>>();
        stream.OnNextAsync(Arg.Any<LatticeTreeEvent>(), Arg.Any<StreamSequenceToken?>())
            .Returns(_ =>
            {
                Interlocked.Increment(ref attempts);
                return Task.FromException(new InvalidOperationException("stream down"));
            });
        var provider = Substitute.For<IStreamProvider>();
        provider.GetStream<LatticeTreeEvent>(Arg.Any<StreamId>()).Returns(stream);

        using var services = ServicesWith(provider, "Default");
        var options = new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" };
        var batch = LatticeEventPublisher.CreateBatch(services, options, "tree-throw", NullLogger.Instance);

        var keys = Enumerable.Range(0, LatticeEventPublisher.PublishWindow + 5)
            .Select(i => $"k{i}").ToList();

        Assert.DoesNotThrowAsync(async () => await LatticeEventPublisher.PublishManyAsync(
            batch, LatticeTreeEventKind.Set, keys, static k => k));

        Assert.That(attempts, Is.EqualTo(keys.Count),
            "Every entry must be attempted; a faulting window must not abandon its successors.");
        await Task.CompletedTask;
    }
}
