using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins issue #3509: a durable materialiser-pin advance coalesced inside the
/// <see cref="LeafCursorReporter"/> debounce window must be flushed when the silo stops,
/// through <see cref="LeafCursorReporterShutdownFlushParticipant"/>, rather than lost.
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterShutdownFlushTests
{
    private const string Tree = "shutdown-flush-tree";
    private static readonly string Consumer = $"_lattice_materialiser_{Tree}_leaf-0";

    [SetUp]
    public void SetUp() => WalMaterialiserPinPressure.ResetForTests();

    [TearDown]
    public void TearDown() => WalMaterialiserPinPressure.ResetForTests();

    [Test]
    public async Task FlushPendingDurablePinsAsync_persists_coalesced_advance_that_was_never_written()
    {
        var (reporter, pin, _) = CreateReporter();

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);
        Assert.That(Landed(pin, 2, 2), Is.False, "the second note is coalesced inside the debounce window");

        await reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.That(Landed(pin, 2, 2), Is.True, "the stop-time flush must persist the coalesced advance");
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_with_nothing_pending_makes_no_store_call()
    {
        var (reporter, pin, _) = CreateReporter();

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        var before = pin.Attempts;

        await reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.That(pin.Attempts, Is.EqualTo(before));
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_does_not_rewrite_a_pin_after_a_successful_flush()
    {
        var (reporter, pin, _) = CreateReporter();

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);

        await reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None);
        var afterFirstFlush = pin.Attempts;
        await reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.That(pin.Attempts, Is.EqualTo(afterFirstFlush), "a persisted advance must clear its pending slot");
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_skips_an_advance_covered_by_a_later_write_through()
    {
        var (reporter, pin, clock) = CreateReporter();

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);

        clock.Advance(1001);
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(3), 3);
        await TestPoll.UntilAsync(() => Landed(pin, 3, 3), "the spaced note writes through", TimeSpan.FromSeconds(2));
        var before = pin.Attempts;

        await reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.That(pin.Attempts, Is.EqualTo(before), "a write-through that covers the pending advance clears it");
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_retries_an_advance_whose_write_faulted()
    {
        var (reporter, pin, _) = CreateReporter();
        pin.Fail = true;

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => pin.Attempts >= 1, "the first note attempts a write", TimeSpan.FromSeconds(2));
        await Task.Delay(50);
        pin.Fail = false;

        await reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.That(Landed(pin, 1, 1), Is.True, "a faulted write must stay pending for the stop-time flush");
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_swallows_a_failing_store()
    {
        var (reporter, pin, _) = CreateReporter();
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);
        pin.Fail = true;
        var before = pin.Attempts;

        Assert.DoesNotThrowAsync(() => reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None));
        Assert.That(pin.Attempts, Is.GreaterThan(before), "the flush must have attempted the write");
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_is_bounded_by_its_deadline_when_the_store_hangs()
    {
        var (reporter, pin, _) = CreateReporter();
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);
        pin.Hang = true;

        var stopwatch = Stopwatch.StartNew();
        Assert.DoesNotThrowAsync(() => reporter.FlushPendingDurablePinsAsync(TimeSpan.FromMilliseconds(100), CancellationToken.None));
        stopwatch.Stop();

        Assert.That(stopwatch.Elapsed, Is.LessThan(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_with_a_cancelled_token_does_not_throw()
    {
        var (reporter, pin, _) = CreateReporter();
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);
        pin.Hang = true;
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.DoesNotThrowAsync(() => reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), cts.Token));
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_does_not_resurrect_a_pin_removed_by_UnregisterTreeAsync()
    {
        var (reporter, pin, _) = CreateReporter();
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);

        await reporter.UnregisterTreeAsync(Tree, CancellationToken.None);
        var before = pin.Attempts;
        await reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.That(pin.Attempts, Is.EqualTo(before));
        Assert.That(Landed(pin, 2, 2), Is.False);
    }

    [Test]
    public async Task FlushPendingDurablePinsAsync_does_not_resurrect_a_pin_removed_by_UnregisterAsync()
    {
        var (reporter, pin, _) = CreateReporter();
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);

        await reporter.UnregisterAsync(Tree, Consumer, CancellationToken.None);
        var before = pin.Attempts;
        await reporter.FlushPendingDurablePinsAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.That(pin.Attempts, Is.EqualTo(before));
        Assert.That(Landed(pin, 2, 2), Is.False);
    }

    [Test]
    public async Task NoteDurableMaterialiserFrontier_coalesced_steady_state_path_does_not_allocate()
    {
        var (reporter, pin, _) = CreateReporter();
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));

        // Warm up: the first coalesced note creates the pending slot; later ones merge into it.
        long tick = 2;
        for (var i = 0; i < 1_000; i++, tick++)
        {
            reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(tick), tick);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10_000; i++, tick++)
        {
            reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(tick), tick);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.Zero, "the coalesced steady-state note path must not allocate");
    }

    [Test]
    public async Task Participant_subscribes_at_ApplicationServices_and_flushes_on_stop()
    {
        var (reporter, pin, _) = CreateReporter();
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(1), 1);
        await TestPoll.UntilAsync(() => Landed(pin, 1, 1), "the first note writes through", TimeSpan.FromSeconds(2));
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(2), 2);

        var lifecycle = Substitute.For<ISiloLifecycle>();
        ILifecycleObserver? observer = null;
        var stage = -1;
        lifecycle
            .Subscribe(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<ILifecycleObserver>())
            .Returns(call =>
            {
                stage = call.ArgAt<int>(1);
                observer = call.ArgAt<ILifecycleObserver>(2);
                return Substitute.For<IDisposable>();
            });

        new LeafCursorReporterShutdownFlushParticipant(reporter).Participate(lifecycle);

        Assert.That(stage, Is.EqualTo(ServiceLifecycleStage.ApplicationServices));
        Assert.That(observer, Is.Not.Null);
        await observer!.OnStart(CancellationToken.None);
        Assert.That(Landed(pin, 2, 2), Is.False, "starting must not flush");

        await observer.OnStop(CancellationToken.None);

        Assert.That(Landed(pin, 2, 2), Is.True, "stopping must flush the coalesced advance");
    }

    [Test]
    public void OnStopAsync_with_a_non_built_in_reporter_completes_without_calling_it()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        var participant = new LeafCursorReporterShutdownFlushParticipant(reporter);

        var task = participant.OnStopAsync(CancellationToken.None);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
        Assert.That(reporter.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void Participate_with_null_lifecycle_throws()
    {
        var participant = new LeafCursorReporterShutdownFlushParticipant(Substitute.For<ILeafCursorReporter>());

        Assert.Throws<ArgumentNullException>(() => participant.Participate(null!));
    }

    [Test]
    public void DefaultDeadline_is_positive_and_bounded()
    {
        Assert.That(LeafCursorReporterShutdownFlushParticipant.DefaultDeadline, Is.GreaterThan(TimeSpan.Zero));
        Assert.That(LeafCursorReporterShutdownFlushParticipant.DefaultDeadline, Is.LessThanOrEqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void AddWalCursorRegistry_registers_the_shutdown_flush_participant_once()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        builder.AddLattice((_, _) => { });

        builder.AddWalCursorRegistry();
        builder.AddWalCursorRegistry();

        var registrations = services
            .Where(d => d.ServiceType == typeof(ILifecycleParticipant<ISiloLifecycle>)
                && d.ImplementationType == typeof(LeafCursorReporterShutdownFlushParticipant))
            .ToList();
        Assert.That(registrations, Has.Count.EqualTo(1));
        Assert.That(registrations[0].Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
    }

    private static (LeafCursorReporter Reporter, TogglingPinGrain Pin, FrozenClock Clock) CreateReporter()
    {
        var options = new LatticeOptions { WalPartitions = 1, WalMaterialiserPinShards = 1 };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var registry = Substitute.For<IWalCursorRegistry>();
        registry.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(Array.Empty<WalCursorSnapshot>()));

        var pin = new TogglingPinGrain();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);

        var clock = new FrozenClock();
        var reporter = new LeafCursorReporter(registry, factory, monitor) { Clock = clock };
        return (reporter, pin, clock);
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static bool Landed(TogglingPinGrain pin, long ticks, long offset)
        => pin.Landed.Any(r => r.Consumer == Consumer && r.Frontier.WallClockTicks == ticks && r.Offset == offset);

    private sealed class FrozenClock : TimeProvider
    {
        private long _ticks = 1_000_000;

        public override long TimestampFrequency => 1000;

        public override long GetTimestamp() => Interlocked.Read(ref _ticks);

        public void Advance(long milliseconds) => Interlocked.Add(ref _ticks, milliseconds);
    }

    private sealed class TogglingPinGrain : IWalMaterialiserPinGrain
    {
        private int _attempts;
        public volatile bool Fail;
        public volatile bool Hang;

        public int Attempts => Volatile.Read(ref _attempts);

        public ConcurrentBag<(string Consumer, HybridLogicalClock Frontier, long Offset)> Landed { get; } = new();

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier)
            => Accept([new MaterialiserPinReport(consumerId, frontier, -1)]);

        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Accept(reports);

        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Accept(reports);

        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync()
            => Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal));

        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync()
            => Task.FromResult<IReadOnlyDictionary<string, long>>(new Dictionary<string, long>(StringComparer.Ordinal));

        public Task RemoveAsync(string consumerId) => Task.CompletedTask;

        public Task ClearAsync() => Task.CompletedTask;

        private Task Accept(IReadOnlyList<MaterialiserPinReport> reports)
        {
            Interlocked.Increment(ref _attempts);
            if (Hang)
            {
                return new TaskCompletionSource().Task;
            }

            if (Fail)
            {
                return Task.FromException(new TimeoutException("durable pin store unavailable"));
            }

            for (var i = 0; i < reports.Count; i++)
            {
                Landed.Add((reports[i].ConsumerId, reports[i].Frontier, reports[i].CheckpointOffset));
            }

            return Task.CompletedTask;
        }
    }
}
