using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSnapshotProvider"/>'s
/// producer-side saga quiesce path. Asserts that
/// <see cref="ISnapshotProvider.ExportAsync"/> waits for in-flight
/// atomic-batch sagas to complete emission up to
/// <see cref="LatticeReplicationOptions.SnapshotSagaQuiesceTimeout"/>
/// and stamps timed-out sagas onto the
/// <see cref="SnapshotStream.SagaBlacklist"/>.
/// </summary>
[TestFixture]
public class LatticeSnapshotProviderSagaQuiesceTests
{
    private const string Tree = "snap-quiesce-tree";

    private static (LatticeSnapshotProvider Provider, IInFlightSagaTracker Tracker) CreateWithTimeout(TimeSpan quiesceTimeout)
    {
        var factory = Substitute.For<IGrainFactory>();
        var cursors = Substitute.For<ILatticeReplicationCursorRegistry>();
        var lattice = Substitute.For<ILattice>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(lattice);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        lattice.EntriesAsync(
            Arg.Any<string?>(),
            Arg.Any<string?>(),
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>()).Returns(EmptyEntries());

        cursors.GetCausalStableAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(new VersionVector()));

        var tracker = new InMemoryInFlightSagaTracker();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-test",
            SnapshotSagaQuiesceTimeout = quiesceTimeout,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.CurrentValue.Returns(options);

        return (new LatticeSnapshotProvider(factory, cursors, tracker, monitor), tracker);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> EmptyEntries()
    {
        await Task.CompletedTask;
        yield break;
    }

    [Test]
    public async Task ExportAsync_returns_empty_blacklist_when_no_sagas_are_in_flight()
    {
        var (provider, _) = CreateWithTimeout(TimeSpan.FromSeconds(30));

        var stream = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);

        Assert.That(stream.SagaBlacklist, Is.Empty);
    }

    [Test]
    public async Task ExportAsync_returns_empty_blacklist_when_in_flight_saga_drains_within_timeout()
    {
        // Use a generous quiesce window and complete the saga early
        // via a background task that fires the remaining emission a
        // few milliseconds after the export starts.
        var (provider, tracker) = CreateWithTimeout(TimeSpan.FromSeconds(2));
        var tx = Guid.NewGuid();
        tracker.ObserveEmission(Tree, tx, batchSize: 2); // 1 of 2 — in flight

        // Drain the saga from a background task with a small
        // pre-poll delay so the snapshot provider observes the
        // in-flight set first.
        var drainTask = Task.Run(async () =>
        {
            await Task.Delay(100);
            tracker.ObserveEmission(Tree, tx, batchSize: 2); // 2 of 2 — completes
        });

        var stream = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);
        await drainTask;

        Assert.That(stream.SagaBlacklist, Is.Empty);
    }

    [Test]
    public async Task ExportAsync_blacklists_saga_that_does_not_drain_within_timeout()
    {
        // Tight quiesce window + saga that never finishes emitting →
        // the saga's transaction id appears on the blacklist.
        var (provider, tracker) = CreateWithTimeout(TimeSpan.FromMilliseconds(150));
        var tx = Guid.NewGuid();
        tracker.ObserveEmission(Tree, tx, batchSize: 5); // only 1 of 5

        var stream = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);

        Assert.That(stream.SagaBlacklist, Is.EqualTo(new[] { tx }));
    }

    [Test]
    public async Task ExportAsync_blacklist_only_includes_sagas_observed_at_quiesce_start()
    {
        // A saga that starts AFTER the quiesce window begins is the
        // post-snapshot case — its keys are entirely past AsOfHlc and
        // the receiver's incremental path recognises the complete
        // batch. The provider must not include such a saga on the
        // blacklist even if it is mid-emission when the timeout fires.
        var (provider, tracker) = CreateWithTimeout(TimeSpan.FromMilliseconds(300));
        var initialTx = Guid.NewGuid();
        tracker.ObserveEmission(Tree, initialTx, batchSize: 5);

        // Start an export, and during the wait, kick off a brand-new
        // saga that's also mid-emission. The export's quiesce window
        // tracks only the initial set, so the late-starting saga
        // must not appear on the blacklist.
        var lateTx = Guid.NewGuid();
        var launchLate = Task.Run(async () =>
        {
            await Task.Delay(80);
            tracker.ObserveEmission(Tree, lateTx, batchSize: 5);
        });

        var stream = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);
        await launchLate;

        Assert.That(stream.SagaBlacklist, Is.EqualTo(new[] { initialTx }));
    }

    [Test]
    public async Task ExportAsync_propagates_cancellation_during_quiesce_wait()
    {
        var (provider, tracker) = CreateWithTimeout(TimeSpan.FromSeconds(30));
        tracker.ObserveEmission(Tree, Guid.NewGuid(), batchSize: 5);

        using var cts = new CancellationTokenSource();
        var task = provider.ExportAsync(Tree, HybridLogicalClock.Zero, cts.Token);
        cts.CancelAfter(TimeSpan.FromMilliseconds(50));

        Assert.That(
            async () => await task,
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// Test double simulating a producer process restart mid-quiesce:
    /// the first <see cref="GetInFlightTransactions"/> call returns
    /// the seeded set (the initial reading captured at quiesce start),
    /// every subsequent call returns empty (the rebooted tracker has
    /// no memory of the prior in-flight sagas). The
    /// <see cref="AnyInFlight"/> override mirrors the real tracker's
    /// hashed scan and reports false once drained.
    /// </summary>
    private sealed class ProducerRestartTracker : IInFlightSagaTracker
    {
        private readonly IReadOnlyList<Guid> _initial;
        private int _calls;

        public ProducerRestartTracker(IReadOnlyList<Guid> initial)
        {
            _initial = initial;
        }

        public void ObserveEmission(string treeName, Guid transactionId, int batchSize)
        {
            // No-op: the simulated restart wiped state; subsequent
            // emits would reseed in a real tracker, but for this
            // test we model the worst-case where no emits are
            // observed at all post-restart.
        }

        public IReadOnlyList<Guid> GetInFlightTransactions(string treeName)
        {
            var n = Interlocked.Increment(ref _calls);
            return n == 1 ? _initial : Array.Empty<Guid>();
        }
    }

    [Test]
    public async Task ExportAsync_returns_empty_blacklist_when_producer_restarts_mid_quiesce()
    {
        // Producer-process restart between quiesce-start and the
        // first poll iteration: the initial set is captured, but
        // by the next tracker read the in-flight set is empty
        // (the rebooted tracker has no memory of prior sagas).
        // The AnyInFlight probe must report false and the provider
        // must return an empty blacklist — the rebooted producer
        // has no way to complete the prior sagas, but it also
        // has no in-tree state for them, so the snapshot
        // boundary is consistent without a blacklist entry.
        var seeded = new[] { Guid.NewGuid(), Guid.NewGuid() };
        var tracker = new ProducerRestartTracker(seeded);

        var factory = Substitute.For<IGrainFactory>();
        var cursors = Substitute.For<ILatticeReplicationCursorRegistry>();
        var lattice = Substitute.For<ILattice>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(lattice);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        lattice.EntriesAsync(
            Arg.Any<string?>(),
            Arg.Any<string?>(),
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>()).Returns(EmptyEntries());
        cursors.GetCausalStableAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<VersionVector?>(new VersionVector()));

        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-test",
            SnapshotSagaQuiesceTimeout = TimeSpan.FromSeconds(30),
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.CurrentValue.Returns(options);

        var provider = new LatticeSnapshotProvider(factory, cursors, tracker, monitor);

        var stream = await provider.ExportAsync(Tree, HybridLogicalClock.Zero);

        Assert.That(stream.SagaBlacklist, Is.Empty,
            "A producer-restart-drained tracker must yield an empty blacklist, not a stale one.");
    }
}
