using System.Collections.Concurrent;

using Microsoft.Extensions.Options;

using NSubstitute;

using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Concurrency coverage for the durable-pin fan-out in
/// <see cref="LeafCursorReporter"/>. Both terminal paths - the tree-deletion
/// purge and the single-consumer unregister - address every key
/// <see cref="WalMaterialiserPinRouting.EnumerateReadKeys"/> enumerates, which
/// is <c>2 * shards + 1</c> entries (seventeen at the default shard count).
/// <para>
/// Those keys are distinct, so they are distinct grain activations with no
/// ordering relationship to one another. Walking them serialises one scheduler
/// round trip - and, off-silo, one network round trip - per key, where a single
/// concurrent round suffices.
/// </para>
/// <para>
/// A test that only counted calls would pass just as happily against a
/// sequential loop, so these tests observe the property that actually differs:
/// every call must be in flight at the same moment. Each substituted call
/// parks on a shared gate that is only released once all of them have arrived,
/// so a sequential implementation cannot get past the first one. A short
/// timeout releases the gate and raises a flag, so a regression fails in about
/// a second rather than hanging.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterDurablePinFanOutTests
{
    private const string Tree = "tree";
    private const string Consumer = "_lattice_materialiser_tree_leaf-1";
    private const int PinShards = 4;

    /// <summary>
    /// How long a parked call waits for its peers before declaring the fan-out
    /// sequential. Generous enough not to trip on a loaded CI agent, short
    /// enough that a real regression reports quickly.
    /// </summary>
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(10);

    private static int ExpectedKeyCount => WalMaterialiserPinRouting.EnumerateReadKeys(Tree, PinShards).Count;

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1, WalMaterialiserPinShards = PinShards };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// A rendezvous every substituted pin call passes through. It records the
    /// peak number of calls in flight simultaneously, which is the figure that
    /// separates a fan-out from a loop.
    /// </summary>
    private sealed class FanOutGate
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly int _expected;
        private int _inFlight;
        private int _peakInFlight;
        private int _abandoned;

        public FanOutGate(int expected) => _expected = expected;

        public ConcurrentBag<string> Keys { get; } = [];

        public int PeakInFlight => Volatile.Read(ref _peakInFlight);

        /// <summary>True when a call gave up waiting, i.e. its peers never arrived.</summary>
        public bool TimedOut => Volatile.Read(ref _abandoned) != 0;

        public async Task ArriveAsync(string key)
        {
            Keys.Add(key);

            var now = Interlocked.Increment(ref _inFlight);
            var peak = Volatile.Read(ref _peakInFlight);
            while (now > peak && Interlocked.CompareExchange(ref _peakInFlight, now, peak) != peak)
            {
                peak = Volatile.Read(ref _peakInFlight);
            }

            try
            {
                if (now >= _expected)
                {
                    _released.TrySetResult();
                }

                try
                {
                    await _released.Task.WaitAsync(GateTimeout);
                }
                catch (TimeoutException)
                {
                    // Nobody else is coming. Record it and let every later call
                    // through immediately so the test fails fast instead of
                    // paying the timeout once per key.
                    Interlocked.Exchange(ref _abandoned, 1);
                    _released.TrySetResult();
                }
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }
    }

    private static (LeafCursorReporter Reporter, FanOutGate Gate) Create()
    {
        var gate = new FanOutGate(ExpectedKeyCount);

        var registry = Substitute.For<IWalCursorRegistry>();
        registry.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        registry.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>([]));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(callInfo =>
        {
            var key = callInfo.ArgAt<string>(0);
            var grain = Substitute.For<IWalMaterialiserPinGrain>();
            grain.ClearAsync().Returns(_ => gate.ArriveAsync(key));
            grain.RemoveAsync(Arg.Any<string>()).Returns(_ => gate.ArriveAsync(key));
            return grain;
        });

        return (new LeafCursorReporter(registry, factory, Monitor()), gate);
    }

    [Test]
    public async Task Tree_purge_clears_every_pin_key_in_one_concurrent_round()
    {
        var (reporter, gate) = Create();

        await reporter.UnregisterTreeAsync(Tree, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(gate.TimedOut, Is.False,
                "A pin clear waited for its peers and they never arrived, so the purge is walking "
                + "the key enumeration one activation at a time.");
            Assert.That(gate.PeakInFlight, Is.EqualTo(ExpectedKeyCount),
                $"All {ExpectedKeyCount} pin keys are distinct activations with no ordering between "
                + "them, so every clear must be in flight at once rather than one per round trip.");
            Assert.That(gate.Keys, Is.EquivalentTo(WalMaterialiserPinRouting.EnumerateReadKeys(Tree, PinShards)),
                "Going concurrent must not change which keys are addressed.");
        });
    }

    [Test]
    public async Task Consumer_unregister_removes_from_every_pin_key_in_one_concurrent_round()
    {
        var (reporter, gate) = Create();

        await reporter.UnregisterAsync(Tree, Consumer, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(gate.TimedOut, Is.False,
                "A pin removal waited for its peers and they never arrived, so the unregister is "
                + "walking the key enumeration one activation at a time.");
            Assert.That(gate.PeakInFlight, Is.EqualTo(ExpectedKeyCount),
                $"All {ExpectedKeyCount} pin keys are distinct activations with no ordering between "
                + "them, so every removal must be in flight at once rather than one per round trip.");
            Assert.That(gate.Keys, Is.EquivalentTo(WalMaterialiserPinRouting.EnumerateReadKeys(Tree, PinShards)),
                "Going concurrent must not change which keys are addressed.");
        });
    }

    [Test]
    public async Task One_unreachable_pin_key_does_not_abandon_the_rest()
    {
        // Per-key independence is what the sequential try/catch gave, and it is
        // the thing a naive Task.WhenAll would lose: the first fault would
        // surface and the remaining keys would be left holding pins that still
        // floor the tree's WAL trim. Each task must absorb its own failure.
        var addressed = new ConcurrentBag<string>();
        var failing = WalMaterialiserPinRouting.ShardKey(Tree, Consumer, PinShards);

        var registry = Substitute.For<IWalCursorRegistry>();
        registry.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(callInfo =>
        {
            var key = callInfo.ArgAt<string>(0);
            var grain = Substitute.For<IWalMaterialiserPinGrain>();
            grain.RemoveAsync(Arg.Any<string>()).Returns(_ =>
            {
                addressed.Add(key);
                return key == failing
                    ? Task.FromException(new TimeoutException("pin shard unreachable"))
                    : Task.CompletedTask;
            });
            return grain;
        });

        var reporter = new LeafCursorReporter(registry, factory, Monitor());

        Assert.DoesNotThrowAsync(() => reporter.UnregisterAsync(Tree, Consumer, CancellationToken.None));

        Assert.That(
            addressed,
            Is.EquivalentTo(WalMaterialiserPinRouting.EnumerateReadKeys(Tree, PinShards)),
            "One faulting key must not prevent the others being attempted.");

        await Task.CompletedTask;
    }
}
