using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the per-silo hosted-service wrapper that wires
/// <see cref="WalCommitLogWriter.DrainAsync"/> into the host's
/// shutdown lifecycle. The drainer is the seam that makes
/// the writer-side drain automatic on host stop, so a host
/// that registers <c>AddLattice(...)</c> gets bounded-time
/// shutdown for free without operator intervention.
/// </summary>
[TestFixture]
public class WalCommitLogWriterDrainerTests
{
    [SetUp]
    public void SetUp()
    {
        // Hygiene: reset the static per-(tree, partition) tracker map
        // so stale PendingAppend stamps from a prior test do not skew
        // the StallWatchdog's heap walk. Drain state lives on the
        // per-writer instance, so isolation between tests does not
        // depend on this Clear. Each test also uses a unique tree id.
        WalCommitLogWriter._trackers.Clear();
        _treeId = $"tree-drainer-{Interlocked.Increment(ref _treeIdSeed)}";
    }

    private static int _treeIdSeed;
    private string _treeId = null!;

    private WalCommitLogWriter CreateWalWriter(out TaskCompletionSource<long> heldRelease)
    {
        var options = new LatticeOptions
        {
            WalMaxPendingBatches = 1,
            WalAppendDispatchTimeout = TimeSpan.FromMinutes(5),
            WalDrainBudget = TimeSpan.FromMilliseconds(250),
        };
        // The shard substitute returns a never-completing Task<long> for
        // AppendAsync so the held caller stays parked on the shard RPC
        // (filling the cap=1 admission slot) and the next caller parks
        // on the admission semaphore - the saturation shape the drain
        // seam closes. Test cleanup releases the TCS at the end so the
        // abandoned shard-RPC task settles before the fixture tears
        // down.
        heldRelease = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(heldRelease.Task);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");

        var optionsResolver = TestOptionsResolver.Create(baseOptions: options, factory: grainFactory);
        return new WalCommitLogWriter(grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver);
    }

    private WalRecord MakeMutation(string key) => new()
    {
        TreeId = _treeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-test",
    };

    [Test]
    public async Task StartAsync_is_a_no_op()
    {
        // The drainer's StartAsync intentionally does nothing - the
        // writer is constructed lazily by DI on first use and the drain
        // seam is purely passive until StopAsync fires. This test pins
        // that StartAsync returns a completed task synchronously with
        // no side effects on the underlying writer.
        var writer = CreateWalWriter(out var heldRelease);
        var drainer = new WalCommitLogWriterDrainer(writer, NullLogger<WalCommitLogWriterDrainer>.Instance);

        await drainer.StartAsync(CancellationToken.None);

        // Writer is still usable post-Start: a fresh append acquires its
        // admission slot normally. We don't await the append (the
        // substitute hangs forever); we just observe that it didn't
        // throw on dispatch.
        var append = writer.AppendAsync(MakeMutation("k"));
        Assert.That(append.IsFaulted, Is.False, "StartAsync must not put the writer into a drained / faulted state");

        // Release the parked shard-RPC substitute so the abandoned
        // append task can settle before the fixture tears down.
        heldRelease.TrySetResult(0L);
    }

    [Test]
    public async Task StopAsync_invokes_DrainAsync_on_underlying_WalCommitLogWriter()
    {
        // The host-stop path: StopAsync must invoke DrainAsync on the
        // writer so every parked admission caller is released within
        // bounded time of the host shutdown signal. Verified by parking
        // a caller on the writer's admission semaphore, then calling
        // StopAsync and asserting the parked caller surfaces a typed
        // TimeoutException naming WalDrainBudget - the canonical
        // attribution string the drain emits.
        var writer = CreateWalWriter(out var heldRelease);
        var drainer = new WalCommitLogWriterDrainer(writer, NullLogger<WalCommitLogWriterDrainer>.Instance);
        await drainer.StartAsync(CancellationToken.None);

        // Park a caller. Two appends on the same key route to the same
        // partition; the first acquires the only admission slot
        // (cap=1), the second parks.
        const string SharedKey = "shared-key";
        var held = writer.AppendAsync(MakeMutation(SharedKey));
        var parked = writer.AppendAsync(MakeMutation(SharedKey));
        await Task.Delay(80);
        Assert.That(parked.IsCompleted, Is.False, "parked caller should be on the admission semaphore");

        await drainer.StopAsync(CancellationToken.None);

        Assert.That(
            async () => await parked,
            Throws.InstanceOf<TimeoutException>()
                .With.Message.Contains(nameof(LatticeOptions.WalDrainBudget)),
            "StopAsync must invoke DrainAsync so parked admission callers surface a typed TimeoutException naming WalDrainBudget; if it stops at no-op the parked caller stays parked forever and the host's bounded shutdown grace would expire");

        heldRelease.TrySetResult(0L);
    }

    [Test]
    public async Task StopAsync_is_a_no_op_when_writer_is_not_WalCommitLogWriter()
    {
        // Hosts that replace ICommitLogWriter with a non-WalCommitLogWriter
        // implementation (in-process fakes, test doubles, future
        // alternates) opt out of the writer-side drain contract. The
        // drainer must safely no-op in that case so AddLattice's
        // unconditional hosted-service registration does not break
        // those hosts.
        var nonWalWriter = Substitute.For<ICommitLogWriter>();
        var drainer = new WalCommitLogWriterDrainer(nonWalWriter, NullLogger<WalCommitLogWriterDrainer>.Instance);

        // Both StartAsync and StopAsync should complete synchronously
        // without touching the substitute.
        await drainer.StartAsync(CancellationToken.None);
        await drainer.StopAsync(CancellationToken.None);

        // Substitute received no calls - the drainer correctly
        // recognised it as a non-WalCommitLogWriter and did not invoke
        // any methods on it.
        _ = nonWalWriter.DidNotReceiveWithAnyArgs().AppendAsync(default!, default);
        _ = nonWalWriter.DidNotReceiveWithAnyArgs().AppendManyAsync(default!, default);
    }

    [Test]
    public async Task StopAsync_is_idempotent_when_called_twice()
    {
        // StopAsync can be invoked twice if the host's lifecycle is
        // shut down by both a SIGTERM and an explicit
        // IHostApplicationLifetime.StopApplication() call. The second
        // invocation must not throw, must not double-release any
        // tracker, and must complete promptly.
        var writer = CreateWalWriter(out var heldRelease);
        var drainer = new WalCommitLogWriterDrainer(writer, NullLogger<WalCommitLogWriterDrainer>.Instance);
        await drainer.StartAsync(CancellationToken.None);

        const string SharedKey = "shared-key";
        var held = writer.AppendAsync(MakeMutation(SharedKey));
        var parked = writer.AppendAsync(MakeMutation(SharedKey));
        await Task.Delay(80);

        await drainer.StopAsync(CancellationToken.None);
        // Second invocation must be a clean no-op.
        Assert.That(async () => await drainer.StopAsync(CancellationToken.None), Throws.Nothing,
            "the second StopAsync must no-op rather than throwing or asserting on already-drained state");

        Assert.That(
            async () => await parked,
            Throws.InstanceOf<TimeoutException>()
                .With.Message.Contains(nameof(LatticeOptions.WalDrainBudget)));

        heldRelease.TrySetResult(0L);
    }
}
