using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the writer-side provider-failure counter
/// (<c>WalCommitLogWriter._providerFailureCounts</c>) consumed by
/// <see cref="WalSaturationSampler"/> as the third Saturated input.
/// The counter captures the regime where the downstream provider's
/// commit calls return quickly (so neither the admission depth nor
/// the dispatch deadline crosses the threshold) but terminally fail
/// at a high rate - the canonical Azure-Tables-single-account
/// 409-Conflict burst.
/// </summary>
[TestFixture]
public class WalCommitLogWriterProviderFailureTests
{
    private const string TreeId = "tree-providerfail";

    [SetUp]
    public void SetUp()
    {
        // Hermetic isolation: each test starts with empty counter
        // dictionaries so cross-test parallelism cannot share static
        // state.
        WalCommitLogWriter._providerFailureCounts.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
    }

    private static WalCommitLogWriter CreateWriter(
        IWalShardGrain shard,
        LatticeOptions? options = null,
        string clusterId = "site-test")
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns(clusterId);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: optionsMonitor.Get(string.Empty), factory: grainFactory);
        return new WalCommitLogWriter(grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver);
    }

    private static WalRecord MakeMutation(string key = "k") => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-test",
    };

    // --- Single-entry path ---

    [Test]
    public void AppendAsync_provider_throw_increments_per_tree_shard_counter()
    {
        // The writer's broad catch must increment the per-(tree, shard)
        // provider-failure counter when the downstream grain RPC throws
        // a non-timeout, non-cancellation exception. This is the
        // canonical 409-Conflict shape the saturation sampler reads.
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(new InvalidOperationException("simulated provider 409-Conflict")));

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromSeconds(30),
        });

        Assert.That(
            async () => await writer.AppendAsync(MakeMutation()),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("simulated provider 409-Conflict"));

        var hits = WalCommitLogWriter._providerFailureCounts
            .Where(kv => kv.Key.TreeId == TreeId)
            .ToList();
        Assert.That(hits, Has.Count.EqualTo(1),
            "exactly one (tree, shard) slot must be incremented by a single provider failure");
        Assert.That(hits[0].Value, Is.EqualTo(1L),
            "first failure on a fresh slot must initialise the cumulative count to 1");
    }

    [Test]
    public async Task AppendAsync_caller_cancellation_does_not_increment_counter()
    {
        // A caller-driven OperationCanceledException whose token
        // matches the caller's CT is excluded from the counter so a
        // healthy caller-side abandonment never inflates the
        // saturation signal. The exclusion is critical because the
        // saga coordinator itself cancels in-flight RPCs as part of
        // normal control flow.
        var release = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        // The grain observes the cancellation token and throws an
        // OCE with that token when cancelled.
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var token = (CancellationToken)callInfo[1];
                return Task.Run(async () =>
                {
                    await Task.Delay(Timeout.Infinite, token);
                    return 0L;
                });
            });

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMinutes(5),
        });

        using var callerCts = new CancellationTokenSource();
        var append = writer.AppendAsync(MakeMutation(), callerCts.Token);
        await Task.Delay(50);
        callerCts.Cancel();

        Assert.That(async () => await append, Throws.InstanceOf<OperationCanceledException>());

        var hits = WalCommitLogWriter._providerFailureCounts
            .Where(kv => kv.Key.TreeId == TreeId)
            .ToList();
        Assert.That(hits, Is.Empty,
            "caller-driven cancellation must not inflate the provider-failure counter");
    }

    [Test]
    public void AppendAsync_LatticeShuttingDownException_does_not_increment_counter()
    {
        // The shutdown-back-pressure regime is its own typed surface
        // (LatticeShuttingDownException is the caller-detection
        // mechanism); counting it as "provider failure" would conflate
        // the steady-state saturation regime with the one-way
        // shutdown regime. The exclusion catches both this writer's
        // own drain refusal (re-thrown from a nested call) and a
        // downstream peer-silo's drain refusal that surfaces via
        // Orleans grain serialization.
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(new LatticeShuttingDownException(
                "downstream peer-silo writer is draining (WalDrainBudget)")));

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromSeconds(30),
        });

        Assert.That(
            async () => await writer.AppendAsync(MakeMutation()),
            Throws.InstanceOf<LatticeShuttingDownException>());

        var hits = WalCommitLogWriter._providerFailureCounts
            .Where(kv => kv.Key.TreeId == TreeId)
            .ToList();
        Assert.That(hits, Is.Empty,
            "LatticeShuttingDownException must not inflate the provider-failure counter - the shutdown regime is its own surface");
    }

    [Test]
    public void AppendAsync_repeated_provider_failures_accumulate_per_tree_shard_count()
    {
        // The counter is cumulative across the lifetime of the writer
        // singleton; the sampler subtracts the prior tick's reading to
        // derive a per-window delta. Pins the "monotonic counter" half
        // of that contract.
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(new InvalidOperationException("simulated provider failure")));

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromSeconds(30),
        });

        Assert.That(async () => await writer.AppendAsync(MakeMutation()),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(async () => await writer.AppendAsync(MakeMutation()),
            Throws.InstanceOf<InvalidOperationException>());

        var hits = WalCommitLogWriter._providerFailureCounts
            .Where(kv => kv.Key.TreeId == TreeId)
            .ToList();
        Assert.That(hits, Has.Count.EqualTo(1),
            "two failures on the same key must hit a single (tree, shard) slot");
        Assert.That(hits[0].Value, Is.EqualTo(2L),
            "the cumulative counter must accumulate every failure for the same slot");
    }

    [Test]
    public void AppendAsync_dispatch_timeout_does_not_double_count_into_provider_failure()
    {
        // The dispatch-deadline path increments its own counter
        // (_dispatchTimeoutCounts) and throws TimeoutException. The
        // TimeoutException is then re-thrown OUT of the bounded-
        // deadline branch and does NOT fall through to the provider-
        // failure broad-catch (the deadline catch returns before
        // reaching the broad catch). Pins that the two counters are
        // not double-counting the same failure.
        var release = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(release.Task);

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
        });

        Assert.That(
            async () => await writer.AppendAsync(MakeMutation()),
            Throws.TypeOf<TimeoutException>()
                .With.Message.Contains(nameof(LatticeOptions.WalAppendDispatchTimeout)));

        // Dispatch-timeout counter increments by 1.
        Assert.That(
            WalCommitLogWriter._dispatchTimeoutCounts.Where(kv => kv.Key.TreeId == TreeId).Sum(kv => kv.Value),
            Is.EqualTo(1L),
            "the dispatch-deadline trip must increment its own counter");

        // Provider-failure counter must NOT increment.
        Assert.That(
            WalCommitLogWriter._providerFailureCounts.Where(kv => kv.Key.TreeId == TreeId).Sum(kv => kv.Value),
            Is.EqualTo(0L),
            "the dispatch-deadline path must NOT cross-contaminate the provider-failure counter");

        release.TrySetResult(0L);
    }

    // --- Batched path ---

    [Test]
    public void AppendManyAsync_provider_throw_increments_per_tree_shard_counter()
    {
        // The batched-path broad catch must also feed the counter so a
        // saga that issues a multi-key SetManyAsync gets the same
        // saturation attribution as a single-key SetAsync.
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<long>>(new InvalidOperationException("batched 409-Conflict")));

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromSeconds(30),
        });

        var entries = new[] { MakeMutation("a"), MakeMutation("b") };
        Assert.That(async () => await writer.AppendManyAsync(entries),
            Throws.InstanceOf<InvalidOperationException>());

        var hits = WalCommitLogWriter._providerFailureCounts
            .Where(kv => kv.Key.TreeId == TreeId)
            .ToList();
        // Two-entry batch may route to one or two distinct partitions
        // depending on the partition hash; assert at least one slot
        // was incremented to attribute the failure.
        Assert.That(hits, Has.Count.GreaterThanOrEqualTo(1),
            "batched-path failure must increment at least one (tree, shard) slot");
        Assert.That(hits.Sum(kv => kv.Value), Is.GreaterThanOrEqualTo(1L),
            "batched-path failure must register at least one increment");
    }

    [Test]
    public void AppendManyAsync_LatticeShuttingDownException_does_not_increment_counter()
    {
        // Batched-path equivalent of the single-entry exclusion test.
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<long>>(new LatticeShuttingDownException("peer-silo draining")));

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromSeconds(30),
        });

        var entries = new[] { MakeMutation("a"), MakeMutation("b") };
        Assert.That(async () => await writer.AppendManyAsync(entries),
            Throws.InstanceOf<LatticeShuttingDownException>());

        var hits = WalCommitLogWriter._providerFailureCounts
            .Where(kv => kv.Key.TreeId == TreeId)
            .ToList();
        Assert.That(hits, Is.Empty,
            "LatticeShuttingDownException on the batched path must not inflate the provider-failure counter either");
    }

    // --- GetTracker drain gate ---

    [Test]
    public async Task GetTracker_drain_gate_throws_typed_LatticeShuttingDownException()
    {
        // Pin the post-drain refusal type: any new AppendAsync after
        // DrainAsync must throw LatticeShuttingDownException (not the
        // legacy InvalidOperationException) so caller-side detection
        // is a single `is` check. Mirrors the historical contract on
        // the WalDrainBudget message text via the typed surface.
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));

        var writer = CreateWriter(shard, new LatticeOptions());
        await writer.DrainAsync(CancellationToken.None);

        Assert.That(
            async () => await writer.AppendAsync(MakeMutation()),
            Throws.InstanceOf<LatticeShuttingDownException>()
                .With.Message.Contains(nameof(LatticeOptions.WalDrainBudget)),
            "GetTracker's post-drain refusal must surface as the typed LatticeShuttingDownException for caller-side `is` checks");
    }

    [Test]
    public async Task GetTracker_drain_gate_typed_exception_is_also_InvalidOperationException()
    {
        // The typed exception derives from InvalidOperationException
        // so existing catch handlers that match on
        // InvalidOperationException continue to absorb it without a
        // forced rewrite. This is the subclass-compatibility half of
        // the typed-exception contract.
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));

        var writer = CreateWriter(shard, new LatticeOptions());
        await writer.DrainAsync(CancellationToken.None);

        Assert.That(
            async () => await writer.AppendAsync(MakeMutation()),
            Throws.InstanceOf<InvalidOperationException>(),
            "legacy InvalidOperationException catch handlers must still catch the typed shutdown exception");
    }
}