using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the pre-admission saturation gate inside
/// <see cref="WalCommitLogWriter"/>. The gate fires before the
/// per-partition admission semaphore and refuses fast with
/// <see cref="LatticeSaturatedException"/> when the per-tree
/// <see cref="IWalSaturationSignal"/> stays
/// <see cref="WalSaturationState.Saturated"/> for longer than
/// <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/>,
/// so callers see the back-pressure in budget time instead of parking
/// on the admission semaphore for
/// <see cref="LatticeOptions.WalAppendDispatchTimeout"/>.
/// </summary>
[TestFixture]
public class WalCommitLogWriterAdmissionSaturationTests
{
    private const string TreeId = "tree-admsat";

    [SetUp]
    public void SetUp()
    {
        WalCommitLogWriter._trackers.Clear();
        WalCommitLogWriter._providerFailureCounts.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
    }

    private static WalCommitLogWriter CreateWriter(
        IWalShardGrain shard,
        IWalSaturationSignal? signal,
        LatticeOptions? options = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");

        var optionsResolver = TestOptionsResolver.Create(baseOptions: optionsMonitor.Get(string.Empty), factory: grainFactory);
        return new WalCommitLogWriter(grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver, signal);
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

    private static IWalShardGrain CreateHealthyShard()
    {
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(0L));
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var list = (IReadOnlyList<WalRecord>)callInfo[0];
                var offsets = new long[list.Count];
                for (var i = 0; i < offsets.Length; i++) offsets[i] = i;
                return Task.FromResult<IReadOnlyList<long>>(offsets);
            });
        return shard;
    }

    // --- Signal not registered (no-op fast path) ---

    [Test]
    public async Task AppendAsync_no_signal_registered_proceeds_to_admission_as_normal()
    {
        // The signal is an optional ctor dependency; unit-test
        // deployments build the writer without it. The gate must be
        // a silent no-op in that case so existing test fixtures (the
        // 9 sites in test/lattice/) continue to work unchanged.
        var shard = CreateHealthyShard();
        var writer = CreateWriter(shard, signal: null, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(100),
        });

        var offset = await writer.AppendAsync(MakeMutation());

        Assert.That(offset, Is.EqualTo(0L),
            "no-signal path must dispatch through to the shard as if the gate did not exist");
    }

    // --- Signal Healthy / Throttled (no-op fast path) ---

    [Test]
    public async Task AppendAsync_signal_Healthy_does_not_call_WaitForHealthyAsync()
    {
        // Healthy fast path is one ConcurrentDictionary lookup and a
        // direct return. The wait-for-healthy await must not fire
        // because that would force every healthy dispatch to allocate
        // a TCS for nothing.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Healthy);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(100),
        });

        await writer.AppendAsync(MakeMutation());

        await signal.DidNotReceive().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AppendAsync_signal_Throttled_does_not_call_WaitForHealthyAsync()
    {
        // Throttled is the natural lead-up regime under the recovery-
        // window upgrade. The gate must not park on WaitForHealthyAsync
        // for Throttled because that would over-pressure the writer
        // for any tree that crosses the Throttled threshold ever.
        // Only Saturated triggers the gate.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Throttled);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(100),
        });

        await writer.AppendAsync(MakeMutation());

        await signal.DidNotReceive().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // --- Signal Saturated, budget = Zero (gate disabled) ---

    [Test]
    public async Task AppendAsync_signal_Saturated_with_zero_budget_does_not_call_WaitForHealthyAsync()
    {
        // Budget=Zero is the operator opt-out: the gate is bypassed
        // entirely (the historical pre-admission-gate behaviour),
        // so even a Saturated tree does not park on
        // WaitForHealthyAsync.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.Zero,
        });

        await writer.AppendAsync(MakeMutation());

        await signal.DidNotReceive().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // --- Signal Saturated, signal recovers within budget (proceed) ---

    [Test]
    public async Task AppendAsync_signal_Saturated_then_recovers_within_budget_proceeds_to_admission()
    {
        // The signal observes Saturated at the time the gate fires
        // but WaitForHealthyAsync completes (signal recovered) within
        // the budget. The caller proceeds into the admission semaphore
        // as if the gate had not fired.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);
        // Recovery completes immediately.
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.FromSeconds(5),
        });

        var offset = await writer.AppendAsync(MakeMutation());

        Assert.That(offset, Is.EqualTo(0L));
        await signal.Received(1).WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // --- Signal Saturated, budget elapses without recovery (refuse) ---

    [Test]
    public void AppendAsync_signal_Saturated_past_budget_throws_LatticeSaturatedException()
    {
        // The canonical failure shape: the per-tree signal stays
        // Saturated for longer than WalAdmissionSaturationWaitBudget,
        // and a re-read after budget expiry confirms the regime
        // persists. The caller must observe LatticeSaturatedException
        // in budget time, NOT a TimeoutException, and the exception
        // must carry the originating tree id.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);
        // The wait never completes within the budget; it surfaces an
        // OCE when the linked CTS fires.
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var ct = (CancellationToken)callInfo[1];
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                ct.Register(() => tcs.TrySetCanceled(ct));
                return tcs.Task;
            });

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(50),
        });

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await writer.AppendAsync(MakeMutation()));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo(TreeId),
                "the refusal must carry the originating tree id for caller-side attribution");
            Assert.That(ex.Message, Does.Contain(nameof(LatticeOptions.WalAdmissionSaturationWaitBudget)),
                "the refusal message must name the budget option so operators can find the lever");
        });
    }

    // --- Signal Saturated, signal recovers AFTER budget but BEFORE re-check (race) ---

    [Test]
    public async Task AppendAsync_signal_Saturated_but_recovered_at_recheck_suppresses_refusal()
    {
        // The wait expires AND the re-check after expiry observes
        // the tree at Healthy (the signal recovered between the wait
        // expiring and the re-check firing). The gate must suppress
        // the refusal so a borderline recovery is not penalised - the
        // caller proceeds into the admission semaphore as normal.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        // First call returns Saturated (drives the wait), second
        // returns Healthy (the recheck after budget expiry).
        signal.GetCurrentState(Arg.Any<string>())
            .Returns(WalSaturationState.Saturated, WalSaturationState.Healthy);
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var ct = (CancellationToken)callInfo[1];
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                ct.Register(() => tcs.TrySetCanceled(ct));
                return tcs.Task;
            });

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(50),
        });

        // Must not throw - the borderline recovery path suppresses
        // the refusal.
        var offset = await writer.AppendAsync(MakeMutation());
        Assert.That(offset, Is.EqualTo(0L));
    }

    // --- Caller cancellation wins over budget refusal ---

    [Test]
    public void AppendAsync_caller_cancellation_during_saturation_wait_surfaces_OperationCanceledException()
    {
        // When the caller's CT cancels during the saturation wait
        // (rather than the budget expiring), the gate must surface
        // OperationCanceledException carrying the caller's token, not
        // LatticeSaturatedException. The saga coordinator (the
        // canonical caller) detects this as caller-driven
        // abandonment and pivots accordingly.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var ct = (CancellationToken)callInfo[1];
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                ct.Register(() => tcs.TrySetCanceled(ct));
                return tcs.Task;
            });

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.FromSeconds(30),
        });

        using var cts = new CancellationTokenSource();
        var task = writer.AppendAsync(MakeMutation(), cts.Token);
        cts.CancelAfter(TimeSpan.FromMilliseconds(20));

        // TaskCanceledException : OperationCanceledException - either
        // shape satisfies the contract that caller-driven cancellation
        // does NOT surface as LatticeSaturatedException.
        var ex = Assert.CatchAsync(async () => await task);
        Assert.That(ex, Is.InstanceOf<OperationCanceledException>(),
            "caller cancellation during the saturation wait must surface OperationCanceledException, not LatticeSaturatedException");
        Assert.That(ex, Is.Not.InstanceOf<LatticeSaturatedException>(),
            "the typed saturation refusal must not absorb caller-driven cancellation");
    }

    // --- Batched-path coverage (mirror of single-entry path) ---

    [Test]
    public void AppendBatchAsync_signal_Saturated_past_budget_throws_LatticeSaturatedException()
    {
        // The batched-path gate is wired identically to the single-
        // entry path. A separate test pins the symmetry so a future
        // refactor that drops the gate from one path lights up here.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var ct = (CancellationToken)callInfo[1];
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                ct.Register(() => tcs.TrySetCanceled(ct));
                return tcs.Task;
            });

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(50),
        });

        var batch = new List<WalRecord> { MakeMutation("a"), MakeMutation("b") };
        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await writer.AppendManyAsync(batch));
        Assert.That(ex!.TreeId, Is.EqualTo(TreeId));
    }
}
