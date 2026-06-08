using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Saga-coordinator shutdown-refused fast-fail and quiesce-on-Saturated
/// tests. Covers the
/// three terminal-shutdown exception shapes the saga must detect
/// (typed <see cref="LatticeShuttingDownException"/>, untyped
/// <see cref="InvalidOperationException"/> carrying the
/// <c>WalDrainBudget</c> sentinel, and Orleans grain-rejection on
/// "Unable to create local activation"), the caller-facing
/// <see cref="LatticeShuttingDownException"/> surface that
/// <see cref="AtomicWriteGrain.ExecuteAsync"/> raises on the
/// shutdown-refused regime, and the silent-no-op fallback when no
/// <see cref="IWalSaturationSignal"/> is registered in DI.
/// </summary>
public partial class AtomicWriteGrainTests
{
    /// <summary>
    /// Builds a CreateGrain variant that registers a custom
    /// <see cref="IWalSaturationSignal"/> against the grain's
    /// <see cref="IGrainContext.ActivationServices"/>. Mirrors the
    /// base CreateGrain factory shape but seeds the activation
    /// service provider so the saga's
    /// <c>ResolveSaturationSignal</c> finds the signal on first
    /// lookup.
    /// </summary>
    private static (AtomicWriteGrain grain,
                     FakePersistentState<AtomicWriteState> state,
                     ILattice lattice,
                     IShardRootGrain shard) CreateGrainWithSignal(
        IWalSaturationSignal? signal,
        FakePersistentState<AtomicWriteState>? existingState = null,
        LatticeOptions? options = null,
        Microsoft.Extensions.Hosting.IHostApplicationLifetime? lifetime = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("atomic-write", $"{TreeId}/{OperationId}"));

        var sc = new ServiceCollection();
        if (signal is not null)
        {
            sc.AddSingleton(signal);
        }
        if (lifetime is not null)
        {
            sc.AddSingleton(lifetime);
        }
        context.ActivationServices.Returns(sc.BuildServiceProvider());

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        shard.GetRawEntryAsync(Arg.Any<string>())
            .Returns(Task.FromResult<LwwEntry?>(null));
        shard.GetRawEntriesAsync(Arg.Any<List<string>>())
            .Returns(async callInfo =>
            {
                var keys = (List<string>)callInfo[0];
                var results = new List<LwwEntry?>(keys.Count);
                foreach (var key in keys)
                {
                    var entry = await shard.GetRawEntryAsync(key);
                    results.Add(entry);
                }
                return results;
            });

        var opts = options ?? new LatticeOptions();
        var routing = new RoutingInfo(
            TreeId,
            ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount));
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(routing);
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(routing);

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var state = existingState ?? new FakePersistentState<AtomicWriteState>();

        var grain = new AtomicWriteGrain(
            context,
            grainFactory,
            reminderRegistry,
            optionsMonitor,
            new LoggerFactory().CreateLogger<AtomicWriteGrain>(),
            state);
        return (grain, state, lattice, shard);
    }

    // --- Shutdown-refused fast-fail: typed LatticeShuttingDownException ---

    [Test]
    public void ExecuteAsync_surfaces_LatticeShuttingDownException_when_SetManyAsync_throws_typed_shutdown()
    {
        // The saga's batched dispatch surfaces a
        // LatticeShuttingDownException (the writer-side drain refusal
        // shape after the writer-side typed-exception adoption).
        // The saga must detect this via
        // IsTerminalShutdownRefusal, short-circuit the retry loop,
        // and re-throw a LatticeShuttingDownException to its caller
        // (rather than a plain InvalidOperationException). The bench
        // and any other consumer can then detect the regime via a
        // single `is` check.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        var refusal = new LatticeShuttingDownException(
            "WAL append dispatch to tree 'X' partition 0 refused: the owning WalCommitLogWriter is shutting down (WalDrainBudget).");
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(refusal);

        var ex = Assert.ThrowsAsync<LatticeShuttingDownException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        Assert.That(ex!.Message, Does.Contain("silo is shutting down"),
            "Saga's outer throw must surface the typed exception with a message explaining the silo-shutting-down regime.");
        Assert.That(ex.InnerException, Is.SameAs(refusal),
            "Saga must preserve the original refusal as InnerException for log diagnostics.");
    }

    [Test]
    public async Task ExecuteAsync_does_not_retry_when_first_attempt_is_shutdown_refusal()
    {
        // Pre-shutdown-detection: the saga's catch ran the retry loop
        // (one attempt against the same drained writer, surfacing the
        // same exception). With shutdown-detection: the saga detects the
        // shutdown-refused shape and pivots to compensation on the
        // first attempt without spending retry budget.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(new LatticeShuttingDownException("WalDrainBudget refused"));

        Assert.ThrowsAsync<LatticeShuttingDownException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));

        // SetManyAsync called exactly once - retry budget was NOT consumed.
        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Shutdown-refused fast-fail: legacy untyped shape ---

    [Test]
    public void ExecuteAsync_detects_legacy_InvalidOperationException_WalDrainBudget_shape()
    {
        // Rolling-upgrade compatibility: a peer silo running an older
        // build raises the untyped InvalidOperationException(WalDrainBudget)
        // shape rather than the new LatticeShuttingDownException.
        // The saga's IsTerminalShutdownRefusal substring check must
        // still catch it.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        var legacyRefusal = new InvalidOperationException(
            "WAL append dispatch to tree 'X' partition 0 refused: WalDrainBudget.");
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(legacyRefusal);

        // The saga's outer throw is a LatticeShuttingDownException
        // wrapping the legacy InvalidOperationException as
        // InnerException - both shapes route through the same
        // hard fast-path.
        var ex = Assert.ThrowsAsync<LatticeShuttingDownException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        Assert.That(ex!.Message, Does.Contain("silo is shutting down"));
        Assert.That(ex.InnerException, Is.SameAs(legacyRefusal));
    }

    // --- Shutdown-refused fast-fail: Orleans message rejection ---

    [Test]
    public async Task ExecuteAsync_detects_OrleansMessageRejection_Unable_to_create_local_activation_shape()
    {
        // The Orleans runtime refuses to re-activate a grain that
        // has been deactivated as part of the same shutdown. The
        // saga's IsTerminalShutdownRefusal must detect the type-name
        // + substring shape and fast-fail rather than retry.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        // Substitute Orleans' OrleansMessageRejectionException with a
        // type-name lookalike so the test does not depend on
        // Orleans.Runtime internals at compile time. The detection
        // matches FullName + message substring, so any type that
        // matches both predicates is caught.
        var rejection = new FakeOrleansMessageRejectionException(
            "Forwarding failed: tried to forward message ... Unable to create local activation. Rejecting now.");
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(rejection);

        Assert.ThrowsAsync<LatticeShuttingDownException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Non-shutdown failures still follow the retry / compensate path ---

    [Test]
    public void ExecuteAsync_does_not_persist_state_on_shutdown_fast_path()
    {
        // The hard fast-path must NOT call WriteStateAsync after
        // detecting shutdown-refused - the Azure-Tables grain-storage
        // backend is the same backend the WAL writer just refused us
        // on, so a persist call would also race the drain and wedge
        // for the host's deactivation deadline. The persisted state
        // stays at Execute with the current NextIndex; the next silo
        // activation re-runs the saga from there.
        //
        // Test structure: simulate a pre-existing Execute-phase saga
        // (skipping the Prepare-phase write entirely) so the test
        // isolates the dispatch-failure-onward path. WriteCount must
        // stay at exactly 0 - any post-dispatch persist would race
        // the drain.
        var existing = new FakePersistentState<AtomicWriteState>();
        existing.State.Phase = AtomicWritePhase.Execute;
        existing.State.TreeId = TreeId;
        existing.State.Entries = MakeEntries(("k1", [1]));
        existing.State.PreValues = [new AtomicPreValue { Key = "k1" }];
        existing.State.NextIndex = 0;
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        existing.State.TouchedShards = [map.Resolve("k1")];
        existing.State.TransactionId = Guid.NewGuid();
        existing.State.KeyFingerprint = ComputeFingerprint(("k1", [1]));
        existing.State.AtomicBatchSize = 1;
        existing.State.SagaStartedAtTicks = DateTimeOffset.UtcNow.UtcTicks;

        var (grain, state, lattice, _) = CreateGrainWithSignal(signal: null, existingState: existing);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(new LatticeShuttingDownException("WalDrainBudget refused"));

        Assert.ThrowsAsync<LatticeShuttingDownException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));

        // The fast-path must NOT persist saga state, NOT call
        // BroadcastTerminalsAsync, NOT unregister reminders, and NOT
        // call CompleteSagaAsync. The only side-effect is the typed
        // throw to the caller. Persisted state stays at Execute with
        // NextIndex=0; the next silo activation re-runs from there.
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.Zero,
                "Hard fast-path must not persist saga state after the shutdown-refused throw - any post-dispatch write would race the host's deactivation deadline.");
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Execute),
                "Persisted phase must stay at Execute so the next silo activation re-runs the saga from where it left off.");
            Assert.That(state.State.NextIndex, Is.EqualTo(0),
                "NextIndex must stay at its pre-dispatch value for crash-resume correctness.");
            Assert.That(state.State.FailureMessage, Is.Null,
                "FailureMessage must stay null - the fast-path does NOT stamp the shutdown-refused sentinel into persisted state (the sentinel was a load-bearing artifact of the older compensate-pivot path; the hard fast-path makes it unnecessary).");
        });
    }

    [Test]
    public async Task ExecuteAsync_retries_on_non_shutdown_failure_then_compensates()
    {
        // Genuine business / storage failures must NOT trigger the
        // shutdown-refused fast-fail - they should retry up to
        // MaxRetriesPerStep then pivot to compensation with the
        // plain "failed" outcome.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(new InvalidOperationException("simulated storage transient failure"));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        // The thrown type must NOT be LatticeShuttingDownException -
        // genuine failures stay on the plain InvalidOperationException
        // surface to preserve the historical caller contract.
        Assert.That(ex, Is.Not.InstanceOf<LatticeShuttingDownException>(),
            "Non-shutdown failures must not be classified as shutdown-refused.");
        // SetManyAsync should be called twice (initial + one retry per MaxRetriesPerStep=1).
        await lattice.Received(2).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Quiesce-on-Saturated gate ---

    [Test]
    public async Task ExecuteAsync_quiesces_before_SetManyAsync_when_signal_reports_Saturated()
    {
        // When the saturation signal reports Saturated, the saga
        // awaits WaitForHealthyAsync before dispatching. The mock
        // signal must observe both calls (GetCurrentState and
        // WaitForHealthyAsync) in that order.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1])));

        Received.InOrder(() =>
        {
            signal.GetCurrentState(TreeId);
            signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task ExecuteAsync_does_not_await_WaitForHealthyAsync_when_signal_reports_Healthy()
    {
        // When the signal reports Healthy (the steady state), the
        // saga must not call WaitForHealthyAsync - the gate is a
        // pure fast-path read on the cached state.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Healthy);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1])));

        signal.Received().GetCurrentState(TreeId);
        await signal.DidNotReceive().WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_does_not_await_WaitForHealthyAsync_when_signal_reports_Throttled()
    {
        // Throttled is the natural lead-up regime under the recovery-
        // window upgrade. Dispatching through it is correct - new
        // appends will land, possibly after a brief admission wait.
        // The saga must NOT park on WaitForHealthyAsync here (which
        // would unnecessarily inflate saga latency by one sampler
        // tick on every Throttled-classified call).
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Throttled);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1])));

        await signal.DidNotReceive().WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_silently_skips_quiesce_gate_when_no_signal_registered()
    {
        // Hosts that did not register a saturation sampler (single-
        // node deployments, unit tests, custom AddLattice
        // configurations) must continue to work without the signal.
        // The saga falls back to its pre-shutdown-detection behaviour:
        // dispatch
        // every batch immediately, retry on failure.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        // No throw - the saga completes normally.
        await grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1])));
    }

    // --- IsTerminalShutdownRefusal exception-walk coverage ---

    [Test]
    public async Task ExecuteAsync_detects_shutdown_refusal_wrapped_in_AggregateException()
    {
        // The saga's IsTerminalShutdownRefusal walks any
        // AggregateException to find a shutdown shape in the inner
        // exceptions. ILattice.SetManyAsync's shard-bucketing fan-out
        // uses Task.WhenAll which surfaces the first faulted task's
        // exception unwrapped on the await, but the saga's compensation
        // path or a downstream Orleans serializer might re-wrap in
        // an AggregateException - pin the walk-into-inners behavior.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        var inner = new LatticeShuttingDownException("inner: writer is draining");
        var aggregate = new AggregateException("outer: aggregate wrapping shutdown", inner);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(aggregate);

        // The fast-path must fire on the wrapped shape and throw
        // LatticeShuttingDownException (not the unhandled AggregateException
        // or InvalidOperationException for compensate-pivot).
        Assert.ThrowsAsync<LatticeShuttingDownException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));

        // SetManyAsync called exactly once - the wrapped detection
        // must short-circuit the retry loop too.
        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ExecuteAsync_detects_shutdown_refusal_wrapped_in_InnerException_chain()
    {
        // The walk also follows Exception.InnerException, not just
        // AggregateException.InnerExceptions. Pin the chain-walk.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        var inner = new LatticeShuttingDownException("inner: writer is draining");
        var outerWrap = new InvalidOperationException("outer: generic wrapper", inner);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(outerWrap);

        Assert.ThrowsAsync<LatticeShuttingDownException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Shutdown-aware quiesce gate ---

    [Test]
    public async Task ExecuteAsync_does_not_park_on_quiesce_when_host_is_already_stopping()
    {
        // When the host has already signalled ApplicationStopping
        // (the typical "saga is in flight when SIGTERM arrives"
        // scenario), the saga's quiesce gate must short-circuit
        // immediately - the saturation signal will never return to
        // Healthy under shutdown (the writer is draining, not
        // draining briefly), and waiting MaxSagaQuiesceWait per
        // saga would compound into a multi-second drain tail on
        // every in-flight saga at SIGTERM. The short-circuit lets
        // the saga's next dispatch surface
        // LatticeShuttingDownException immediately via the hard
        // fast-path.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);
        // Set up WaitForHealthyAsync to NEVER complete - if the
        // saga incorrectly parks on it, the test will time out.
        // Setting up the expectation lets us assert it was never
        // awaited.
        signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(_ => new TaskCompletionSource<object>().Task);

        var lifetime = Substitute.For<Microsoft.Extensions.Hosting.IHostApplicationLifetime>();
        using var stopped = new CancellationTokenSource();
        stopped.Cancel(); // Application is ALREADY stopping.
        lifetime.ApplicationStopping.Returns(stopped.Token);

        // Stub SetManyAsync to throw LatticeShuttingDownException so
        // the dispatch surfaces the typed exception after the
        // (skipped) quiesce gate.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal, lifetime: lifetime);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(new LatticeShuttingDownException("writer drained"));

        var sw = System.Diagnostics.Stopwatch.StartNew();
        Assert.ThrowsAsync<LatticeShuttingDownException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        sw.Stop();

        // The total saga wall-clock must be far below
        // MaxSagaQuiesceWait (5 s). Allow generous CI margin: 2 s
        // accommodates a cold-JIT pass without false-positive flakes
        // while still asserting that we did NOT park on the gate.
        Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(2)),
            "saga must short-circuit the quiesce gate when ApplicationStopping is requested - waiting the full MaxSagaQuiesceWait per saga would compound into a multi-second drain tail at SIGTERM");
        // WaitForHealthyAsync must NOT have been called.
        await signal.DidNotReceive().WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_quiesce_wait_bails_when_host_starts_stopping_mid_wait()
    {
        // The quiesce wait must link the host's ApplicationStopping
        // token so a SIGTERM that fires DURING the wait bails
        // immediately instead of running out the full
        // MaxSagaQuiesceWait budget. Equivalent contract to the
        // already-stopping case but exercises the linked-CTS path.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(TreeId).Returns(WalSaturationState.Saturated);

        // WaitForHealthyAsync respects the token and completes via
        // OCE when the linked CTS fires.
        signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var token = (CancellationToken)callInfo[1];
                return Task.Delay(Timeout.Infinite, token);
            });

        var lifetime = Substitute.For<Microsoft.Extensions.Hosting.IHostApplicationLifetime>();
        using var stopping = new CancellationTokenSource();
        lifetime.ApplicationStopping.Returns(stopping.Token);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal, lifetime: lifetime);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(new LatticeShuttingDownException("writer drained"));

        // Kick off ExecuteAsync on a background task, then signal
        // ApplicationStopping after a small delay so the quiesce
        // wait is observed mid-flight.
        var executeTask = Task.Run(() => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        await Task.Delay(150);
        var preCancelElapsed = System.Diagnostics.Stopwatch.GetTimestamp();
        stopping.Cancel();

        // The saga must bail quickly after the cancel - well before
        // MaxSagaQuiesceWait (5 s) would have elapsed.
        Assert.ThrowsAsync<LatticeShuttingDownException>(async () => await executeTask);
        var postCancelElapsed = System.Diagnostics.Stopwatch.GetElapsedTime(preCancelElapsed);
        Assert.That(postCancelElapsed, Is.LessThan(TimeSpan.FromSeconds(2)),
            "saga must observe the linked ApplicationStopping token and bail mid-wait rather than running out the full quiesce budget");
    }

    /// <summary>
    /// Fake <see cref="Exception"/> whose <see cref="System.Type.FullName"/>
    /// matches the Orleans rejection type the saga detects by
    /// substring. Lets the test verify the detection path without
    /// taking a direct dependency on Orleans.Runtime internals.
    /// </summary>
    private sealed class FakeOrleansMessageRejectionException : Exception
    {
        public FakeOrleansMessageRejectionException(string message) : base(message) { }
    }
}