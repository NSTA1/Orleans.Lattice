using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Saga-coordinator quiesce-on-Saturated tests covering the
/// admission-budget consumer-coverage behaviour: the saga's quiesce
/// budget is now derived from
/// <c>min(MaxSagaQuiesceWait, perTree.WalAppendDispatchTimeout)</c>
/// (the historical pre-saturation-fast-path saga used a fixed 5
/// seconds), and on budget exhaustion with the tree still
/// <see cref="WalSaturationState.Saturated"/> the
/// saga raises <see cref="LatticeSaturatedException"/> to the caller
/// rather than re-dispatching the same RowKeys into a still-throttled
/// account (which is the single-account 409-Conflict amplification
/// regime documented in <c>benchmark/azure-throughput/throughput.md</c>
/// section 32).
/// </summary>
public partial class AtomicWriteGrainTests
{
    // --- Healthy / Throttled fast paths: no quiesce wait ---

    [Test]
    public async Task ExecuteAsync_signal_Healthy_does_not_call_WaitForHealthyAsync()
    {
        // Healthy fast path is one ConcurrentDictionary lookup; the
        // saga proceeds directly into SetManyAsync without awaiting
        // WaitForHealthyAsync.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Healthy);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await signal.DidNotReceive().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ExecuteAsync_signal_Throttled_does_not_call_WaitForHealthyAsync()
    {
        // Throttled is the natural lead-up regime under the recovery-
        // window upgrade; the saga must not park on WaitForHealthyAsync
        // for it. Only Saturated triggers the quiesce gate.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Throttled);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await signal.DidNotReceive().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // --- Saturated, signal recovers within budget (proceed) ---

    [Test]
    public async Task ExecuteAsync_signal_Saturated_then_recovers_proceeds_to_SetManyAsync()
    {
        // The signal observes Saturated at the time the quiesce gate
        // fires but recovery completes within the budget. The saga
        // proceeds into SetManyAsync as if the gate had not fired.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await signal.Received().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Saturated past budget: refuse with LatticeSaturatedException ---

    [Test]
    public void ExecuteAsync_signal_Saturated_past_budget_throws_LatticeSaturatedException()
    {
        // The canonical saga-quiesce-budget failure shape. The signal stays
        // Saturated throughout the quiesce budget, AND a re-read at
        // budget expiry confirms the regime persists. The saga must
        // refuse with LatticeSaturatedException carrying the
        // originating tree id, NOT proceed into SetManyAsync (which
        // would amplify the 409-Conflict burst by re-entering the
        // same RowKeys into a still-throttled account).
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

        // Use a short dispatch timeout so the saga's effective budget
        // is min(MaxSagaQuiesceWait, perTree.WalAppendDispatchTimeout)
        // = perTree.WalAppendDispatchTimeout = 50 ms. Keeps the test
        // wall-clock under 1 s.
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal, options: new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
        });

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k", [1]))));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo(TreeId),
                "the saga's refusal must carry the originating tree id for caller-side attribution");
            Assert.That(ex.Message, Does.Contain("saga"),
                "the saga's refusal message must name the saga so log diagnostics can distinguish the saga's quiesce refusal from the writer-side admission refusal");
        });
    }

    [Test]
    public async Task ExecuteAsync_signal_Saturated_past_budget_does_not_dispatch_SetManyAsync()
    {
        // The whole point of the gate: refusing the dispatch prevents
        // the saga from re-entering the same RowKeys into a
        // still-throttled account. The downstream SetManyAsync must
        // never be invoked when the quiesce gate refuses.
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

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal, options: new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
        });

        // Catch the refusal silently; we want to verify SetManyAsync
        // received no calls, not the exception itself (covered above).
        try
        {
            await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));
        }
        catch (LatticeSaturatedException) { /* expected */ }
        catch (InvalidOperationException) { /* saga compensation envelope, also expected */ }

        await lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Borderline-recovery race ---

    [Test]
    public async Task ExecuteAsync_signal_Saturated_but_recovered_at_recheck_proceeds()
    {
        // The wait expires AND the re-check after expiry observes
        // the tree at Healthy (a borderline recovery between the
        // wait expiring and the re-check firing). The saga must
        // suppress the refusal so the caller is not penalised by
        // a race; the next dispatch lands on a healthy writer.
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

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal, options: new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
        });
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Host shutdown short-circuit ---

    [Test]
    public async Task ExecuteAsync_signal_Saturated_during_shutdown_skips_wait_and_proceeds_to_dispatch()
    {
        // When the host is shutting down, the signal will never
        // return Healthy (nothing will drain the in-flight batches).
        // The quiesce gate must short-circuit so the saga's next
        // dispatch surfaces LatticeShuttingDownException via the
        // writer's drain gate rather than burning the host-
        // deactivation budget on a wait that cannot succeed.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);

        var lifetime = Substitute.For<Microsoft.Extensions.Hosting.IHostApplicationLifetime>();
        using var stoppedCts = new CancellationTokenSource();
        stoppedCts.Cancel();
        lifetime.ApplicationStopping.Returns(stoppedCts.Token);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal, lifetime: lifetime);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        // The wait MUST NOT be entered when the host is stopping.
        await signal.DidNotReceive().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        // The dispatch MUST proceed (so the writer's drain gate can
        // surface LatticeShuttingDownException with attribution).
        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    // --- Bubble-up: writer-side LatticeSaturatedException wrapped in AggregateException ---

    [Test]
    public void ExecuteAsync_writer_side_LatticeSaturatedException_in_AggregateException_surfaces_typed_to_caller()
    {
        // The writer-side admission gate throws
        // LatticeSaturatedException; SetManyAsync's leaf-fan-out
        // pattern wraps it in an AggregateException via
        // Task.WhenAll. The saga's IsTerminalSaturationRefusal +
        // ExtractSaturationTreeId must walk the AggregateException
        // chain and preserve the original tree id for caller
        // attribution.
        var innerSaturation = new LatticeSaturatedException(
            "WAL append dispatch to tree 'leaf-tree' partition 0 refused: budget elapsed.",
            treeId: "leaf-tree");
        var wrapped = new AggregateException("fan-out failed", innerSaturation);

        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .ThrowsAsync(wrapped);

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k", [1]))));
        Assert.Multiple(() =>
        {
            // The saga preserves the inner exception so log
            // diagnostics retain the writer-side cause...
            Assert.That(ex!.InnerException, Is.SameAs(wrapped),
                "the saga must preserve the original failure chain for log diagnostics");
            // ...AND walks the chain to attribute the back-pressure
            // to the tree the writer-side gate named, not the
            // saga's own tree id.
            Assert.That(ex.TreeId, Is.EqualTo("leaf-tree"),
                "the saga must extract the originating tree id from the wrapped LatticeSaturatedException so caller-side attribution points at the tree the writer-side gate refused");
        });
    }

    // --- No signal registered: silent no-op fallback ---

    [Test]
    public async Task ExecuteAsync_no_signal_registered_proceeds_to_SetManyAsync_without_quiesce()
    {
        // Single-node / unit-test deployments build the saga without
        // an IWalSaturationSignal in DI. The quiesce gate must be a
        // silent no-op so existing test fixtures continue to work
        // unchanged. (Pinned by the existing
        // ExecuteAsync_no_signal_registered tests in
        // ShutdownRefused.cs too; this one specifically pins the
        // saga's SagaQuiesceOutcome.Proceed return path on the
        // no-signal branch.)
        var (grain, _, lattice, _) = CreateGrainWithSignal(signal: null);
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        await lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }
}
