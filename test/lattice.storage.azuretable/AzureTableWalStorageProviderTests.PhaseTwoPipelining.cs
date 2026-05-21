using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for the per-shard "previous batch's phase-2 task"
/// slot exposed via
/// <see cref="AzureTableWalStorageProvider.AwaitPreviousPipelinedAsync"/>
/// and configured by
/// <see cref="AzureTableWalStorageOptions.PipelinePhaseTwoCommits"/>.
/// The tests exercise the in-memory slot exchange directly without
/// driving the table-client path, so no Azurite endpoint is required.
/// The combination of these tests plus
/// <see cref="PhaseTwoWorkerTests"/> pins the pipelining semantics
/// (sticky failure, strict ordering preserved, slot identity-based
/// swap) so the end-to-end Azurite suite only needs to confirm the
/// integrated path lights up.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderPhaseTwoPipeliningTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private AzureTableWalStorageProvider CreateProvider(bool pipeline)
    {
        // Connection string is only used if the provider performs
        // real I/O. These tests exercise only the in-memory slot
        // exchange, so the value just has to parse.
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "Tpipe" + Guid.NewGuid().ToString("N"),
            PipelinePhaseTwoCommits = pipeline,
        };
        return new AzureTableWalStorageProvider(Options.Create(options), _serializer);
    }

    private AzureTableWalStorageProvider CreateProviderWithFaultHandler(
        Action<Exception> handler)
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "Tpipe" + Guid.NewGuid().ToString("N"),
            PipelinePhaseTwoCommits = true,
            PipelinedPhaseTwoFaultHandler = handler,
        };
        return new AzureTableWalStorageProvider(Options.Create(options), _serializer);
    }

    [Test]
    public void PipelinePhaseTwoCommits_default_value_is_false()
    {
        // The option flips an observable durability characteristic
        // (phase-2 failure surfaces on the *next* AppendBatchAsync
        // rather than the failing one), so the default must be off
        // until a host explicitly opts in.
        var options = new AzureTableWalStorageOptions();
        Assert.That(options.PipelinePhaseTwoCommits, Is.False);
    }

    [Test]
    public async Task AwaitPreviousPipelinedAsync_first_call_on_a_shard_completes_immediately()
    {
        // No previous task is stashed in the slot the first time
        // pipelining runs on a shard, so the call must complete
        // synchronously even though `currentTask` is still in flight.
        await using var sut = CreateProvider(pipeline: true);

        var currentTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var awaitTask = sut.AwaitPreviousPipelinedAsync("_m_|tree|0", currentTcs.Task);

        Assert.That(awaitTask.IsCompletedSuccessfully, Is.True,
            "with an empty slot the helper has nothing to await and must complete synchronously");

        // Resolve the stashed task before DisposeAsync drains the
        // slot; otherwise the fixture hangs at teardown.
        currentTcs.SetResult();
    }

    [Test]
    public async Task AwaitPreviousPipelinedAsync_second_call_awaits_the_first_task()
    {
        // The second call's await must observe the first call's
        // task, not its own; that overlap is the whole point of the
        // pipelined mode.
        await using var sut = CreateProvider(pipeline: true);

        var firstTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", firstTcs.Task).ConfigureAwait(false);
        var secondAwait = sut.AwaitPreviousPipelinedAsync("_m_|tree|0", secondTcs.Task);

        Assert.That(secondAwait.IsCompleted, Is.False,
            "the second call must block on the first task, which is still in flight");

        firstTcs.SetResult();
        await secondAwait.ConfigureAwait(false);

        Assert.That(secondTcs.Task.IsCompleted, Is.False,
            "completing the first task must not also resolve the second; the second is the new slot occupant");

        // Resolve the stashed task before DisposeAsync drains the
        // slot.
        secondTcs.SetResult();
    }

    [Test]
    public async Task AwaitPreviousPipelinedAsync_propagates_previous_failure_on_next_call()
    {
        // Sticky-failure: a phase-2 fault observed by the first call
        // surfaces to the *second* call's await, which is the
        // signal WalShardGrain uses to trigger its resync. Without
        // that propagation the grain would silently advance
        // _nextOffset past a hole.
        await using var sut = CreateProvider(pipeline: true);

        var firstTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // First call returns synchronously (empty slot); the failure
        // is intentionally faulted only after the slot has been
        // stashed so the second caller is the one that observes it.
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", firstTcs.Task).ConfigureAwait(false);

        var phaseTwoFailure = new InvalidOperationException("phase-2 blew up");
        firstTcs.SetException(phaseTwoFailure);

        var observed = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", secondTcs.Task).ConfigureAwait(false));

        Assert.That(observed, Is.SameAs(phaseTwoFailure),
            "the next call must surface the previous batch's phase-2 fault verbatim");

        // Cleanup so DisposeAsync does not deadlock waiting on the
        // still-pending secondTcs task.
        secondTcs.SetResult();
    }

    [Test]
    public async Task AwaitPreviousPipelinedAsync_isolates_slots_across_distinct_shards()
    {
        // The slot is keyed by manifest partition key, so a stash
        // against tree|0 must not be observed by a call against
        // tree|1. Cross-shard isolation is a structural invariant of
        // the pipelining mode (mirroring the per-shard worker
        // dictionary).
        await using var sut = CreateProvider(pipeline: true);

        var shardZeroTask = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardOneTask = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", shardZeroTask.Task).ConfigureAwait(false);
        var followUpOnShardOne = sut.AwaitPreviousPipelinedAsync(
            "_m_|tree|1",
            shardOneTask.Task);

        Assert.That(followUpOnShardOne.IsCompletedSuccessfully, Is.True,
            "shard 1's slot is empty even though shard 0's slot now holds a task");

        shardZeroTask.SetResult();
        shardOneTask.SetResult();
    }

    [Test]
    public async Task AwaitPreviousPipelinedAsync_two_appends_chain_through_the_slot()
    {
        // Append A -> empty slot -> returns immediately, slot = A.
        // Append B -> slot held A -> awaits A, slot = B.
        // Append C -> slot held B -> awaits B, slot = C.
        // This chain is the structural pin for the "one-deep" pipeline
        // depth the option promises.
        await using var sut = CreateProvider(pipeline: true);

        var a = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var b = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var c = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var awaitA = sut.AwaitPreviousPipelinedAsync("_m_|tree|0", a.Task);
        Assert.That(awaitA.IsCompletedSuccessfully, Is.True);

        var awaitB = sut.AwaitPreviousPipelinedAsync("_m_|tree|0", b.Task);
        Assert.That(awaitB.IsCompleted, Is.False);
        a.SetResult();
        await awaitB.ConfigureAwait(false);

        var awaitC = sut.AwaitPreviousPipelinedAsync("_m_|tree|0", c.Task);
        Assert.That(awaitC.IsCompleted, Is.False,
            "awaitC must block until B completes - the pipeline is one-deep, not zero-deep");
        b.SetResult();
        await awaitC.ConfigureAwait(false);

        c.SetResult();
    }

    [Test]
    public async Task DisposeAsync_drains_outstanding_pipelined_tasks()
    {
        // Shutdown must observe (and swallow) any still-in-flight
        // pipelined task so the AppDomain unload path doesn't strand
        // a half-resolved TCS chain.
        var sut = CreateProvider(pipeline: true);

        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", pending.Task).ConfigureAwait(false);

        var disposeTask = sut.DisposeAsync().AsTask();
        Assert.That(disposeTask.IsCompleted, Is.False,
            "DisposeAsync must block until the outstanding pipelined task resolves");

        pending.SetResult();
        await disposeTask.ConfigureAwait(false);
    }

    [Test]
    public async Task DisposeAsync_swallows_faults_from_outstanding_pipelined_tasks()
    {
        // The faulted-pipelined-task path on dispose must not throw;
        // the worker has already surfaced the fault through its TCSs
        // and Dispose is the terminal stage.
        var sut = CreateProvider(pipeline: true);

        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", pending.Task).ConfigureAwait(false);
        pending.SetException(new InvalidOperationException("phase-2 blew up during shutdown"));

        Assert.DoesNotThrowAsync(async () => await sut.DisposeAsync().ConfigureAwait(false));
    }

    [Test]
    public async Task Last_batch_phase_two_fault_with_no_successor_is_never_surfaced_to_the_caller()
    {
        // FIX VALIDATION (gap #1: stranded last-batch fault).
        //
        // In pipelined mode a phase-2 fault used to be surfaced only
        // to the *next* AppendBatchAsync on the same shard, which
        // meant a quiescent shard's last fault was observed only by
        // DisposeAsync (which swallows). The data itself is still
        // recoverable (phase 0+1 are durable and ReconcileAsync rolls
        // them forward at next activation), but the application that
        // issued the last append had no signal that its phase-2
        // failed.
        //
        // The fix wires
        // AzureTableWalStorageOptions.PipelinedPhaseTwoFaultHandler
        // to a one-shot ContinueWith on the slot occupant, so the
        // configured observer fires exactly once on fault regardless
        // of whether a successor call ever arrives.
        var observedFaults = new List<Exception>();
        var faultObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sut = CreateProviderWithFaultHandler(ex =>
        {
            lock (observedFaults)
            {
                observedFaults.Add(ex);
            }
            faultObserved.TrySetResult();
        });

        // Drive the helper directly to simulate the
        // AppendBatchAsync -> DispatchPhaseTwoAsync -> slot-stash
        // sequence without standing up Azurite. The provider's real
        // path also calls AttachPipelinedFaultObserver before
        // stashing; we mirror that here.
        var lastBatchPhaseTwo = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attached = sut.AttachPipelinedFaultObserver(lastBatchPhaseTwo.Task);
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", attached).ConfigureAwait(false);

        // No successor call arrives. The last batch's phase-2 then
        // faults. The handler must observe it without any further
        // append.
        var phaseTwoFailure = new InvalidOperationException("phase-2 commit on the last batch blew up");
        lastBatchPhaseTwo.SetException(phaseTwoFailure);

        var observed = await Task.WhenAny(faultObserved.Task, Task.Delay(2000)).ConfigureAwait(false);
        Assert.That(observed, Is.SameAs(faultObserved.Task),
            "the configured fault handler must observe the slot occupant's failure even when no successor call arrives");
        lock (observedFaults)
        {
            Assert.That(observedFaults, Has.Count.EqualTo(1));
            Assert.That(observedFaults[0], Is.SameAs(phaseTwoFailure));
        }
    }

    [Test]
    public async Task PipelinedPhaseTwoFaultHandler_is_not_invoked_on_successful_phase_two()
    {
        // The handler must fire only on fault. A successful phase-2
        // commit on a quiescent shard is not an observability event.
        var observedFaults = new List<Exception>();
        await using var sut = CreateProviderWithFaultHandler(ex =>
        {
            lock (observedFaults)
            {
                observedFaults.Add(ex);
            }
        });

        var lastBatch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attached = sut.AttachPipelinedFaultObserver(lastBatch.Task);
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", attached).ConfigureAwait(false);

        lastBatch.SetResult();

        // Give the (non-)continuation any chance to run before the
        // assertion, then verify nothing was observed.
        await Task.Delay(50).ConfigureAwait(false);
        lock (observedFaults)
        {
            Assert.That(observedFaults, Is.Empty,
                "successful phase-2 must not invoke the fault handler");
        }
    }

    [Test]
    public async Task PipelinedPhaseTwoFaultHandler_swallows_handler_exceptions()
    {
        // A throwing handler must not corrupt the pipeline's task
        // graph - the handler is for observability only, never on
        // the request path.
        await using var sut = CreateProviderWithFaultHandler(_ =>
            throw new InvalidOperationException("handler is misbehaving"));

        var lastBatch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attached = sut.AttachPipelinedFaultObserver(lastBatch.Task);
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", attached).ConfigureAwait(false);

        Assert.DoesNotThrow(() => lastBatch.SetException(new InvalidOperationException("phase-2 blew up")));

        // DisposeAsync must remain clean even though the handler threw.
        Assert.DoesNotThrowAsync(async () => await sut.DisposeAsync().ConfigureAwait(false));
    }

    [Test]
    public void AttachPipelinedFaultObserver_returns_input_task_unchanged_when_no_handler_configured()
    {
        // Zero-alloc fast path: with no handler the helper must
        // hand back the exact same Task instance it was given. The
        // pipelined slot then stores the worker's task, not a wrapper,
        // so any other observer (next call, DisposeAsync) sees the
        // worker's task identity directly.
        var sut = CreateProvider(pipeline: true);
        var input = Task.CompletedTask;
        var output = sut.AttachPipelinedFaultObserver(input);
        Assert.That(output, Is.SameAs(input));
    }

    [Test]
    public async Task AttachPipelinedFaultObserver_returns_input_task_identity_even_when_handler_configured()
    {
        // Even with a handler, the returned task is the same
        // logical task the caller supplied; the fault-observation
        // continuation is fire-and-forget and does not alter the
        // task's settled value or timing relative to the slot's
        // contract. The slot stores the unmodified task identity so
        // the next call's await observes the worker's task directly.
        var faultObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sut = CreateProviderWithFaultHandler(_ => faultObserved.TrySetResult());

        var input = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var output = sut.AttachPipelinedFaultObserver(input.Task);

        Assert.That(output, Is.SameAs(input.Task),
            "the helper must hand back the input task identity even when a handler is wired");

        var failure = new InvalidOperationException("boom");
        input.SetException(failure);

        var caught = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await output.ConfigureAwait(false));
        Assert.That(caught, Is.SameAs(failure),
            "the returned task surfaces the antecedent's fault verbatim");

        var handlerRan = await Task.WhenAny(faultObserved.Task, Task.Delay(2000)).ConfigureAwait(false);
        Assert.That(handlerRan, Is.SameAs(faultObserved.Task),
            "the side-channel handler must still fire on fault even though the returned task is identity-equal to the input");
    }

    [Test]
    public async Task AwaitPreviousPipelinedAsync_with_default_cancellation_token_still_awaits_the_predecessor()
    {
        // Symmetric to the cancelable case: when the caller has
        // nothing to cancel, the helper takes the non-WaitAsync
        // branch (no wrapper Task, no CancellationTokenRegistration
        // allocation per call) but the correctness invariant is
        // unchanged - the second call still blocks on the
        // predecessor's task identity.
        await using var sut = CreateProvider(pipeline: true);

        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", first.Task).ConfigureAwait(false);
        var awaitTask = sut.AwaitPreviousPipelinedAsync(
            "_m_|tree|0", second.Task, CancellationToken.None);

        Assert.That(awaitTask.IsCompleted, Is.False,
            "the default CancellationToken must not short-circuit the wait on the predecessor");
        first.SetResult();
        await awaitTask.ConfigureAwait(false);
        second.SetResult();
    }

    [Test]
    public async Task AwaitPreviousPipelinedAsync_honors_caller_cancellation_while_waiting_for_predecessor()
    {
        // FIX VALIDATION (gap #2: missing CT seam).
        //
        // AwaitPreviousPipelinedAsync now accepts a CancellationToken
        // and uses Task.WaitAsync(ct) so a stuck predecessor releases
        // the *current* caller without disturbing the predecessor's
        // task (which is shared state owned by the worker; any other
        // observer continues to see it through to its terminal state).
        var sut = CreateProvider(pipeline: true);
        var stuckPredecessor = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var successor = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            // First call: slot is empty, returns immediately and
            // stashes stuckPredecessor.Task as the slot occupant.
            await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", stuckPredecessor.Task).ConfigureAwait(false);

            using var cts = new CancellationTokenSource();
            // Second call: swaps successor.Task in, must await
            // stuckPredecessor under cancellation.
            var successorAwait = sut.AwaitPreviousPipelinedAsync("_m_|tree|0", successor.Task, cts.Token);

            await Task.Delay(50).ConfigureAwait(false);
            Assert.That(successorAwait.IsCompleted, Is.False,
                "successor must block on stuckPredecessor until cancelled");

            cts.Cancel();

            OperationCanceledException? observed = null;
            try
            {
                await successorAwait.ConfigureAwait(false);
            }
            catch (OperationCanceledException oce)
            {
                observed = oce;
            }

            Assert.That(observed, Is.Not.Null,
                "the supplied CT must release the current caller's wait via Task.WaitAsync");

            Assert.That(stuckPredecessor.Task.IsCompleted, Is.False,
                "cancellation must NOT disturb the predecessor's task; it remains in flight for any other observer");
        }
        finally
        {
            // Settle outstanding tasks before disposing so the
            // provider's DisposeAsync drain has nothing in flight.
            stuckPredecessor.TrySetResult();
            successor.TrySetResult();
            await sut.DisposeAsync().ConfigureAwait(false);
        }
    }
}
