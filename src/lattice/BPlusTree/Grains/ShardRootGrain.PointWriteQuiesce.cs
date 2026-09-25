using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Serialization.Invocation;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Serial-turn exclusion for interleaved point writes (issue #812).
/// <para>
/// <see cref="IShardRootGrain.SetAsync(string, byte[])"/> and its TTL overload are
/// <see cref="AlwaysInterleaveAttribute"/> so point writes aimed at one shard root
/// overlap each other instead of queueing behind one leaf round trip apiece. That
/// alone is not enough. Orleans admits a non-reentrant request while only
/// always-interleave requests are running, so without this guard every serial
/// turn could start part-way through an in-flight point write. That includes the
/// split / fold phase transitions (<c>EnterRejectPhaseAsync</c>,
/// <c>CompleteSplitAsync</c>, <c>MarkLeavesMovedAwayAsync</c>, the coordinators'
/// authoritative final drains), activation shutdown, and the serial reads.
/// </para>
/// <para>
/// Those turns were written, and are still reasoned about, on the premise that no
/// point write straddles them: a write either finished before the turn began or
/// had not yet passed its <see cref="ThrowIfRejectedForKey"/> gate. Breaking that
/// premise lost acknowledged writes in
/// <c>ShardConsolidationChaosTests.Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing</c>
/// 3 runs out of 3. A write that passed the gate before a fold froze its donor
/// could land on the donor's leaf after the fold's authoritative drain.
/// </para>
/// <para>
/// The guard restores that premise exactly, without serialising point writes
/// against each other:
/// </para>
/// <list type="bullet">
/// <item><description>A point write is admitted only while no serial turn is
/// active, and is counted in <see cref="_interleavedPointWritesInFlight"/> for its
/// whole duration.</description></item>
/// <item><description>A serial turn (any incoming call whose interface method is
/// not <see cref="AlwaysInterleaveAttribute"/>) marks itself active first. That
/// holds back new point writes. It then waits for the point writes already in
/// flight to drain before it runs.</description></item>
/// <item><description>Every other always-interleave call (for example
/// <see cref="IShardRootGrain.SetManyAsync"/> and the optimistic read) passes
/// straight through. Batch writes count themselves only for deactivation, never
/// for serial-turn exclusion.</description></item>
/// </list>
/// <para>
/// The steady-state cost to a point write is one counter check and one
/// increment/decrement pair. A serial turn pays nothing when no point write is in
/// flight. Contention allocates one <see cref="TaskCompletionSource"/> per drain
/// or release, not per call.
/// </para>
/// <para>
/// Serial turns do not deadlock against point writes. A serial turn waiting on an
/// in-flight point write blocks the activation to other serial work exactly as
/// that write's own non-reentrant turn used to, so any serial callback the write
/// path needed would already have deadlocked before the write was made
/// interleavable. The write path's own callbacks into this grain (the leaf's
/// <see cref="IShardRootGrain.PublishLeafByteFootprintAsync"/> from its persist
/// path, the split-link and digest hops) are all always-interleave.
/// </para>
/// <para>
/// Deactivation is the exception, and the guard fences it explicitly. An
/// activation that is deactivating waits for its running requests and serves no
/// new calls, always-interleave ones included. A point write dispatched after
/// <c>DeactivateOnIdle</c> therefore stalls: its leaf's persist path awaits the
/// footprint publish to this activation, which cannot be served, until the
/// response timeout. A split the leaf performed is carried back on that timed-out
/// reply, so its sibling is never linked and acknowledged writes are lost. The
/// consolidation chaos fixture reached this through <c>ForceDeactivateAsync</c>:
/// the non-reentrant point write it replaced held the turn, so deactivation could
/// never be requested mid-write. Every deactivation this grain requests now goes
/// through <see cref="RequestDeactivationFencingPointWrites"/>, which sets
/// <see cref="_deactivationRequested"/> first, then defers the runtime request
/// until both point and batch writes have drained. This also covers flush timer
/// callbacks, which bypass the serial-turn guard. New writes are refused with
/// <see cref="ShardRootDeactivatingException"/> before touching a leaf, and the
/// caller retries on the next activation. If the runtime refuses the request,
/// the admission fence is cleared so the activation remains usable.
/// Deactivations the runtime starts on its own (silo shutdown, rebalancing) have
/// no grain-side hook before the runtime stops serving calls. That exposure is the
/// same one every always-interleave write path here, <see cref="IShardRootGrain.SetManyAsync"/>
/// included, already carries.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Per-interface-method cache of whether the method carries
    /// <see cref="AlwaysInterleaveAttribute"/>, so the per-call classification
    /// does no reflection after first use.
    /// </summary>
    private static readonly ConcurrentDictionary<MethodInfo, bool> AlwaysInterleaveByMethod = new();

    /// <summary>Point writes currently admitted and not yet finished.</summary>
    private int _interleavedPointWritesInFlight;

    /// <summary>Batch writes counted only for deactivation, not serial-turn exclusion.</summary>
    private int _interleavedBatchWritesInFlight;

    /// <summary>
    /// Serial turns that are active, or waiting for in-flight point writes to
    /// drain. While non-zero, new point writes wait.
    /// </summary>
    private int _serialTurnsActive;

    /// <summary>Completed when <see cref="_interleavedPointWritesInFlight"/> reaches zero.</summary>
    private TaskCompletionSource? _pointWritesDrained;

    /// <summary>Completed when <see cref="_serialTurnsActive"/> reaches zero.</summary>
    private TaskCompletionSource? _serialTurnsReleased;

    /// <summary>
    /// Fences new writes while deactivation is pending or issued. Cleared only
    /// when the runtime refuses the request.
    /// </summary>
    private bool _deactivationRequested;

    private bool _deactivationIssued;

    /// <summary>Point writes currently in flight. Exposed for unit tests.</summary>
    internal int InterleavedPointWritesInFlight => _interleavedPointWritesInFlight;

    /// <summary>True once this activation has requested its own deactivation. Exposed for unit tests.</summary>
    internal bool DeactivationRequested => _deactivationRequested;

    /// <summary>
    /// Fences new point and batch writes, then requests deactivation once every
    /// admitted write has drained. Every <c>DeactivateOnIdle</c> this grain issues
    /// goes through here; see the class remarks for why.
    /// </summary>
    private void RequestDeactivationFencingPointWrites()
    {
        _deactivationRequested = true;
        IssueDeactivationIfWritesDrained();
    }

    private void IssueDeactivationIfWritesDrained()
    {
        if (!_deactivationRequested || _deactivationIssued
            || _interleavedPointWritesInFlight != 0
            || Volatile.Read(ref _interleavedBatchWritesInFlight) != 0)
        {
            return;
        }

        try
        {
            this.DeactivateOnIdle();
            _deactivationIssued = true;
        }
        catch
        {
            _deactivationRequested = false;
            throw;
        }
    }

    private void CompleteDeferredDeactivation()
    {
        // Both release paths retain the activation scheduler: SetManyAsync and
        // the point-write filter await without ConfigureAwait(false).
        if (!_deactivationRequested) return;

        try
        {
            IssueDeactivationIfWritesDrained();
        }
        catch (Exception ex)
        {
            // The write has finished: a failed runtime request must not replace
            // its result. IssueDeactivationIfWritesDrained reopened admission.
            logger.LogWarning(ex,
                "Could not request deferred deactivation of shard {ShardKey}; the admission fence was cleared and the activation stays up.",
                context.GrainId.Key.ToString());
        }
    }

    private void BeginBatchWrite()
    {
        if (_deactivationRequested)
        {
            throw new ShardRootDeactivatingException(ShardKeyForFence());
        }

        Interlocked.Increment(ref _interleavedBatchWritesInFlight);
    }

    private void EndBatchWrite()
    {
        if (Interlocked.Decrement(ref _interleavedBatchWritesInFlight) == 0)
        {
            CompleteDeferredDeactivation();
        }
    }

    /// <summary>
    /// Classifies an incoming call for the quiesce guard.
    /// </summary>
    internal static IncomingTurnKind ClassifyIncomingTurn(IInvokable request)
    {
        if (request.GetInterfaceType() == typeof(IShardRootGrain)
            && request.GetMethodName() == nameof(IShardRootGrain.SetAsync))
        {
            return IncomingTurnKind.PointWrite;
        }

        var method = request.GetMethod();
        if (method is not null
            && AlwaysInterleaveByMethod.GetOrAdd(method, static m => m.IsDefined(typeof(AlwaysInterleaveAttribute), inherit: true)))
        {
            return IncomingTurnKind.Interleaved;
        }

        return IncomingTurnKind.Serial;
    }

    /// <summary>
    /// Runs a point write, first waiting out any active serial turn. Refuses it
    /// once this activation has requested its own deactivation.
    /// </summary>
    private Task InvokePointWriteAsync(IIncomingGrainCallContext context)
    {
        if (_deactivationRequested)
        {
            return Task.FromException(new ShardRootDeactivatingException(ShardKeyForFence()));
        }

        if (_serialTurnsActive > 0)
        {
            return InvokePointWriteAfterSerialTurnsAsync(context);
        }

        return InvokeAdmittedPointWrite(context);
    }

    private string ShardKeyForFence() => this.GetGrainId().Key.ToString()!;

    private async Task InvokePointWriteAfterSerialTurnsAsync(IIncomingGrainCallContext context)
    {
        // Loop: another serial turn may have become active between the release
        // and this continuation running.
        while (_serialTurnsActive > 0)
        {
            _serialTurnsReleased ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await _serialTurnsReleased.Task;
        }

        // The serial turn this write waited out may have been the one that
        // requested deactivation (ForceDeactivateAsync, for example).
        if (_deactivationRequested)
        {
            throw new ShardRootDeactivatingException(ShardKeyForFence());
        }

        await InvokeAdmittedPointWrite(context);
    }

    private Task InvokeAdmittedPointWrite(IIncomingGrainCallContext context)
    {
        _interleavedPointWritesInFlight++;
        Task invocation;
        try
        {
            invocation = InvokeRoutingFiltered(context);
        }
        catch
        {
            EndPointWrite();
            throw;
        }

        if (invocation.IsCompleted)
        {
            EndPointWrite();
            return invocation;
        }

        return AwaitPointWriteAsync(invocation);
    }

    private async Task AwaitPointWriteAsync(Task invocation)
    {
        try
        {
            await invocation;
        }
        finally
        {
            EndPointWrite();
        }
    }

    private void EndPointWrite()
    {
        if (--_interleavedPointWritesInFlight == 0)
        {
            if (_pointWritesDrained is { } drained)
            {
                _pointWritesDrained = null;
                drained.TrySetResult();
            }
            CompleteDeferredDeactivation();
        }
    }

    /// <summary>
    /// Runs a serial turn: holds back new point writes, waits for in-flight ones
    /// to drain, then invokes.
    /// </summary>
    private Task InvokeSerialTurnAsync(IIncomingGrainCallContext context)
    {
        _serialTurnsActive++;
        if (_interleavedPointWritesInFlight > 0)
        {
            return InvokeSerialTurnAfterDrainAsync(context);
        }

        Task invocation;
        try
        {
            invocation = InvokeRoutingFiltered(context);
        }
        catch
        {
            EndSerialTurn();
            throw;
        }

        if (invocation.IsCompleted)
        {
            EndSerialTurn();
            return invocation;
        }

        return AwaitSerialTurnAsync(invocation);
    }

    private async Task InvokeSerialTurnAfterDrainAsync(IIncomingGrainCallContext context)
    {
        try
        {
            while (_interleavedPointWritesInFlight > 0)
            {
                _pointWritesDrained ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await _pointWritesDrained.Task;
            }

            await InvokeRoutingFiltered(context);
        }
        finally
        {
            EndSerialTurn();
        }
    }

    private async Task AwaitSerialTurnAsync(Task invocation)
    {
        try
        {
            await invocation;
        }
        finally
        {
            EndSerialTurn();
        }
    }

    private void EndSerialTurn()
    {
        if (--_serialTurnsActive == 0 && _serialTurnsReleased is { } released)
        {
            _serialTurnsReleased = null;
            released.TrySetResult();
        }
    }

    /// <summary>
    /// How the quiesce guard treats an incoming call.
    /// </summary>
    internal enum IncomingTurnKind
    {
        /// <summary>A point <see cref="IShardRootGrain.SetAsync(string, byte[])"/>, either overload.</summary>
        PointWrite,

        /// <summary>Any other always-interleave call; not held back and does not hold anything back.</summary>
        Interleaved,

        /// <summary>A non-reentrant call; exclusive against point writes.</summary>
        Serial,
    }
}
