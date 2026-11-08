namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Retention of a refused replay's queue position (issue #6006, the remainder of
/// #5992). A wait-arm refusal of an Interactive replay refuses its callers fast,
/// as before, but the replay's own gate wait stays parked in its arrival order
/// instead of being abandoned. Nothing is re-enqueued, so the gate's single
/// queue is the arrival order for retained and ordinary waiters alike.
/// </summary>
/// <remarks>
/// <para>
/// <b>Bounded in count.</b> No new retention is taken while the retained count
/// has reached R, the replay permits in circulation read at the decision. A
/// walk-down evicts nothing that is already retained, so the absolute bound is
/// the gate's size.
/// </para>
/// <para>
/// <b>Bounded in time.</b> A retained wait is dropped at
/// <see cref="RetainedReplayMaxAge"/>, and when the activation goes away: a
/// deactivation (including a working-set shed) cancels the barrier's lifetime,
/// which cancels the wait. There is no <c>DelayDeactivation</c>; a dropped
/// retention is re-decided by the next activation.
/// </para>
/// <para>
/// <b>Kept out of the wait-arm mean.</b> A retained wait is the longest wait
/// by construction, so folding it into the smoothed queue-wait mean would raise
/// the mean and cause more refusals. It is still recorded on the queue-wait
/// histogram.
/// </para>
/// </remarks>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// The longest a refused replay keeps its gate position before the wait is
    /// dropped unadmitted: the replay-demand lease, so a retained leaf's declared
    /// pages are kept for as long as it can wait for them.
    /// </summary>
    internal static readonly TimeSpan RetainedReplayMaxAge = TimeSpan.FromMinutes(10);

    /// <summary>Test-only override of <see cref="RetainedReplayMaxAge"/>.</summary>
    internal static TimeSpan? RetainedReplayMaxAgeForTest;


    /// <summary>Test-only view of <see cref="_retainedReplayWaiters"/>.</summary>
    internal static int RetainedReplayWaitersForTest => Volatile.Read(ref _retainedReplayWaiters);

    /// <summary>
    /// The retention this activation's replay holds while its gate wait is
    /// parked and not yet admitted, or <see langword="null"/>. A caller that
    /// joins the barrier while it is set is refused fast.
    /// </summary>
    private RetainedReplayRefusal? _replayRetention;

    /// <summary>
    /// Completed when this activation's replay becomes retained, so callers
    /// already waiting on the barrier are refused at once. Allocated by the first
    /// waiting caller of a barrier, on the slow path only.
    /// </summary>
    private TaskCompletionSource? _replayRetainedSignal;

    /// <summary>
    /// Set when this activation's retained replay aged out, so the barrier's
    /// refusal handling does not report the refusal its callers already had.
    /// </summary>
    private bool _retainedReplayExpired;

    /// <summary>
    /// Test isolation only. Advanced by <see cref="ResetReplayConcurrencyGateForTest"/>,
    /// so a retained wait parked on a gate a test discarded never returns its
    /// slot or its arrival to the counters of the gate that replaced it. The
    /// gate is never discarded in production, where this reads zero for the
    /// life of the process and costs one integer compare when a retained wait ends.
    /// </summary>
    private static int _replayGateGeneration;

    /// <summary>
    /// Test-only: a fault thrown after a retained wait has registered and before
    /// it is awaited, so a test can prove that window strands no permit and no
    /// retention slot.
    /// </summary>
    internal static Exception? RetainedReplayFaultForTest;

    private static TimeSpan EffectiveRetainedReplayMaxAge
        => RetainedReplayMaxAgeForTest ?? RetainedReplayMaxAge;

    /// <summary>
    /// Whether a refusal of <paramref name="admissionClass"/> on
    /// <paramref name="verdict"/> could be retained right now: an Interactive
    /// wait-arm refusal with the retained count below R. Read only; the
    /// acquisition takes the slot with <see cref="TryReserveReplayRetention"/>.
    /// </summary>
    private static bool IsReplayRetainable(LatticeReplayAdmissionClass admissionClass, ReplayPermitDrainVerdict verdict)
        => admissionClass == LatticeReplayAdmissionClass.Interactive
            && verdict == ReplayPermitDrainVerdict.WaitExceeded
            && Volatile.Read(ref _retainedReplayWaiters) < RetentionLimit();

    /// <summary>
    /// Takes a retention slot if the retained count is below R. A slot taken
    /// here is returned exactly once by <see cref="ReleaseReplayRetention"/>.
    /// </summary>
    internal static bool TryReserveReplayRetention()
    {
        var limit = RetentionLimit();
        while (true)
        {
            var retained = Volatile.Read(ref _retainedReplayWaiters);
            if (retained >= limit)
                return false;
            if (Interlocked.CompareExchange(ref _retainedReplayWaiters, retained + 1, retained) == retained)
                return true;
        }
    }

    internal static void ReleaseReplayRetention() => Interlocked.Decrement(ref _retainedReplayWaiters);

    /// <summary>Test-only: sets the retained count directly, as the admission seeds do.</summary>
    internal static void SeedRetainedReplayWaitersForTest(int retained)
        => Volatile.Write(ref _retainedReplayWaiters, retained);

    /// <summary>Test-only: whether this activation's replay is retained and not yet admitted.</summary>
    internal bool ReplayRetainedForTest => _replayRetention is not null;

    /// <summary>Test-only: whether this activation holds a resident working-set registration.</summary>
    internal bool ResidentFootprintRegisteredForTest => !ReferenceEquals(_residency, LeafResidencyRegistration.None);

    /// <summary>
    /// Test-only: completes when this activation's replay becomes retained, as a
    /// caller waiting on the barrier is woken; already complete while it is.
    /// </summary>
    internal Task WhenReplayRetainedForTest()
        => _replayRetention is not null
            ? Task.CompletedTask
            : (_replayRetainedSignal ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

    /// <summary>Test-only: the refusal a caller receives while the replay is retained, or <see langword="null"/>.</summary>
    internal LatticeSaturatedException? CreateRetainedRefusalForTest() => _replayRetention?.Create();

    /// <summary>R: the replay permits in circulation now.</summary>
    private static int RetentionLimit()
        => CirculatingReplayPermits(
            Volatile.Read(ref _replayConcurrencyCeiling),
            Volatile.Read(ref _withheldReplayPermits));

    /// <summary>
    /// Marks this activation's replay retained and refuses every caller already
    /// waiting on its barrier.
    /// </summary>
    private void EnterReplayRetention(RetainedReplayRefusal refusal)
    {
        _replayRetention = refusal;
        ReportReplayAdmissionRefusal(_replayPermitTreeId);
        var signal = _replayRetainedSignal;
        _replayRetainedSignal = null;
        signal?.TrySetResult();
    }

    /// <summary>
    /// Waits retained: the same gate wait an ordinary arrival takes, dropped at
    /// the maximum age. The callers are refused only once the wait holds its
    /// gate position, so a caller's retry can never arrive ahead of it. The
    /// retention slot is returned exactly once, on every exit, and the wait never
    /// enters the wait-arm mean.
    /// </summary>
    private async Task WaitRetainedReplayPermitAsync(
        SemaphoreSlim gate, long queuedAt, RetainedReplayRefusal refusal, LatticeOptions options, CancellationToken cancellationToken)
    {
        using var expiry = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            expiry.CancelAfter(EffectiveRetainedReplayMaxAge);
            try
            {
                var wait = gate.WaitAsync(expiry.Token);
                try
                {
                    EnterReplayRetention(refusal);
                    if (Volatile.Read(ref RetainedReplayFaultForTest) is { } fault)
                        throw fault;

                    // Advisory (#5877 DT2): its order against the gate does not
                    // matter, only that a retained replay declares at all.
                    await DeclareReplayDemandAsync(options);
                }
                catch
                {
                    // The wait already holds a gate position. Withdraw it, and
                    // return the permit if it was granted meanwhile, so a fault
                    // here strands nothing.
                    expiry.Cancel();
                    try
                    {
                        await wait.ConfigureAwait(true);
                        gate.Release();
                    }
                    catch (OperationCanceledException)
                    {
                    }

                    throw;
                }

                await wait.ConfigureAwait(true);
            }
            catch (OperationCanceledException)
            {
                RecordReplayPermitQueueWait(queuedAt, LatticeMetrics.PermitQueueWaitCanceled, retained: true);
                if (cancellationToken.IsCancellationRequested)
                    throw;

                // Aged out: the entry leaves the gate unadmitted. Nobody is waiting
                // on it, since its callers were refused when it was retained, so it
                // ends as the same refusal and the next request decides afresh.
                _replayAdmissionPhase = ReplayAdmissionPhase.RefusedAdmission;
                _retainedReplayExpired = true;
                throw refusal.Create();
            }

            RecordReplayPermitQueueWait(queuedAt, LatticeMetrics.PermitQueueWaitAcquired, retained: true);
        }
        finally
        {
            _replayRetention = null;
            if (refusal.OnCurrentGate)
                ReleaseReplayRetention();
        }
    }

    /// <summary>
    /// The refusal a retained replay's callers receive. Each caller gets its own
    /// instance, so concurrent throws never share a stack trace.
    /// </summary>
    private sealed class RetainedReplayRefusal(string message, string treeId)
    {
        private readonly int _generation = Volatile.Read(ref _replayGateGeneration);

        /// <summary>Whether the gate this retention was taken on is still the current one.</summary>
        public bool OnCurrentGate => _generation == Volatile.Read(ref _replayGateGeneration);

        public LatticeSaturatedException Create()
            => new(message, treeId, LatticeSaturationSource.ReplayPermitAdmission);
    }
}
