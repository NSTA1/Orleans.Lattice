namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo, byte-denominated admission gate for activation-time leaf-snapshot
/// hydration (issue #2765). Bounds the <b>aggregate</b> bytes of snapshot loads
/// that are materialising concurrently, so the transient cost of a cold start is
/// a function of this process's heap rather than of however many leaves Orleans
/// happens to activate at once.
/// <para>
/// The unbounded shape it replaces is what killed the deployed process. Orleans
/// activates many leaves concurrently on cold start; each activation reads its
/// leaf snapshot through the storage provider, which materialises the whole
/// persisted blob (a SQLite blob read, then the serializer's own copy of it,
/// then the rows). Nothing capped how many did that simultaneously, so the
/// aggregate transient allocation scaled with the activation storm and crossed
/// the .NET heap <b>hard limit</b> - which the runtime sizes from the container's
/// cgroup memory limit. The result is a MANAGED <see cref="OutOfMemoryException"/>,
/// so the process exits 0 and is never reported as OOM-killed, which is why the
/// restart loop read for a long time as an unexplained clean exit.
/// </para>
/// <para>
/// <b>The budget is derived, never configured.</b> There is deliberately no
/// option, environment variable or flag: a bound that only works once an
/// operator sets it does not fix a process that is already restart-looping, and
/// raising the container's memory limit was explicitly ruled out as a remedy.
/// The ceiling comes from <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/>,
/// which is the same cgroup-derived figure the heap hard limit itself is sized
/// from, so the gate tracks the container it is actually running in.
/// </para>
/// <para>
/// <b>Forward progress is guaranteed by the sole-occupant rule, not by the
/// floor.</b> A leaf whose snapshot is larger than the entire budget - which is
/// exactly the population this issue is about, since the measured corpus carried
/// blobs at 3.5x <see cref="LatticeOptions.MaxLeafBytes"/> - is admitted as soon
/// as it is the only claimant. Without that rule an oversized leaf would wait
/// forever on a budget it can never fit inside, and the gate would convert a
/// crash loop into a permanent stall, which is not an improvement.
/// </para>
/// <para>
/// Admission is strictly first-in-first-out. A later, smaller claim is never
/// allowed to barge past a queued larger one, because barging is precisely what
/// would starve the oversized leaves whose division is the only thing that ever
/// brings the corpus back under bound.
/// </para>
/// </summary>
internal sealed class LeafSnapshotHydrationAdmission
{
    /// <summary>
    /// Divisor applied to the heap hard limit to size the gate. One eighth
    /// leaves the remaining seven eighths for the resident projections, the WAL
    /// replay path and the ordinary request path, all of which are live while a
    /// cold start is running.
    /// </summary>
    internal const int HeapBudgetDivisor = 8;

    /// <summary>
    /// Floor on the derived budget. This exists to stop a very small heap
    /// serialising every hydration end to end for no benefit; it is explicitly
    /// <b>not</b> what makes progress possible, since the sole-occupant rule
    /// already admits a claim larger than the whole budget.
    /// </summary>
    internal const long MinimumBudgetBytes = 32L * 1024 * 1024;

    /// <summary>
    /// Budget used when the runtime reports no heap hard limit at all
    /// (<see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> of zero). An
    /// unknown ceiling is not licence to be unbounded: the defect being fixed is
    /// precisely an unbounded fan-out, so the unknown case takes a fixed,
    /// conservative bound rather than opting out of the gate.
    /// </summary>
    internal const long UnknownHeapLimitBudgetBytes = 512L * 1024 * 1024;

    /// <summary>
    /// Multiplier converting a snapshot's <b>stored</b> size into the peak
    /// managed heap one hydration of it actually costs.
    /// <para>
    /// This is the single most important number in the gate, and sizing the
    /// budget against stored bytes instead would make it several times too
    /// permissive. The leaf's persistent state is written by the ADO.NET grain
    /// storage provider as JSON <b>text</b>, so reading it back is not a byte
    /// copy. The live failing stack shows the sequence, and the lifetimes
    /// overlap rather than succeed one another:
    /// </para>
    /// <list type="number">
    /// <item><description>the provider reads the column
    /// (<c>SqliteValueReader.GetValue</c> / <c>GetBlob</c>);</description></item>
    /// <item><description><c>System.String.Ctor(char[], int, int)</c> builds a
    /// string of the whole document - UTF-16, so <b>2x</b> the stored byte
    /// length;</description></item>
    /// <item><description>the <c>char[]</c> being copied <i>from</i> is still
    /// live while that happens - another <b>2x</b>;</description></item>
    /// <item><description>Newtonsoft then parses an object graph on top of
    /// it.</description></item>
    /// </list>
    /// <para>
    /// Four multiples are therefore unavoidable before the parse allocates
    /// anything, which is why the observed <see cref="OutOfMemoryException"/>
    /// lands in <c>String.Ctor</c> and <c>JsonTextReader.ParseReadString</c>
    /// rather than in the blob read. Five is a deliberately conservative floor
    /// on that shape, not a tuning parameter: erring low re-admits the very
    /// fan-out this gate exists to stop, while erring high only serialises a
    /// cold start that is already the slow path.
    /// </para>
    /// <para>
    /// A worked example, purely to show the shape of the asymmetry - the gate
    /// derives its budget at runtime and carries no assumption about container
    /// size, so none of these figures appear anywhere in the code. On the 12 GiB
    /// limit the reported defect was measured against, the budget resolves to
    /// 1.5 GiB; the largest leaf in that corpus is 226 MB stored, so it costs
    /// about 1.13 GiB and exactly <b>one</b> such hydration is admitted at a
    /// time, with the second queuing. The mean leaf there is about 613 KB,
    /// costing roughly 3 MB, so several hundred ordinary leaves still activate
    /// together. That is the intended asymmetry - the gate is invisible to a
    /// healthy tree and binds hard precisely on the population that was killing
    /// the process - and it holds at any limit, because both the budget and the
    /// charge scale with it.
    /// </para>
    /// </summary>
    internal const int HydrationHeapAmplification = 5;

    private static readonly Lazy<LeafSnapshotHydrationAdmission> SharedInstance =
        new(
            () => new LeafSnapshotHydrationAdmission(
                ResolveBudgetBytes(GC.GetGCMemoryInfo().TotalAvailableMemoryBytes)),
            LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly object _gate = new();
    private readonly LinkedList<Waiter> _waiters = new();
    private readonly long _budgetBytes;
    private long _inFlightBytes;
    private int _admittedCount;

    /// <summary>
    /// Creates a gate with an explicit budget. Public to the assembly so tests
    /// can drive a small, deterministic budget; production resolves
    /// <see cref="Shared"/>, whose budget is derived from the runtime.
    /// </summary>
    internal LeafSnapshotHydrationAdmission(long budgetBytes)
        => _budgetBytes = Math.Max(1L, budgetBytes);

    /// <summary>The process-wide gate, sized once from the heap hard limit.</summary>
    internal static LeafSnapshotHydrationAdmission Shared => SharedInstance.Value;

    /// <summary>Aggregate bytes this gate will admit concurrently.</summary>
    internal long BudgetBytes => _budgetBytes;

    /// <summary>Bytes currently reserved by admitted, undisposed leases.</summary>
    internal long InFlightBytes
    {
        get { lock (_gate) { return _inFlightBytes; } }
    }

    /// <summary>Claims admitted and not yet released.</summary>
    internal int AdmittedCount
    {
        get { lock (_gate) { return _admittedCount; } }
    }

    /// <summary>Claims currently queued behind the budget.</summary>
    internal int QueuedCount
    {
        get { lock (_gate) { return _waiters.Count; } }
    }

    /// <summary>
    /// Sizes the gate from the runtime's heap hard limit. Separated from
    /// <see cref="Shared"/> so the sizing rule is testable without a container:
    /// the value it consumes is environmental, and a rule that can only be
    /// exercised by arranging the environment is a rule that is never exercised.
    /// </summary>
    /// <param name="heapHardLimitBytes">
    /// <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/>, or a non-positive
    /// value when the runtime reports no limit.
    /// </param>
    internal static long ResolveBudgetBytes(long heapHardLimitBytes)
        => heapHardLimitBytes <= 0
            ? UnknownHeapLimitBudgetBytes
            : Math.Max(MinimumBudgetBytes, heapHardLimitBytes / HeapBudgetDivisor);

    /// <summary>
    /// Converts a snapshot's stored size into the peak managed heap a hydration
    /// of it costs, by <see cref="HydrationHeapAmplification"/>.
    /// <para>
    /// Every claim is converted here, inside the gate, and callers pass stored
    /// bytes throughout. That placement is deliberate: converting at the call
    /// sites would make it possible to admit an estimate in heap units and
    /// reconcile a measurement in stored units, which silently releases four
    /// fifths of a reservation the instant the true size arrives - the gate
    /// would read as working and behave as though it were absent.
    /// </para>
    /// </summary>
    internal static long ToHeapCostBytes(long storedBytes)
        => storedBytes <= 0
            ? 0L
            : storedBytes > long.MaxValue / HydrationHeapAmplification
                ? long.MaxValue
                : storedBytes * HydrationHeapAmplification;

    /// <summary>
    /// The largest stored snapshot size whose hydration still fits inside the
    /// whole budget - the inverse of <see cref="ToHeapCostBytes(long)"/>
    /// evaluated at <see cref="BudgetBytes"/>.
    /// <para>
    /// It exists so that a caller reasoning about stored bytes never has to
    /// divide by the amplification itself. Comparing a stored-byte figure
    /// against <see cref="BudgetBytes"/> directly is a units error that reads
    /// perfectly: both sides are bytes, the comparison compiles, and it is
    /// wrong by the amplification factor. Keeping the only two conversions in
    /// this class is what the placement of <see cref="ToHeapCostBytes(long)"/>
    /// is for, and an inverse that callers need but is not offered here is an
    /// invitation to write the division at the call site instead.
    /// </para>
    /// </summary>
    internal long MaxClaimableStoredBytes => _budgetBytes / HydrationHeapAmplification;

    /// <summary>
    /// Reserves <paramref name="estimatedBytes"/> of hydration budget, waiting in
    /// first-in-first-out order until the reservation fits or the caller is the
    /// only claimant. Dispose the returned lease to release the reservation.
    /// </summary>
    /// <param name="estimatedBytes">
    /// Best estimate of the bytes this hydration will materialise. An estimate is
    /// enough because <see cref="LeafSnapshotHydrationLease.Reconcile(long)"/>
    /// corrects it the moment the true size is known, which tightens admission
    /// for everyone still queued rather than only for the next activation.
    /// </param>
    /// <param name="cancellationToken">Abandons the claim while it is queued.</param>
    internal Task<LeafSnapshotHydrationLease> AcquireAsync(
        long estimatedBytes,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<LeafSnapshotHydrationLease>(cancellationToken);
        }

        var want = Normalise(estimatedBytes);
        Waiter waiter;
        lock (_gate)
        {
            // The empty-queue test is what makes admission first-in-first-out.
            // Without it a small claim arriving while a large one is queued would
            // be admitted ahead of it, indefinitely, and the oversized leaves -
            // the only ones whose division ever shrinks the corpus - would be the
            // ones that never ran.
            if (_waiters.Count == 0 && CanAdmitLocked(want))
            {
                AdmitLocked(want);
                return Task.FromResult(new LeafSnapshotHydrationLease(this, want, queued: false));
            }

            waiter = new Waiter(want);
            waiter.Node = _waiters.AddLast(waiter);
        }

        return waiter.WaitAsync(this, cancellationToken);
    }

    // A claim fits when it stays inside the budget, OR when nothing else holds a
    // reservation at all. The second arm is the sole-occupant rule: a snapshot
    // larger than the whole budget still has to load, because the alternative is
    // a leaf that can never come online and therefore can never be divided back
    // under bound.
    private bool CanAdmitLocked(long bytes)
        => _admittedCount == 0 || _inFlightBytes + bytes <= _budgetBytes;

    private void AdmitLocked(long bytes)
    {
        _inFlightBytes += bytes;
        _admittedCount++;
    }

    internal void Release(long heldBytes)
    {
        List<Waiter>? ready;
        lock (_gate)
        {
            _inFlightBytes -= heldBytes;
            _admittedCount--;
            ready = DrainLocked();
        }

        Complete(ready);
    }

    /// <summary>
    /// Replaces a lease's estimate with the measured size, returning the bytes
    /// now held. Shrinking a reservation can admit queued claims, so the queue is
    /// drained here as well as on release.
    /// </summary>
    internal long Reconcile(long heldBytes, long actualBytes)
    {
        var want = Normalise(actualBytes);
        List<Waiter>? ready;
        lock (_gate)
        {
            _inFlightBytes += want - heldBytes;
            ready = DrainLocked();
        }

        Complete(ready);
        return want;
    }

    internal void Abandon(Waiter waiter)
    {
        List<Waiter>? ready;
        lock (_gate)
        {
            if (waiter.Node is { } node)
            {
                _waiters.Remove(node);
                waiter.Node = null;
            }

            ready = DrainLocked();
        }

        Complete(ready);
    }

    // Admits from the head of the queue only, stopping at the first claim that
    // does not fit. Returns the waiters to complete; they are completed OUTSIDE
    // the lock so a continuation that re-enters this gate cannot deadlock it.
    private List<Waiter>? DrainLocked()
    {
        List<Waiter>? ready = null;
        while (_waiters.First is { } node)
        {
            var waiter = node.Value;
            if (!CanAdmitLocked(waiter.Bytes))
            {
                break;
            }

            _waiters.Remove(node);
            waiter.Node = null;
            AdmitLocked(waiter.Bytes);
            (ready ??= []).Add(waiter);
        }

        return ready;
    }

    private void Complete(List<Waiter>? ready)
    {
        if (ready is null)
        {
            return;
        }

        foreach (var waiter in ready)
        {
            // A waiter that lost the race to its own cancellation has already
            // been removed from the queue and completed, so the reservation it
            // was just granted would leak. Hand it straight back.
            if (!waiter.TryAdmit(new LeafSnapshotHydrationLease(this, waiter.Bytes, queued: true)))
            {
                Release(waiter.Bytes);
            }
        }
    }

    private static long Normalise(long storedBytes) => ToHeapCostBytes(storedBytes);

    internal sealed class Waiter(long bytes)
    {
        private readonly TaskCompletionSource<LeafSnapshotHydrationLease> _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal long Bytes { get; } = bytes;

        internal LinkedListNode<Waiter>? Node { get; set; }

        internal bool TryAdmit(LeafSnapshotHydrationLease lease) => _completion.TrySetResult(lease);

        internal async Task<LeafSnapshotHydrationLease> WaitAsync(
            LeafSnapshotHydrationAdmission owner,
            CancellationToken cancellationToken)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                return await _completion.Task.ConfigureAwait(false);
            }

            await using var registration = cancellationToken.Register(
                static state =>
                {
                    var waiter = (Waiter)state!;
                    waiter._completion.TrySetCanceled();
                },
                this).ConfigureAwait(false);

            try
            {
                return await _completion.Task.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                owner.Abandon(this);
                throw;
            }
        }
    }
}

/// <summary>
/// A reservation taken from <see cref="LeafSnapshotHydrationAdmission"/>. Held
/// for the whole time the hydration is materialising bytes - the blob read, the
/// payload validation and the attach or full decode - and released on dispose.
/// </summary>
internal sealed class LeafSnapshotHydrationLease : IDisposable
{
    private readonly LeafSnapshotHydrationAdmission? _owner;
    private long _heldBytes;
    private int _disposed;

    internal LeafSnapshotHydrationLease(
        LeafSnapshotHydrationAdmission? owner,
        long heldBytes,
        bool queued)
    {
        _owner = owner;
        _heldBytes = heldBytes;
        Queued = queued;
    }

    /// <summary>
    /// <see langword="true"/> when this claim had to wait behind the budget.
    /// Reported rather than inferred, because "the gate is deployed and nothing
    /// ever queued" and "the gate is not deployed" are otherwise identical.
    /// </summary>
    internal bool Queued { get; }

    /// <summary>Bytes this lease currently reserves.</summary>
    internal long HeldBytes => Volatile.Read(ref _heldBytes);

    /// <summary>
    /// A lease over no gate at all, for the paths that never reserve (a leaf with
    /// no tree id, or one that cannot address a snapshot grain). Disposing it is
    /// a no-op, so callers need no null checks.
    /// </summary>
    internal static LeafSnapshotHydrationLease None { get; } = new(null, 0L, queued: false);

    /// <summary>
    /// Corrects the reservation to the size actually loaded. An underestimate
    /// that is never reconciled would let the gate admit far more than its budget
    /// and bound nothing; correcting it here clamps the rest of the storm within
    /// the same cold start instead of only helping the next one.
    /// </summary>
    internal void Reconcile(long actualBytes)
    {
        if (_owner is null || Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        Volatile.Write(ref _heldBytes, _owner.Reconcile(Volatile.Read(ref _heldBytes), actualBytes));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _owner?.Release(Volatile.Read(ref _heldBytes));
    }
}
