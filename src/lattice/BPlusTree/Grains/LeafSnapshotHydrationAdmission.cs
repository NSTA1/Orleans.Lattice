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
/// <para>
/// <b>What this gate models, and what it does not (issue #2844).</b> It models
/// two predicates, and they are different quantities that fail for different
/// reasons. The first is <i>aggregate</i>: how many bytes all concurrent
/// hydrations may hold at once, bounded by <see cref="BudgetBytes"/>. The
/// second is <i>contiguity</i>: how large a single unbroken allocation a
/// hydration may attempt while other hydrations are also churning the large
/// object heap, bounded by
/// <see cref="ConcurrentContiguousCeilingBytes"/> - a claim above it runs as
/// sole occupant. Aggregate headroom does not predict contiguous feasibility,
/// which is why a claim using a third of the budget could be admitted with
/// three quarters of it free and still throw
/// <see cref="OutOfMemoryException"/> materialising one buffer.
/// </para>
/// <para>
/// It does <b>not</b> model whether a given contiguous allocation will in fact
/// succeed, and it cannot: the runtime exposes no largest-free-region figure,
/// so there is nothing to test a request against.
/// <see cref="GCMemoryInfo"/> reports totals and fragmentation, neither of
/// which answers the question. Sole occupancy therefore improves the odds - it
/// removes the concurrent large-object churn that is the one contributor this
/// process controls - and guarantees nothing. A hydration that fails anyway is
/// reported as such rather than being silently reattributed to a budget that
/// was never exhausted; see <c>LeafSnapshotUnaffordableException</c>, whose
/// message distinguishes a claim that exceeded the budget from one that fitted
/// inside it and failed regardless.
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

    /// <summary>
    /// Multiplier converting a snapshot's <b>stored</b> size into the
    /// <b>largest single contiguous</b> allocation one hydration of it
    /// requires - a different quantity from
    /// <see cref="HydrationHeapAmplification"/>, and the one that actually
    /// failed in issue #2844.
    /// <para>
    /// Of the four multiples the amplification above enumerates, the two
    /// largest are the UTF-16 <see cref="string"/> of the whole JSON document
    /// (2x the stored byte length) and the <c>char[]</c> it is copied from
    /// (another 2x). Both are single objects, so each must be satisfied by one
    /// <b>unbroken</b> run of memory on the large object heap; the parsed graph
    /// on top is many small objects and imposes no such requirement. The
    /// largest single contiguous request a hydration makes is therefore twice
    /// the stored size, not five times it.
    /// </para>
    /// </summary>
    internal const int ContiguousAllocationMultiple = 2;

    /// <summary>
    /// The largest contiguous allocation this gate will let a hydration attempt
    /// <b>alongside</b> other hydrations. A claim above it is admitted only as
    /// sole occupant, and excludes every other claim for as long as it is held.
    /// <para>
    /// <b>This is deliberately an absolute figure, and deliberately NOT derived
    /// from the memory grant.</b> Every other limit in this system - the
    /// hydration budget above (heap limit / <see cref="HeapBudgetDivisor"/>),
    /// the resident working-set budget, the write-ahead-log replay gate - scales
    /// with the grant and therefore admits <i>more</i> concurrent work as the
    /// grant grows. That is correct for a quantity measured in total bytes and
    /// wrong for this one: whether one unbroken multi-hundred-megabyte run can
    /// be found does not improve because the container was given more memory, so
    /// a ceiling that rose with the grant would relax exactly as the population
    /// it governs got larger. Issue #2844 is that failure observed in
    /// production: a claim reserving 32.6% of a 1.125 GiB budget was admitted
    /// with 776 MiB of headroom and threw
    /// <see cref="OutOfMemoryException"/> inside the provider's blob read
    /// anyway.
    /// </para>
    /// <para>
    /// The value is twice <see cref="LatticeOptions.DefaultMaxLeafBytes"/> -
    /// that is, <see cref="ContiguousAllocationMultiple"/> applied to a leaf of
    /// exactly the size a leaf is configured to be. That is the whole
    /// justification, and it is a statement about leaves rather than about
    /// memory: a leaf within its own size bound is by definition healthy and
    /// must hydrate concurrently with its peers, and a leaf whose contiguous
    /// requirement exceeds what a bound-sized leaf needs is by definition
    /// oversized - the population this gate exists to bind on, and the
    /// population issue #2844 was measured against, where the corpus carried
    /// blobs at 3.5x the bound. A tree configured with a larger
    /// <see cref="LatticeOptions.MaxLeafBytes"/> than the default is therefore
    /// judged against the default rather than against its own setting, which is
    /// intentional: the constraint is physical, so it cannot be relaxed by
    /// configuring the thing that provokes it.
    /// </para>
    /// <para>
    /// Getting this number somewhat wrong is cheap <b>in one direction only</b>,
    /// which is why it is safe to state it at all. Too low, and healthy
    /// hydrations serialise that need not have: a cold start is slower, and a
    /// cold start is already the slow path. Too high, and the gate fails to
    /// prevent a failure it was never able to prevent reliably in the first
    /// place, leaving the behaviour exactly as it is today. Neither arm refuses
    /// a claim, so neither arm can convert this into a leaf that never comes
    /// online - the outcome the sole-occupant rule exists to rule out, and the
    /// reason a hard cap on claim size was rejected as the remedy.
    /// </para>
    /// </summary>
    internal const long ConcurrentContiguousCeilingBytes =
        ContiguousAllocationMultiple * LatticeOptions.DefaultMaxLeafBytes;

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
    private int _exclusiveCount;

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
    /// Claims admitted as sole occupant because their contiguous requirement
    /// exceeded <see cref="ConcurrentContiguousCeilingBytes"/>, and not yet
    /// released. Never more than one, and reported rather than inferred for the
    /// same reason <see cref="LeafSnapshotHydrationLease.Queued"/> is: "the
    /// contiguity rule is deployed and nothing ever tripped it" and "the
    /// contiguity rule is not deployed" are otherwise identical readings.
    /// </summary>
    internal int ExclusiveCount
    {
        get { lock (_gate) { return _exclusiveCount; } }
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
    /// The largest single contiguous allocation a hydration of
    /// <paramref name="storedBytes"/> requires, by
    /// <see cref="ContiguousAllocationMultiple"/>.
    /// <para>
    /// Kept here beside <see cref="ToHeapCostBytes(long)"/> and for the same
    /// reason: two different multiples of the same stored figure, both
    /// denominated in bytes, are trivially swapped at a call site and the
    /// mistake compiles and reads correctly. Neither conversion is performed
    /// anywhere else.
    /// </para>
    /// </summary>
    internal static long ToContiguousBytes(long storedBytes)
        => storedBytes <= 0
            ? 0L
            : storedBytes > long.MaxValue / ContiguousAllocationMultiple
                ? long.MaxValue
                : storedBytes * ContiguousAllocationMultiple;

    /// <summary>
    /// Whether a hydration of <paramref name="storedBytes"/> must run as sole
    /// occupant because its largest contiguous allocation exceeds
    /// <see cref="ConcurrentContiguousCeilingBytes"/>.
    /// <para>
    /// The test is on the <b>contiguous</b> requirement, never on the heap cost
    /// or the stored size. Those are the quantities the aggregate budget is
    /// denominated in, and testing either of them here would reproduce the
    /// defect: the whole point is that a claim can be comfortable in aggregate
    /// terms and impossible in contiguous ones.
    /// </para>
    /// </summary>
    internal static bool RequiresSoleOccupancy(long storedBytes)
        => ToContiguousBytes(storedBytes) > ConcurrentContiguousCeilingBytes;

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
        var exclusive = RequiresSoleOccupancy(estimatedBytes);
        Waiter waiter;
        lock (_gate)
        {
            // The empty-queue test is what makes admission first-in-first-out.
            // Without it a small claim arriving while a large one is queued would
            // be admitted ahead of it, indefinitely, and the oversized leaves -
            // the only ones whose division ever shrinks the corpus - would be the
            // ones that never ran.
            if (_waiters.Count == 0 && CanAdmitLocked(want, exclusive))
            {
                AdmitLocked(want, exclusive);
                return Task.FromResult(
                    new LeafSnapshotHydrationLease(this, estimatedBytes, want, exclusive, queued: false));
            }

            waiter = new Waiter(estimatedBytes, want, exclusive);
            waiter.Node = _waiters.AddLast(waiter);
        }

        return waiter.WaitAsync(this, cancellationToken);
    }

    // A claim fits when it stays inside the budget, OR when nothing else holds a
    // reservation at all. The second arm is the sole-occupant rule: a snapshot
    // larger than the whole budget still has to load, because the alternative is
    // a leaf that can never come online and therefore can never be divided back
    // under bound.
    //
    // The contiguity rule (issue #2844) is layered on top as a MUTUAL exclusion,
    // and it has to be mutual to be worth anything. Admitting a large-contiguity
    // claim only when the gate is empty, while still letting later claims join it
    // once admitted, would leave its allocation racing exactly the concurrent
    // large-object churn the rule exists to remove - and would do so by
    // construction, because that claim reserves only a fraction of the budget and
    // so leaves plenty of room for others to be admitted alongside it. Hence both
    // directions: an exclusive claim waits for an empty gate, and an empty gate is
    // what every other claim then waits for.
    private bool CanAdmitLocked(long bytes, bool exclusive)
    {
        if (_exclusiveCount > 0)
        {
            return false;
        }

        return exclusive
            ? _admittedCount == 0
            : _admittedCount == 0 || _inFlightBytes + bytes <= _budgetBytes;
    }

    private void AdmitLocked(long bytes, bool exclusive)
    {
        _inFlightBytes += bytes;
        _admittedCount++;
        if (exclusive)
        {
            _exclusiveCount++;
        }
    }

    internal void Release(long heldBytes, bool exclusive)
    {
        List<Waiter>? ready;
        lock (_gate)
        {
            _inFlightBytes -= heldBytes;
            _admittedCount--;
            if (exclusive)
            {
                _exclusiveCount--;
            }

            ready = DrainLocked();
        }

        Complete(ready);
    }

    /// <summary>
    /// Replaces a lease's estimate with the measured size, returning the bytes
    /// now held. Shrinking a reservation can admit queued claims, so the queue is
    /// drained here as well as on release.
    /// <para>
    /// Exclusivity is fixed at admission and is deliberately never revisited
    /// here. A measurement arrives only after the blob has been read, so the
    /// contiguous allocation this rule governs has already either succeeded or
    /// thrown; promoting a lease to exclusive at that point would exclude other
    /// claims to protect an allocation that is over, and demoting one would
    /// release a guarantee that was already spent. Only the aggregate figure is
    /// still live at this point, so only the aggregate figure is corrected.
    /// </para>
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
            if (!CanAdmitLocked(waiter.Bytes, waiter.Exclusive))
            {
                break;
            }

            _waiters.Remove(node);
            waiter.Node = null;
            AdmitLocked(waiter.Bytes, waiter.Exclusive);
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
            var lease = new LeafSnapshotHydrationLease(
                this, waiter.StoredBytes, waiter.Bytes, waiter.Exclusive, queued: true);
            if (!waiter.TryAdmit(lease))
            {
                Release(waiter.Bytes, waiter.Exclusive);
            }
        }
    }

    private static long Normalise(long storedBytes) => ToHeapCostBytes(storedBytes);

    internal sealed class Waiter(long storedBytes, long bytes, bool exclusive)
    {
        private readonly TaskCompletionSource<LeafSnapshotHydrationLease> _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Stored bytes this claim was sized from.</summary>
        internal long StoredBytes { get; } = storedBytes;

        internal long Bytes { get; } = bytes;

        /// <summary>Whether this claim must be admitted as sole occupant.</summary>
        internal bool Exclusive { get; } = exclusive;

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
    private long _storedBytes;
    private long _heldBytes;
    private int _disposed;

    internal LeafSnapshotHydrationLease(
        LeafSnapshotHydrationAdmission? owner,
        long storedBytes,
        long heldBytes,
        bool exclusive,
        bool queued)
    {
        _owner = owner;
        _storedBytes = storedBytes;
        _heldBytes = heldBytes;
        Exclusive = exclusive;
        Queued = queued;
    }

    /// <summary>
    /// <see langword="true"/> when this claim had to wait behind the budget.
    /// Reported rather than inferred, because "the gate is deployed and nothing
    /// ever queued" and "the gate is not deployed" are otherwise identical.
    /// </summary>
    internal bool Queued { get; }

    /// <summary>
    /// <see langword="true"/> when this claim was admitted as <b>sole
    /// occupant</b> because its contiguous requirement exceeded
    /// <see cref="LeafSnapshotHydrationAdmission.ConcurrentContiguousCeilingBytes"/>
    /// (issue #2844), rather than merely because it fitted the remaining
    /// aggregate budget.
    /// </summary>
    internal bool Exclusive { get; }

    /// <summary>Bytes this lease currently reserves.</summary>
    internal long HeldBytes => Volatile.Read(ref _heldBytes);

    /// <summary>
    /// The largest single contiguous allocation the hydration under this lease
    /// requires. Distinct from <see cref="HeldBytes"/>, which is an aggregate
    /// figure: this is the one that has to be satisfied by an unbroken run of
    /// memory, and therefore the one that can fail while the aggregate budget is
    /// still comfortable. Reported on the failure path so that an
    /// <see cref="OutOfMemoryException"/> raised inside the storage provider is
    /// attributable to the quantity that actually ran out.
    /// </summary>
    internal long ContiguousBytes
        => LeafSnapshotHydrationAdmission.ToContiguousBytes(Volatile.Read(ref _storedBytes));

    /// <summary>
    /// A lease over no gate at all, for the paths that never reserve (a leaf with
    /// no tree id, or one that cannot address a snapshot grain). Disposing it is
    /// a no-op, so callers need no null checks.
    /// </summary>
    internal static LeafSnapshotHydrationLease None { get; } =
        new(null, 0L, 0L, exclusive: false, queued: false);

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

        Volatile.Write(ref _storedBytes, actualBytes);
        Volatile.Write(ref _heldBytes, _owner.Reconcile(Volatile.Read(ref _heldBytes), actualBytes));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _owner?.Release(Volatile.Read(ref _heldBytes), Exclusive);
    }
}
