namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo, byte-denominated bound on the <b>resident</b> leaf working set
/// (issue #2767). Where <see cref="LeafSnapshotHydrationAdmission"/> bounds how
/// many snapshot hydrations are materialising <i>at once</i>, this bounds how
/// much hydrated leaf content stays resident <i>after</i> they complete, and
/// sheds the excess by deactivating leaves.
/// <para>
/// The two are not alternatives and neither subsumes the other. #2766's gate is
/// released when an activation finishes, so a cost that survives the release is
/// invisible to it at every budget and at every amplification constant. That
/// cost was measured directly: attaching a leaf snapshot retains the whole
/// encoded frame for the life of the activation
/// (<see cref="LeafSnapshotHydrationSource"/> holds it so unhydrated blocks
/// remain seekable), so resident heap rose at <b>1.008x the frame size per
/// retained activation</b> with <b>zero rows materialised and zero frame bytes
/// read</b>, and did not fall by one page under a forced blocking compacting
/// gen2 collection while hydration was quiescent. The identical hydrations
/// performed and then released cost 0.2% of that. The exhaustion is resident,
/// not transient.
/// </para>
/// <para>
/// <b>Why deactivation rather than releasing the frame in place.</b> Dropping a
/// frame while any of its blocks are still unmaterialised would lose those rows,
/// and materialising them first is not an eviction strategy: decoded rows are
/// <i>larger</i> than the frame they came from, so "hydrate then drop" raises
/// the peak it is meant to lower. Deactivation releases the entire activation,
/// and the reload path it implies already exists and is already exercised -
/// Orleans reactivates on the next touch and
/// <c>TryRehydrateFromSnapshotAsync</c> re-attaches. No new durability claim is
/// made and no new synchronous-reload seam is introduced.
/// </para>
/// <para>
/// <b>The budget is derived, never configured</b>, for the same reason #2766's
/// is: a bound that only works once an operator sets it does not fix a process
/// that is already exhausting its heap, and raising the container's memory limit
/// was explicitly excluded as a remedy. The ceiling comes from
/// <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/>, the same cgroup-derived
/// figure the heap hard limit itself is sized from.
/// </para>
/// </summary>
internal sealed class LeafResidentWorkingSet
{
    /// <summary>
    /// Divisor applied to the heap hard limit to size the resident working set.
    /// <para>
    /// One quarter, against the one eighth
    /// <see cref="LeafSnapshotHydrationAdmission.HeapBudgetDivisor"/> takes for
    /// in-flight hydration. The two denominators are disjoint - one bounds bytes
    /// materialising, the other bytes retained - so together they claim three
    /// eighths of the limit and leave five eighths for the vector plane, the WAL
    /// replay path and the ordinary request path, all of which are live while a
    /// cold start runs.
    /// </para>
    /// <para>
    /// Erring low costs reactivations, which are bounded work on an already-slow
    /// path. Erring high re-admits the exhaustion this exists to stop, which is
    /// unbounded. The asymmetry is why this is a quarter rather than a half.
    /// </para>
    /// </summary>
    internal const int HeapBudgetDivisor = 4;

    /// <summary>
    /// Floor on the derived budget, so a very small heap does not shed every
    /// leaf the moment it activates and convert an exhaustion into a livelock.
    /// </summary>
    internal const long MinimumBudgetBytes = 64L * 1024 * 1024;

    /// <summary>
    /// Budget used when the runtime reports no heap hard limit at all
    /// (<see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> of zero). An
    /// unknown ceiling is not licence to be unbounded: the defect being fixed is
    /// an unbounded resident set, so the unknown case takes a fixed,
    /// conservative bound rather than opting out.
    /// </summary>
    internal const long UnknownHeapLimitBudgetBytes = 1024L * 1024 * 1024;

    private static readonly Lazy<LeafResidentWorkingSet> SharedInstance =
        new(
            () => new LeafResidentWorkingSet(
                ResolveBudgetBytes(GC.GetGCMemoryInfo().TotalAvailableMemoryBytes)),
            LazyThreadSafetyMode.ExecutionAndPublication);

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> PrimedShedTrees = new(StringComparer.Ordinal);

    private readonly object _gate = new();
    private readonly LinkedList<LeafResidencyRegistration> _registrations = new();
    private readonly long _budgetBytes;
    private long _residentBytes;
    private long _sequence;

    /// <summary>
    /// Creates a working set with an explicit budget. Internal so tests can
    /// drive a small, deterministic budget; production resolves
    /// <see cref="Shared"/>, whose budget is derived from the runtime.
    /// </summary>
    internal LeafResidentWorkingSet(long budgetBytes)
        => _budgetBytes = Math.Max(1L, budgetBytes);

    /// <summary>The process-wide working set, sized once from the heap hard limit.</summary>
    internal static LeafResidentWorkingSet Shared => SharedInstance.Value;

    /// <summary>Aggregate resident leaf bytes this working set will retain.</summary>
    internal long BudgetBytes => _budgetBytes;

    /// <summary>Bytes currently accounted to live, un-shed registrations.</summary>
    internal long ResidentBytes
    {
        get { lock (_gate) { return _residentBytes; } }
    }

    /// <summary>Registrations held and not yet released.</summary>
    internal int RegisteredCount
    {
        get { lock (_gate) { return _registrations.Count; } }
    }

    /// <summary>
    /// Sizes the working set from the runtime's heap hard limit. Separated from
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
    /// Accounts <paramref name="residentBytes"/> against the working set for an
    /// activation that has just come online, shedding older leaves if that puts
    /// the total over budget. Dispose the returned registration when the
    /// activation is torn down.
    /// </summary>
    /// <param name="treeId">Tree the leaf belongs to, for the shed counter's tag.</param>
    /// <param name="residentBytes">
    /// Bytes this activation retains: its hydration frame, if it still holds
    /// one, plus its materialised rows.
    /// </param>
    /// <param name="snapshotBanked">
    /// <see langword="true"/> when this activation rehydrated from a durable
    /// snapshot. See <see cref="SelectShedCandidateLocked"/> for why this, and
    /// not recency alone, is the primary ordering.
    /// </param>
    /// <param name="shed">
    /// Invoked, outside the lock, to deactivate the leaf. Must be a graceful
    /// deactivation so the leaf's capture-on-deactivate seam still runs.
    /// </param>
    /// <param name="isPinned">
    /// Consulted at selection time; a registration that reports itself pinned is
    /// never shed. Supplied so an operation that spans several turns - a split
    /// in particular - cannot be interrupted by a sweep. Optional: a leaf that
    /// supplies none is always eligible.
    /// </param>
    internal LeafResidencyRegistration Register(
        string treeId,
        long residentBytes,
        bool snapshotBanked,
        Action shed,
        Func<bool>? isPinned = null)
    {
        ArgumentNullException.ThrowIfNull(shed);

        PrimeShedCounter(treeId);

        var registration = new LeafResidencyRegistration(
            this, treeId, Math.Max(0L, residentBytes), snapshotBanked, shed, isPinned);

        List<LeafResidencyRegistration>? condemned;
        lock (_gate)
        {
            registration.Sequence = ++_sequence;
            registration.Node = _registrations.AddLast(registration);
            _residentBytes += registration.Bytes;
            condemned = ShedToBudgetLocked(registration);
        }

        // Outside the lock: a deactivation callback is arbitrary code and can
        // re-enter this type (its own Release runs on teardown), so invoking it
        // under the lock would deadlock the whole silo's activation path.
        Shed(condemned);
        return registration;
    }

    /// <summary>
    /// Selects leaves to shed until the total is back under budget, marking each
    /// as condemned and deducting its bytes immediately.
    /// <para>
    /// Deducting at condemnation rather than at teardown is what stops a single
    /// sweep condemning the entire working set: deactivation is asynchronous, so
    /// a sweep that waited for the bytes to actually come back would keep
    /// selecting against an unchanged total and shed everything. The matching
    /// <see cref="LeafResidencyRegistration.Dispose"/> therefore does not deduct
    /// a second time.
    /// </para>
    /// </summary>
    private List<LeafResidencyRegistration>? ShedToBudgetLocked(LeafResidencyRegistration admitting)
    {
        List<LeafResidencyRegistration>? condemned = null;
        while (_residentBytes > _budgetBytes)
        {
            var candidate = SelectShedCandidateLocked(admitting);
            if (candidate is null)
            {
                // Forward progress. Nothing older is available to shed, so the
                // admitting leaf stays resident even though it is over budget -
                // exactly the sole-occupant rule #2766 established, and for the
                // same reason: a leaf that can never come online can never be
                // divided back under bound, so refusing it converts an
                // exhaustion into a permanent stall.
                break;
            }

            candidate.Condemn();
            _residentBytes -= candidate.Bytes;
            (condemned ??= []).Add(candidate);
        }

        return condemned;
    }

    /// <summary>
    /// Chooses the next leaf to shed: a snapshot-banked leaf in preference to an
    /// unbanked one, and the oldest activation within whichever class that
    /// selects.
    /// <para>
    /// <b>The class preference is the load-bearing clause, and pure recency
    /// would be actively harmful here.</b> Shedding is only cheap for a leaf
    /// that can come back on the fast path. A snapshot-banked leaf reactivates
    /// by re-attaching its snapshot and replaying the tail. A leaf with no
    /// banked snapshot reactivates <i>cold</i>, replaying the whole readable WAL
    /// window, and a cold activation must first queue for a replay permit -
    /// which is precisely the queue measured in issue #2768 as failing to drain,
    /// at 103 cancellations across 18 leaves, 100% of them cancelled while still
    /// queued and 0% mid-replay. Shedding an unbanked leaf therefore does not
    /// buy a reload, it buys an entry into a saturated queue whose cancellations
    /// bank nothing and reproduce their own cause. A recency-only order would
    /// convert a resident-memory problem into additional load on the one queue
    /// already known not to drain.
    /// </para>
    /// <para>
    /// An unbanked leaf is still shed when no banked candidate remains, rather
    /// than never: refusing outright would let a working set consisting only of
    /// unbanked leaves sit permanently over budget, which is the exhaustion this
    /// type exists to bound. The preference orders the two classes; it does not
    /// exempt either.
    /// </para>
    /// <para>
    /// Only a <b>strictly older</b> registration is eligible. That is what stops
    /// a leaf being shed between attaching its frame and taking any benefit from
    /// it: the sweep an activation triggers can never select that activation,
    /// nor any activation admitted after it, so every leaf is guaranteed to
    /// outlive at least the admission that would otherwise have evicted it.
    /// </para>
    /// <para>
    /// A <b>pinned</b> registration is never selected. Orleans defers a
    /// requested deactivation to the end of the current turn, so a turn-local
    /// operation needs no protection - but a split spans several turns, and a
    /// batched transfer spans many, so a sweep landing between batches would
    /// interrupt a partial transfer. The pin is the exclusion that makes a
    /// multi-turn operation safe from this type, and it is consulted at
    /// selection rather than at shed time because bytes are deducted when a
    /// candidate is condemned: declining later would leave the ledger
    /// permanently under-counting a leaf that is still resident.
    /// </para>
    /// </summary>
    private LeafResidencyRegistration? SelectShedCandidateLocked(LeafResidencyRegistration admitting)
    {
        LeafResidencyRegistration? best = null;
        for (var node = _registrations.First; node is not null; node = node.Next)
        {
            var candidate = node.Value;
            if (candidate.IsCondemned || candidate.Sequence >= admitting.Sequence)
            {
                continue;
            }

            if (candidate.IsPinned)
            {
                continue;
            }

            if (best is null)
            {
                best = candidate;
                continue;
            }

            if (candidate.SnapshotBanked != best.SnapshotBanked)
            {
                if (candidate.SnapshotBanked)
                {
                    best = candidate;
                }

                continue;
            }

            if (candidate.Sequence < best.Sequence)
            {
                best = candidate;
            }
        }

        return best;
    }

    private static void Shed(List<LeafResidencyRegistration>? condemned)
    {
        if (condemned is null)
        {
            return;
        }

        foreach (var registration in condemned)
        {
            registration.InvokeShed();
        }
    }

    /// <summary>
    /// Emits both class arms of <see cref="LatticeMetrics.LeafResidencySheds"/>
    /// at zero the first time a tree registers, so an absent series means "this
    /// build does not bound residency" rather than "this tree never needed
    /// shedding".
    /// </summary>
    private static void PrimeShedCounter(string treeId)
    {
        if (treeId.Length == 0 || !PrimedShedTrees.TryAdd(treeId, 0))
        {
            return;
        }

        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var tenantTag = LatticeTenantLabel.ForTree(treeId);
        LatticeMetrics.LeafResidencySheds.Add(0, treeTag, LatticeMetrics.LeafResidencyClassBanked, tenantTag);
        LatticeMetrics.LeafResidencySheds.Add(0, treeTag, LatticeMetrics.LeafResidencyClassUnbanked, tenantTag);
    }

    internal void Release(LeafResidencyRegistration registration)
    {
        lock (_gate)
        {
            if (registration.Node is { } node)
            {
                _registrations.Remove(node);
                registration.Node = null;
            }

            // Condemned registrations already had their bytes deducted by the
            // sweep that selected them; deducting again here would drive the
            // total negative and silently raise the effective budget.
            if (!registration.IsCondemned)
            {
                _residentBytes -= registration.Bytes;
            }
        }
    }
}

/// <summary>
/// One activation's claim on <see cref="LeafResidentWorkingSet"/>. Held for the
/// life of the activation and disposed on teardown.
/// </summary>
internal sealed class LeafResidencyRegistration : IDisposable
{
    private readonly LeafResidentWorkingSet? _owner;
    private readonly Action? _shed;
    private readonly Func<bool>? _isPinned;
    private int _disposed;
    private int _condemned;

    internal LeafResidencyRegistration(
        LeafResidentWorkingSet? owner,
        string treeId,
        long bytes,
        bool snapshotBanked,
        Action? shed,
        Func<bool>? isPinned = null)
    {
        _owner = owner;
        TreeId = treeId;
        Bytes = bytes;
        SnapshotBanked = snapshotBanked;
        _shed = shed;
        _isPinned = isPinned;
    }

    /// <summary>
    /// A registration over no working set at all, for the activation paths that
    /// never account (a leaf with no tree id). Disposing it is a no-op, so
    /// callers need no null checks.
    /// </summary>
    internal static LeafResidencyRegistration None { get; } =
        new(null, string.Empty, 0L, snapshotBanked: false, shed: null);

    /// <summary>Tree this leaf belongs to.</summary>
    internal string TreeId { get; }

    /// <summary>Resident bytes this registration accounts for.</summary>
    internal long Bytes { get; }

    /// <summary>
    /// <see langword="true"/> when the activation rehydrated from a durable
    /// snapshot, so shedding it costs a snapshot re-attach rather than a cold
    /// whole-window WAL replay.
    /// </summary>
    internal bool SnapshotBanked { get; }

    /// <summary>Monotonic admission order, oldest first.</summary>
    internal long Sequence { get; set; }

    /// <summary>Position in the owner's registration list.</summary>
    internal LinkedListNode<LeafResidencyRegistration>? Node { get; set; }

    /// <summary>
    /// <see langword="true"/> once a sweep has selected this registration for
    /// shedding and deducted its bytes.
    /// </summary>
    internal bool IsCondemned => Volatile.Read(ref _condemned) != 0;

    /// <summary>
    /// <see langword="true"/> while this leaf is running an operation that must
    /// not be interrupted by a sweep - a split, whose state is persisted and
    /// therefore spans turns.
    /// <para>
    /// A predicate that throws is read as <b>pinned</b>. Declining to shed risks
    /// sitting over budget for one sweep, which the next admission re-attempts;
    /// shedding on a failed read risks interrupting a multi-turn transfer, which
    /// nothing re-attempts. The recoverable failure is the correct one to take.
    /// </para>
    /// </summary>
    internal bool IsPinned
    {
        get
        {
            if (_isPinned is null)
            {
                return false;
            }

            try
            {
                return _isPinned();
            }
            catch
            {
                return true;
            }
        }
    }

    /// <summary>Marks this registration as selected for shedding.</summary>
    internal void Condemn() => Volatile.Write(ref _condemned, 1);

    /// <summary>
    /// Runs the deactivation callback. Swallows everything: shedding is a
    /// memory-pressure remedy running on an unrelated leaf's activation turn, so
    /// a failure to deactivate one leaf must not fail the activation that
    /// triggered the sweep. The bytes stay deducted either way, which is the
    /// conservative direction - the next sweep re-derives the total from the
    /// live registrations.
    /// </summary>
    internal void InvokeShed()
    {
        if (TreeId.Length != 0)
        {
            LatticeMetrics.LeafResidencySheds.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                SnapshotBanked
                    ? LatticeMetrics.LeafResidencyClassBanked
                    : LatticeMetrics.LeafResidencyClassUnbanked,
                LatticeTenantLabel.ForTree(TreeId));
        }

        try
        {
            _shed?.Invoke();
        }
        catch
        {
            // Deliberately swallowed - see the summary above.
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _owner?.Release(this);
    }
}
