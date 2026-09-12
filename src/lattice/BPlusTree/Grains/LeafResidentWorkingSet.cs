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
/// was explicitly excluded as a remedy.
/// </para>
/// <para>
/// <b>What it is derived from, and why that took two attempts</b> (issue #2788).
/// The original derivation took <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/>
/// alone, on the stated ground that it is "the same cgroup-derived figure the
/// heap hard limit itself is sized from". That ground is false in one direction
/// that matters: when no heap hard limit is configured, the property does not
/// report zero, it reports <b>host physical memory</b>. The
/// <c>heapHardLimitBytes &lt;= 0</c> branch is therefore near-dead, and the live
/// branch could derive a budget larger than the whole container grant - on a
/// 56 GiB host in a 12 GiB container, a 14 GiB budget inside a 12 GiB grant. A
/// bound whose threshold the process cannot reach before dying never engages,
/// and reports its non-engagement as a zero shed count indistinguishable from
/// healthy quiescence.
/// </para>
/// <para>
/// So the grant is now taken as the <b>smaller of the two independently
/// observed ceilings</b>: the runtime's heap hard limit and the container's own
/// cgroup memory limit, each of which may independently be unknown. Taking the
/// smaller is the only choice that is safe in both directions - the heap limit
/// can exceed the container grant (the case above), and the container grant can
/// exceed a deliberately smaller configured heap limit, in which case the heap
/// limit is the real ceiling and must win. Unknown inputs degrade to the other,
/// and two unknowns degrade to <see cref="UnknownHeapLimitBudgetBytes"/>, which
/// is exactly the pre-existing behaviour, so detection failing is never worse
/// than not detecting.
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

    /// <summary>
    /// At or above this, a cgroup memory limit is read as "unlimited" rather
    /// than as a ceiling. cgroup v1 spells unlimited as a page-aligned
    /// saturation of the page counter near <see cref="long.MaxValue"/>, which is
    /// a well-formed positive number and would otherwise be believed.
    /// <para>
    /// 4 EiB is not a boundary any real deployment sits near, so this does not
    /// trade a false positive for a false negative: no container is granted
    /// exabytes, and a limit that large is unlimited in every sense that matters
    /// to a bound denominated in leaf bytes.
    /// </para>
    /// </summary>
    internal const long CgroupUnlimitedSentinelFloor = 1L << 62;

    /// <summary>
    /// Canonical cgroup memory limit paths, v2 first. Probed in order; the first
    /// that yields a real limit wins.
    /// </summary>
    private static readonly string[] CgroupMemoryLimitPaths =
    [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ];

    private static readonly Lazy<LeafResidentWorkingSet> SharedInstance =
        new(
            () => new LeafResidentWorkingSet(
                ResolveBudgetBytes(
                    GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
                    ReadContainerMemoryLimitBytes())),
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
    /// Sizes the working set from the two ceilings the process can observe.
    /// Separated from <see cref="Shared"/> so the sizing rule is testable
    /// without a container: the values it consumes are environmental, and a rule
    /// that can only be exercised by arranging the environment is a rule that is
    /// never exercised.
    /// </summary>
    /// <param name="heapHardLimitBytes">
    /// <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/>. Note this is
    /// <b>host physical memory</b>, not zero, when no heap hard limit is
    /// configured, which is why it cannot be the sole input (issue #2788).
    /// </param>
    /// <param name="containerMemoryLimitBytes">
    /// The cgroup memory limit from <see cref="ReadContainerMemoryLimitBytes"/>,
    /// or a non-positive value when there is none or it could not be read.
    /// </param>
    internal static long ResolveBudgetBytes(long heapHardLimitBytes, long containerMemoryLimitBytes)
    {
        var grant = SmallerKnownLimit(heapHardLimitBytes, containerMemoryLimitBytes);

        return grant <= 0
            ? UnknownHeapLimitBudgetBytes
            : Math.Max(MinimumBudgetBytes, grant / HeapBudgetDivisor);
    }

    /// <summary>
    /// Returns the smaller of two ceilings, treating a non-positive value as
    /// unknown rather than as a ceiling of zero. Unknown on both sides returns a
    /// non-positive value, which the caller maps to the conservative fallback.
    /// </summary>
    private static long SmallerKnownLimit(long first, long second)
    {
        if (first <= 0)
        {
            return second;
        }

        return second <= 0 ? first : Math.Min(first, second);
    }

    /// <summary>
    /// Reads the container's memory limit from the cgroup filesystem, returning
    /// a non-positive value when there is no limit, the platform has no cgroups,
    /// or the value cannot be read or parsed.
    /// </summary>
    /// <remarks>
    /// Every failure degrades to "unknown", which
    /// <see cref="ResolveBudgetBytes"/> maps to the heap hard limit alone - the
    /// behaviour before this method existed. Detection failing is therefore
    /// never worse than not detecting, which is what licenses the deliberately
    /// narrow probe: the two canonical mount paths and nothing else. A silo in
    /// an exotic cgroup layout gets today's budget rather than a wrong one.
    /// <para>
    /// The <see cref="OperatingSystem.IsLinux"/> short-circuit is a cost guard
    /// and is deliberately <b>not</b> claimed as tested behaviour. Removing it
    /// reddens nothing and cannot: on a non-Linux host the two paths resolve
    /// against the current drive root and do not exist, so the loop returns the
    /// same zero by a slower route. It is kept because it is free and states the
    /// intent, not because a test pins it.
    /// </para>
    /// </remarks>
    internal static long ReadContainerMemoryLimitBytes()
    {
        if (!OperatingSystem.IsLinux())
        {
            return 0L;
        }

        foreach (var path in CgroupMemoryLimitPaths)
        {
            try
            {
                if (!File.Exists(path))
                {
                    continue;
                }

                var parsed = ParseCgroupMemoryLimit(File.ReadAllText(path));
                if (parsed > 0)
                {
                    return parsed;
                }
            }
            catch (IOException)
            {
                // Unreadable cgroup file. Fall through to the next candidate and
                // ultimately to unknown.
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        return 0L;
    }

    /// <summary>
    /// Parses a cgroup memory limit file body, returning <b>zero</b> for every
    /// form that means "no limit" and for every form that cannot be read as one.
    /// </summary>
    /// <remarks>
    /// Three distinct spellings of unlimited have to be recognised, and missing
    /// any one of them yields a budget derived from a nonsense ceiling rather
    /// than a safe fallback:
    /// <list type="bullet">
    /// <item>cgroup v2 writes the literal string <c>max</c>;</item>
    /// <item>cgroup v1 writes a page-aligned saturation of the counter, which is
    /// a positive <see cref="long"/> near <see cref="long.MaxValue"/> and so
    /// parses perfectly well as a number - this is the one that does damage
    /// quietly, because it divides by four into a budget of about two exabytes
    /// that no bound can ever reach;</item>
    /// <item>some kernels write that same saturation as an <b>unsigned</b>
    /// 64-bit value that overflows <see cref="long"/> entirely.</item>
    /// </list>
    /// <para>
    /// Only two clauses are needed to cover all three, and the shape is the
    /// result of a perturbation arm rather than of taste. An earlier revision
    /// had four: an explicit <c>max</c>/empty branch, a zero check, a
    /// <c>value &gt; (ulong)long.MaxValue</c> overflow guard, and the sentinel
    /// comparison. Reverting each in isolation showed the first three reddened
    /// <b>nothing</b> - the <c>max</c> and empty cases are already rejected by
    /// the parse, zero already returns zero, and an unsigned value above
    /// <see cref="long.MaxValue"/> already wraps to a negative that the caller
    /// reads as unknown. They were dead clauses that read as careful handling,
    /// which is worse than no handling because it invites trust. Comparing the
    /// sentinel in <see cref="ulong"/> space instead lets the one live clause
    /// cover both saturation spellings, so the guard that remains is the guard
    /// that is tested.
    /// </para>
    /// </remarks>
    internal static long ParseCgroupMemoryLimit(string? contents)
    {
        var text = contents?.Trim();

        if (!ulong.TryParse(text, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var value))
        {
            return 0L;
        }

        return value >= (ulong)CgroupUnlimitedSentinelFloor ? 0L : (long)value;
    }

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
