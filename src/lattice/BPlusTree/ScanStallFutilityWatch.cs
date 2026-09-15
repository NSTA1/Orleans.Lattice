namespace Orleans.Lattice;

/// <summary>
/// Observes what happened to a scan source <em>after</em> a resilient scan gave
/// up on it for futility, and records the answer as
/// <see cref="LatticeMetrics.ScanStallFutilityOutcomes"/>.
/// <para>
/// WHY THIS EXISTS. <c>scan.stall_resumptions{outcome="budget-exhausted"}</c>
/// records that a walk was killed after
/// <see cref="LatticeExtensions.DefaultScanStallResumeAttempts"/> consecutive
/// futile stalls. It does not record whether the shard it abandoned became
/// available shortly afterwards, and under load "this shard was busy for a
/// while" and "this source is genuinely not yielding" are the <em>same
/// observation</em> at that bound. A field run can therefore report a large
/// futility count and still leave unanswerable the only question that count is
/// read to answer: was the bound what stopped the job? This table supplies the
/// missing half. A non-zero <c>recovered</c> means the bound is cutting off
/// recoverable sources and is too tight; a <c>recovered</c> of zero against a
/// populated <c>still-stalled</c> means those sources really were dead and the
/// bound is right.
/// </para>
/// <para>
/// WHAT IT DELIBERATELY DOES NOT DO. It never re-issues the abandoned work and
/// never alters the termination decision. The point is to observe
/// recoverability, not to change policy: raising the consecutive budget trades
/// away the futility protection that stops a genuinely dead source being
/// retried forever, and that trade is a separate decision which this instrument
/// exists to inform rather than to pre-empt. Nothing here issues a grain call,
/// starts a timer, or allocates on the steady-state path.
/// </para>
/// <para>
/// WHY FOUR OUTCOMES AND NOT A BARE <c>recovered</c> COUNTER. A bare counter
/// reading zero is ambiguous between "nothing recovered" and "nothing looked",
/// and a reader supplies whichever complement suits the conclusion they already
/// hold. <see cref="OutcomeUnobserved"/> and <see cref="OutcomeDropped"/> make
/// both non-answers explicit, so a zero <c>recovered</c> is only evidence about
/// the source when the other arms show somebody actually looked.
/// </para>
/// <para>
/// COST. The observation hook on the scan yield path is guarded by
/// <see cref="HasWatches"/>, a single volatile read of a counter that is zero
/// whenever no walk has died futilely inside the window. A deployment that
/// never futility-terminates therefore pays one field read per yielded record,
/// which is what makes this cheap enough to leave enabled by default - an
/// instrument that ships disabled is not present when the incident happens.
/// </para>
/// </summary>
internal sealed class ScanStallFutilityWatch
{
    /// <summary>
    /// The abandoned source served a record at or beyond the position the
    /// futile walk died on, so it was recoverable and the consecutive bound cut
    /// it off early.
    /// </summary>
    internal const string OutcomeRecovered = "recovered";

    /// <summary>
    /// A later walk reached the same source and <em>also</em> terminated for
    /// futility there, so the source was not merely busy.
    /// <para>
    /// A later walk that merely <em>stalls</em> at the same source and then gets
    /// past it is deliberately NOT recorded here: that walk demonstrates the
    /// source was recoverable and is exactly the premature-bound case, so it
    /// must reach <see cref="OutcomeRecovered"/> instead. Only a second futile
    /// termination is evidence of a dead source.
    /// </para>
    /// </summary>
    internal const string OutcomeStillStalled = "still-stalled";

    /// <summary>
    /// The observation window closed without any later scan reaching the
    /// abandoned source, so this futility termination says nothing either way.
    /// <para>
    /// This arm is load-bearing. Without it the absence of
    /// <see cref="OutcomeRecovered"/> would be read as evidence the source was
    /// dead, when it may only mean the caller never came back - and a caller
    /// that abandons its job on the rethrown stall is precisely the population
    /// under investigation.
    /// </para>
    /// </summary>
    internal const string OutcomeUnobserved = "unobserved";

    /// <summary>
    /// The watch was evicted because the table was at capacity, so its answer
    /// is unknown for a reason internal to this instrument.
    /// <para>
    /// Recorded rather than silently discarded so a saturated table can never be
    /// mistaken for a quiet one: without this, a burst large enough to overflow
    /// the table would depress every other arm at exactly the moment the
    /// instrument mattered most.
    /// </para>
    /// </summary>
    internal const string OutcomeDropped = "dropped";

    /// <summary>
    /// How many futility terminations may be under observation at once.
    /// Bounded because a watch holds a scan-source reference and a stall's tag
    /// strings, and a wedge produces futility terminations far faster than a
    /// window's worth of them can resolve.
    /// </summary>
    internal const int DefaultCapacity = 256;

    /// <summary>
    /// The observation window, as a multiple of the ceiling the stall itself
    /// reported (<see cref="ScanPageStalledException.TimeoutSeconds"/>).
    /// <para>
    /// Derived from the stall rather than fixed as an absolute duration for the
    /// same reason as
    /// <see cref="LatticeExtensions.ScanStallResumeBackoffFraction"/>: it then
    /// tracks <see cref="LatticeOptions.MaxScanPageStallDuration"/> when a
    /// deployment retunes it, and there is no second knob to remember.
    /// </para>
    /// <para>
    /// The multiple is STRUCTURAL AND NOT TUNED AGAINST A MEASUREMENT. It is
    /// chosen to span several whole ceilings, so a caller that retries its job
    /// after absorbing one or two more stalls is still inside the window. A
    /// multiple that turns out too small reports itself as
    /// <see cref="OutcomeUnobserved"/> rather than as a flattering zero
    /// <see cref="OutcomeRecovered"/>, which is why erring here is safe.
    /// </para>
    /// </summary>
    internal const int WindowCeilingMultiple = 16;

    private readonly object _gate = new();

    // Keyed by the scan target's own equality. An Orleans grain reference
    // compares by grain identity, so two independently resolved references to
    // the same tree are one key; anything else falls back to its own Equals,
    // which for a plain object is reference identity. A source that fails to
    // join across passes therefore reads OutcomeUnobserved - the honest
    // non-answer - rather than a wrong one.
    private readonly Dictionary<object, List<Entry>> _watches = [];
    private readonly TimeProvider _time;
    private readonly int _capacity;

    // Read on the scan yield path without taking the lock. Zero whenever no
    // futility termination is under observation, which is the steady state.
    private int _count;

    internal ScanStallFutilityWatch(TimeProvider? timeProvider = null, int capacity = DefaultCapacity)
    {
        _time = timeProvider ?? TimeProvider.System;
        _capacity = capacity < 1 ? 1 : capacity;
    }

    /// <summary>
    /// Whether any futility termination is currently under observation. Guards
    /// the per-record hook so the steady-state cost is one volatile read.
    /// </summary>
    internal bool HasWatches => Volatile.Read(ref _count) != 0;

    /// <summary>
    /// Records a futility termination and opens a watch on the source it
    /// abandoned.
    /// <para>
    /// If a watch is already open on the same source and shard, this walk is the
    /// later walk that also gave up there, so the existing watch resolves as
    /// <see cref="OutcomeStillStalled"/> and is replaced. Chaining that way
    /// keeps every futility termination accounted for exactly once.
    /// </para>
    /// </summary>
    /// <param name="source">
    /// The scan target the walk was reading - the <c>ILattice</c> grain
    /// reference or <c>ILatticeView</c> the loop already holds. Orleans grain
    /// references compare by grain identity, so a later pass that re-resolves
    /// the same tree joins the same watch.
    /// </param>
    /// <param name="stall">The stall the walk rethrew.</param>
    /// <param name="bound">
    /// The last key the abandoned walk yielded (exclusive), or - when it yielded
    /// nothing - the inclusive lower bound it started from. <c>null</c> means it
    /// died at the origin with no bound, so any later record counts.
    /// </param>
    /// <param name="boundExclusive">
    /// Whether <paramref name="bound"/> is a yielded key (exclusive) rather than
    /// a start bound (inclusive).
    /// </param>
    /// <param name="reverse">Whether the abandoned walk was descending.</param>
    internal void OpenWatch(
        object source,
        ScanPageStalledException stall,
        string? bound,
        bool boundExclusive,
        bool reverse)
    {
        if (source is null || stall is null)
        {
            return;
        }

        var entry = new Entry(
            stall.ShardIndex,
            stall.TreeId ?? string.Empty,
            stall.Phase ?? string.Empty,
            bound,
            boundExclusive,
            reverse,
            _time.GetUtcNow() + WindowFor(stall.TimeoutSeconds));

        List<(Entry Entry, string Outcome)>? resolved = null;
        lock (_gate)
        {
            SweepLocked(ref resolved);

            // A watch already open on this source and shard means THIS walk is
            // the later walk that also gave up there, which is the evidence
            // still-stalled asserts. Resolve it before adding, so every futility
            // termination is accounted for exactly once and a repeatedly-dead
            // source chains rather than accumulating.
            if (_watches.TryGetValue(source, out var existing))
            {
                for (var i = 0; i < existing.Count; i++)
                {
                    if (existing[i].ShardIndex != entry.ShardIndex)
                    {
                        continue;
                    }

                    (resolved ??= []).Add((existing[i], OutcomeStillStalled));
                    existing.RemoveAt(i);
                    Interlocked.Decrement(ref _count);
                    break;
                }

                if (existing.Count == 0)
                {
                    _watches.Remove(source);
                }
            }

            // Eviction can empty and remove a source's list, so it must run
            // before the list this entry joins is resolved - otherwise the entry
            // could be added to a list already detached from the table and
            // silently never observed.
            EvictWhileOverCapacityLocked(ref resolved);

            if (!_watches.TryGetValue(source, out var list))
            {
                list = [];
                _watches[source] = list;
            }

            list.Add(entry);
            Interlocked.Increment(ref _count);
        }

        Record(resolved);
    }

    /// <summary>
    /// Reports a record yielded by a later scan of <paramref name="source"/>,
    /// resolving any watch whose abandoned position it has passed.
    /// <para>
    /// This is sound without any additional call because a lattice scan is a
    /// k-way merge whose per-shard cursors are all seeded before the first
    /// record is yielded (see <c>LatticeGrain.KeysAsyncCore</c>). A merged
    /// record at or beyond the abandoned position therefore proves the watched
    /// shard served the region it previously refused - which is exactly the
    /// claim <see cref="OutcomeRecovered"/> makes, and no more.
    /// </para>
    /// </summary>
    internal void NoteProgress(object source, string key)
    {
        if (source is null || key is null)
        {
            return;
        }

        List<(Entry Entry, string Outcome)>? resolved = null;
        lock (_gate)
        {
            SweepLocked(ref resolved);

            if (_watches.TryGetValue(source, out var list))
            {
                for (var i = list.Count - 1; i >= 0; i--)
                {
                    if (!IsPast(key, list[i]))
                    {
                        continue;
                    }

                    (resolved ??= []).Add((list[i], OutcomeRecovered));
                    list.RemoveAt(i);
                    Interlocked.Decrement(ref _count);
                }

                if (list.Count == 0)
                {
                    _watches.Remove(source);
                }
            }
        }

        Record(resolved);
    }

    /// <summary>
    /// Closes out any watch whose window has expired. Called at the start of
    /// every resilient scan, which is the event that would otherwise have
    /// resolved one, so expiry needs no timer of its own.
    /// </summary>
    internal void Sweep()
    {
        if (!HasWatches)
        {
            return;
        }

        List<(Entry Entry, string Outcome)>? resolved = null;
        lock (_gate)
        {
            SweepLocked(ref resolved);
        }

        Record(resolved);
    }

    /// <summary>
    /// The observation window derived from a stall's own reported ceiling,
    /// falling back to <see cref="LatticeOptions.DefaultMaxScanPageDuration"/>
    /// when the stall carries no usable value, so the window is always derived
    /// from a real bound rather than from a literal.
    /// </summary>
    internal static TimeSpan WindowFor(double ceilingSeconds)
    {
        var seconds = double.IsFinite(ceilingSeconds) && ceilingSeconds > 0
            ? ceilingSeconds
            : LatticeOptions.DefaultMaxScanPageDuration.TotalSeconds;

        return TimeSpan.FromSeconds(seconds * WindowCeilingMultiple);
    }

    private static bool IsPast(string key, Entry entry)
    {
        if (entry.Bound is null)
        {
            return true;
        }

        var comparison = string.CompareOrdinal(key, entry.Bound);
        if (entry.Reverse)
        {
            comparison = -comparison;
        }

        return entry.BoundExclusive ? comparison > 0 : comparison >= 0;
    }

    private void SweepLocked(ref List<(Entry Entry, string Outcome)>? resolved)
    {
        if (_count == 0)
        {
            return;
        }

        var now = _time.GetUtcNow();
        List<object>? emptied = null;
        foreach (var (source, list) in _watches)
        {
            for (var i = list.Count - 1; i >= 0; i--)
            {
                if (list[i].Deadline > now)
                {
                    continue;
                }

                (resolved ??= []).Add((list[i], OutcomeUnobserved));
                list.RemoveAt(i);
                Interlocked.Decrement(ref _count);
            }

            if (list.Count == 0)
            {
                (emptied ??= []).Add(source);
            }
        }

        if (emptied is not null)
        {
            foreach (var source in emptied)
            {
                _watches.Remove(source);
            }
        }
    }

    private void EvictWhileOverCapacityLocked(ref List<(Entry Entry, string Outcome)>? resolved)
    {
        while (_count >= _capacity)
        {
            object? oldestSource = null;
            var oldestIndex = -1;
            var oldestDeadline = DateTimeOffset.MaxValue;

            foreach (var (source, list) in _watches)
            {
                for (var i = 0; i < list.Count; i++)
                {
                    if (list[i].Deadline >= oldestDeadline)
                    {
                        continue;
                    }

                    oldestDeadline = list[i].Deadline;
                    oldestSource = source;
                    oldestIndex = i;
                }
            }

            if (oldestSource is null)
            {
                return;
            }

            var victims = _watches[oldestSource];
            (resolved ??= []).Add((victims[oldestIndex], OutcomeDropped));
            victims.RemoveAt(oldestIndex);
            Interlocked.Decrement(ref _count);
            if (victims.Count == 0)
            {
                _watches.Remove(oldestSource);
            }
        }
    }

    private static void Record(List<(Entry Entry, string Outcome)>? resolved)
    {
        if (resolved is null)
        {
            return;
        }

        foreach (var (entry, outcome) in resolved)
        {
            LatticeMetrics.ScanStallFutilityOutcomes.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, entry.TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, entry.Phase),
                new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcome),
                LatticeTenantLabel.ForTree(entry.TreeId));
        }
    }

    private sealed record Entry(
        int ShardIndex,
        string TreeId,
        string Phase,
        string? Bound,
        bool BoundExclusive,
        bool Reverse,
        DateTimeOffset Deadline);
}
