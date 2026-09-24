using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Identity of one bounded leaf read issued by a scan-page walk: the leaf,
    /// the shape of the answer, and every argument that narrows it.
    /// <para>
    /// Two reads share an identity exactly when one can stand in for the other.
    /// That is the whole safety argument for
    /// <see cref="ReadLeafEntriesAsync"/> and <see cref="ReadLeafKeysAsync"/>,
    /// so the record carries <em>every</em> parameter of the call it names -
    /// adding an argument to <see cref="IBPlusLeafGrain.GetEntriesAsync"/>
    /// without adding it here would let a narrower read be answered by a wider
    /// one.
    /// </para>
    /// <para>
    /// <see cref="LatticePredicateNode"/> is a <c>readonly record struct</c>,
    /// so the predicate compares structurally. Where it holds a child array the
    /// generated comparison falls back to reference equality, which makes two
    /// separately deserialised but identical predicates compare
    /// <em>unequal</em>. That is the safe direction and is deliberate: an
    /// unequal key declines to coalesce and the read is issued exactly as it is
    /// today. The comparison can never go the other way - equal fields mean
    /// equal predicates - so a false match is not expressible.
    /// </para>
    /// </summary>
    private readonly record struct ScanPageLeafReadKey(
        GrainId LeafId,
        bool Entries,
        string? StartInclusive,
        string? EndExclusive,
        string? AfterExclusive,
        string? BeforeExclusive,
        LatticePredicateNode? Predicate);

    /// <summary>
    /// One leaf read this activation has issued and not yet forgotten, held so
    /// that a later walk asking the identical question can attach to it rather
    /// than enqueue a duplicate behind it.
    /// </summary>
    private sealed class ScanPageLeafReadEntry
    {
        /// <summary>The read itself, joinable for as long as it runs.</summary>
        internal required Task Read { get; init; }

        /// <summary>
        /// The leaf's revision cookie as it stood <b>before</b> this read was
        /// issued, or <see langword="null"/> when the leaf published none.
        /// Issue #2786.
        /// <para>
        /// <b>Sampled before the read, never at its completion, and the order
        /// is load-bearing.</b> A cookie taken before issue and found unchanged
        /// at attach time proves the leaf published no mutation across
        /// <c>[T_issue, T_attach]</c>, a window that strictly contains the
        /// interval in which the leaf actually executed the read. Sampling at
        /// completion instead would take it after a write that landed while the
        /// read was in flight, so the comparison would be against a value that
        /// already reflects the write - it would report "unchanged" and serve
        /// rows that predate it. The post-stamp reads more naturally and is
        /// wrong; see
        /// <c>A_write_landing_while_the_read_was_in_flight_refuses_reuse</c>,
        /// which is the only arm that tells the two designs apart.
        /// </para>
        /// <para>
        /// <see langword="null"/> means "unknown", never "unchanged". The leaf
        /// is activated on another silo, or was not activated anywhere when the
        /// read was issued; either way nothing here can speak for it, and the
        /// reuse gate refuses.
        /// </para>
        /// </summary>
        internal long? RevisionAtIssue { get; init; }
    }

    /// <summary>
    /// The leaf reads this activation currently owns, keyed by identity.
    /// <para>
    /// Per-activation by design, and it carries <b>no cross-activation
    /// obligation</b>: it is an index of reads <em>this</em> activation issued,
    /// so an activation that is lost loses only the ability to attach to reads
    /// that died with it. There is no stale state to carry forward and nothing
    /// to reset, which is why losing it degrades to exactly the behaviour that
    /// shipped before this file existed rather than to something wrong.
    /// </para>
    /// <para>
    /// A plain dictionary under a lock rather than a concurrent one: the map is
    /// read and written from the activation's single-threaded scheduler on the
    /// issue path, but the settle continuation runs on
    /// <see cref="TaskScheduler.Default"/> (it must not need a turn on an
    /// activation this code exists to keep responsive), so the two genuinely
    /// race and the lock is doing real work. It is never held across an await.
    /// </para>
    /// </summary>
    private readonly Dictionary<ScanPageLeafReadKey, ScanPageLeafReadEntry> _scanPageLeafReads = new();

    private readonly object _scanPageLeafReadsGate = new();

    private bool _scanPageLeafReadOutcomesPrimed;

    /// <summary>
    /// How many settled reads this activation will retain for reuse. Issue
    /// #2786. A settled entry no longer removes itself on completion, so the
    /// map needs an explicit bound; each distinct range and predicate is its
    /// own key, so a wide sweep would otherwise retain one entry per page for
    /// the life of the activation. Reuse is an optimisation, so evicting an
    /// eligible entry costs at most one re-read and never correctness.
    /// </summary>
    private const int MaxRetainedSettledScanPageLeafReads = 64;


    /// <summary>
    /// Issues a bounded leaf entry read through the coalescing map, or attaches
    /// to an identical read this activation already owns.
    /// </summary>
    private Task<List<KeyValuePair<string, byte[]>>> ReadLeafEntriesAsync(
        ScanPageWalk scan,
        GrainId leafId,
        IBPlusLeafGrain leafGrain,
        string? startInclusive,
        string? endExclusive,
        string? afterExclusive,
        string? beforeExclusive,
        LatticePredicateNode? predicate)
        => ReadLeafAsync(
            scan,
            new ScanPageLeafReadKey(leafId, Entries: true, startInclusive, endExclusive, afterExclusive, beforeExclusive, predicate),
            () => leafGrain.GetEntriesAsync(startInclusive, endExclusive, afterExclusive, beforeExclusive, predicate),
            static source => new List<KeyValuePair<string, byte[]>>(source));

    /// <summary>
    /// Issues a bounded leaf key read through the coalescing map, or attaches
    /// to an identical read this activation already owns.
    /// </summary>
    private Task<List<string>> ReadLeafKeysAsync(
        ScanPageWalk scan,
        GrainId leafId,
        IBPlusLeafGrain leafGrain,
        string? startInclusive,
        string? endExclusive,
        string? afterExclusive,
        string? beforeExclusive,
        LatticePredicateNode? predicate)
        => ReadLeafAsync(
            scan,
            new ScanPageLeafReadKey(leafId, Entries: false, startInclusive, endExclusive, afterExclusive, beforeExclusive, predicate),
            () => leafGrain.GetKeysAsync(startInclusive, endExclusive, afterExclusive, beforeExclusive, predicate),
            static source => new List<string>(source));

    /// <summary>
    /// The convergence half of issue 2585: makes a scan-page retry differ from
    /// the attempt that preceded it, by attaching to the leaf read that attempt
    /// paid for instead of enqueueing a second copy of it.
    /// <para>
    /// <b>Why a retry loop here does not merely fail to converge, but
    /// diverges.</b> <see cref="Task.WaitAsync(CancellationToken)"/> ends the
    /// <em>wait</em> and never the <em>call</em>, and
    /// <see cref="IBPlusLeafGrain"/> is not reentrant. So the read a ceiling
    /// fire abandons keeps its place in the leaf's queue, and the retry the
    /// fire invites issues an argument-identical read that queues
    /// <em>behind</em> it. Issue 2233 bounded the damage to one orphan per
    /// attempt by standing the walk down instead of letting it walk on, but one
    /// per attempt still accumulates: after N attempts the leaf holds N reads
    /// for the same rows, and attempt N + 1 waits behind all of them. The
    /// sequence of identical-looking stalls in issue 2585 is therefore not a
    /// sequence of identical attempts - each is strictly worse than its
    /// predecessor, and they only look alike because the ceiling truncates them
    /// all at the same number. Every orphan also holds demand against the
    /// process-wide leaf replay gate, so the cost is not confined to this
    /// shard.
    /// </para>
    /// <para>
    /// <b>Attaching is not a staleness compromise; it is serialisably
    /// correct.</b> Because the leaf is non-reentrant, the read being attached
    /// to holds that leaf's turn for its whole duration and every write to the
    /// leaf is ordered strictly before or strictly after it. An attaching
    /// caller therefore observes exactly what the original call observed: a
    /// linearisable read taken at the point the leaf executed it. The staleness
    /// is the staleness any slow call already has, which is why the existing
    /// page-fill contract needs no amendment - a page fill that takes the full
    /// ceiling already returns leaf data read at its start.
    /// </para>
    /// <para>
    /// <b>Why a completed read is not reused on the strength of recency.</b>
    /// The serialisability argument above is specific to a read still in flight
    /// and does not extend past its completion by even a moment: once the
    /// leaf's turn ends, later writes are ordered after the read, so handing
    /// its rows to a new caller on the strength of how recently it finished
    /// returns a scan page that misses committed writes. An earlier revision of
    /// this file retained settled results for one ceiling, reasoning that the
    /// window was short. That was wrong, and wrong in kind rather than in
    /// degree - a scan that misses a committed write is incorrect at any window
    /// length, so there is no duration at which the trade becomes acceptable.
    /// </para>
    /// <para>
    /// <b>What issue #2786 changed, and what it did not.</b> It did not soften
    /// the paragraph above; recency is still not a basis for reuse and never
    /// will be. It supplied the basis that was missing: the leaf's
    /// activation-fenced revision cookie, sampled BEFORE the read is issued and
    /// compared at attach time. Equal cookies mean no mutation was published by
    /// that leaf across a window that strictly contains the read, so the
    /// settled rows are not merely recent, they are provably unchanged. Every
    /// way of failing to establish that - an unstamped entry, a leaf that has
    /// deactivated, a faulted or cancelled read, a cookie that moved - refuses
    /// reuse and issues afresh. See
    /// <see cref="CanReuseSettledScanPageLeafRead"/>.
    /// </para>
    /// <para>
    /// <b>What that costs, and why it is still sufficient.</b> Coalescing alone
    /// fixes divergence outright: queue depth stops growing, so an attempt is
    /// no longer strictly worse than the one before it. It converges because
    /// the read stays in flight and successive retries keep attaching to the
    /// same one, so elapsed read time accumulates across attempts instead of
    /// resetting - whichever retry is attached when the read completes carries
    /// the rows, the continuation token advances, and the walk proceeds. The
    /// residual case is a retry that arrives after a read completed against a
    /// leaf that has since been written: it pays for a fresh read. That is
    /// slower, and it is correct, which is the right way round.
    /// </para>
    /// <para>
    /// <b>Confined to stall-guarded walks.</b> An unguarded walk cannot strand
    /// a read - nothing abandons it - so it has no duplicate to suppress, and
    /// it takes the unmodified path. That keeps every behaviour added here on
    /// exactly the path whose defect it fixes.
    /// </para>
    /// </summary>
    private async Task<TList> ReadLeafAsync<TList>(
        ScanPageWalk scan,
        ScanPageLeafReadKey key,
        Func<Task<TList>> issue,
        Func<TList, TList> copy)
    {
        // Primed above every early return, not below one (issue #2809). The
        // activation hook is the primary site and makes the series exist
        // independent of workload; this call is what keeps the invariant "no
        // leaf-read outcome is ever recorded on an unprimed series" true on its
        // own, for an activation that reached this path without running the
        // hook. It is a latched bool read on every subsequent call.
        PrimeScanPageLeafReadOutcomes();

        if (!scan.IsStallGuarded)
        {
            return await issue().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        }

        if (TryAttachScanPageLeafRead<TList>(key, out var attached, out var servedFromSettled))
        {
            RecordScanPageLeafReadOutcome(
                1,
                servedFromSettled
                    ? LatticeMetrics.OutcomeScanPageLeafReadServedTag
                    : LatticeMetrics.OutcomeScanPageLeafReadJoinedTag);

            // Copied, never shared: the issuing walk owns the instance the leaf
            // returned and appends nothing to it, but handing the same list to
            // two walks would let one observe the other's enumeration. The copy
            // is shallow, which is the same depth the walk's own accumulation
            // already takes.
            return copy(await attached.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext));
        }

        // Pre-stamp. The cookie is sampled BEFORE the read is issued, never
        // after it completes - see ScanPageLeafReadEntry.RevisionAtIssue for
        // why that order is load-bearing rather than incidental.
        var revisionAtIssue = BPlusLeafGrain.TryGetLeafRevision(key.LeafId, out var issuedRevision)
            ? issuedRevision
            : (long?)null;

        var read = issue();
        var entry = new ScanPageLeafReadEntry { Read = read, RevisionAtIssue = revisionAtIssue };
        RegisterScanPageLeafRead(key, entry);
        RecordScanPageLeafReadOutcome(1, LatticeMetrics.OutcomeScanPageLeafReadIssuedTag);

        _ = read.ContinueWith(
            _ => SettleScanPageLeafRead(key, entry),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return await read.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
    }

    /// <summary>
    /// Finds a read for <paramref name="key"/> that is <em>still in flight</em>,
    /// evicting anything it may not hand on.
    /// <para>
    /// <b>An in-flight read may always be joined, and that is a serialisability
    /// argument rather than a heuristic.</b> Because the leaf is non-reentrant
    /// the read holds that leaf's turn for its whole duration, so every write
    /// is ordered strictly before or strictly after it and a joiner observes
    /// exactly what the original caller would have.
    /// </para>
    /// <para>
    /// <b>A settled read carries no such guarantee from its own completion, and
    /// recency never supplies one.</b> Writes may have committed between its
    /// execution and this lookup, so serving its rows on the strength of how
    /// recently it finished would be a stale read, not a cheap one. An earlier
    /// revision of this file retained settled results for one ceiling on the
    /// argument that the window was short; the window's length is irrelevant,
    /// because a scan that misses a committed write is wrong at any duration.
    /// Issue #2786 did not relax that. It added an independent basis - the
    /// leaf's own revision cookie, sampled before the read was issued - so that
    /// a settled read is served only when the leaf has published no mutation
    /// across a window strictly containing it. See
    /// <see cref="CanReuseSettledScanPageLeafRead"/>, every clause of which
    /// fails toward refusing.
    /// </para>
    /// <para>
    /// Completion is read from the task itself rather than from a flag set by a
    /// continuation. The continuation runs on <see cref="TaskScheduler.Default"/>
    /// and can still be pending when a retry looks up, so a flag would report
    /// "in flight" for a read that had already finished - which would bypass
    /// the cookie check entirely and reinstate exactly the stale read this
    /// guard exists to refuse.
    /// </para>
    /// </summary>
    private bool TryAttachScanPageLeafRead<TList>(
        ScanPageLeafReadKey key,
        out Task<TList> attached,
        out bool servedFromSettled)
    {
        servedFromSettled = false;

        lock (_scanPageLeafReadsGate)
        {
            if (!_scanPageLeafReads.TryGetValue(key, out var entry))
            {
                attached = default!;
                return false;
            }

            // Completed in any manner is handed on only with a basis for
            // believing the leaf has not moved on. Issue #2786 supplies that
            // basis; where it cannot be established this degrades to the
            // unconditional refusal that shipped before it - serving a settled
            // success would be a stale read, and attaching to a settled failure
            // would convert a single failure into a permanent one.
            if (entry.Read.IsCompleted)
            {
                if (CanReuseSettledScanPageLeafRead(key, entry))
                {
                    servedFromSettled = true;
                    attached = (Task<TList>)entry.Read;
                    return true;
                }

                _scanPageLeafReads.Remove(key);
                attached = default!;
                return false;
            }

            attached = (Task<TList>)entry.Read;
            return true;
        }
    }

    /// <summary>
    /// Whether a settled read may be served again: the leaf must still publish
    /// the same revision cookie it published before the read was issued.
    /// <para>
    /// Every clause fails toward refusing reuse, which degrades to the
    /// behaviour that shipped before issue #2786 rather than to a stale read. A
    /// faulted or cancelled read is refused so a single failure is not
    /// converted into a permanent one. An unstamped entry is refused because a
    /// missing cookie means "unknown", not "unchanged". An unreadable cookie is
    /// refused for the same reason - the leaf has deactivated or never lived on
    /// this silo, and either way nothing here can speak for it.
    /// </para>
    /// <para>
    /// The success clause is not redundant with
    /// <see cref="SettleScanPageLeafRead"/> dropping failures, and the reason
    /// is the same one that makes this method read the task's own state: that
    /// continuation runs on <see cref="TaskScheduler.Default"/> and is not
    /// synchronously ordered with a reader, so a faulted entry can still be in
    /// the map when this runs. This is the authoritative refusal; the
    /// continuation is housekeeping.
    /// </para>
    /// <para>
    /// <b>That clause looks dead from the outside and is not - do not delete
    /// it.</b> No arm driven through the grain's own timing reddens when it is
    /// removed, because the sweep wins the race in practice every time. The
    /// unreachability is a property of scheduling, not of this method, so it is
    /// pinned by an arm that reaches the state at the map's own surface
    /// instead:
    /// <c>A_settled_entry_that_faulted_before_the_sweep_reached_it_is_still_refused</c>
    /// establishes a genuinely reusable entry, proves it is being served, then
    /// faults it in place. Remove the clause and that arm rethrows the planted
    /// fault to the caller, which is exactly what a real reader would receive.
    /// </para>
    /// <para>
    /// <b>An unchanged cookie is necessary and not sufficient, because a range
    /// read is not a pure function of mutation state.</b> It filters rows
    /// against the wall clock sampled at read time, so a row whose TTL elapses
    /// leaves the answer with nothing written and the cookie unchanged. The
    /// cookie proves no writer ran; it proves nothing about the clock. The
    /// expiry horizon supplies the other half: it is the earliest instant at
    /// which the surfaced rows could begin to answer differently, so requiring
    /// <c>now</c> to be strictly below it closes the row-TTL half that the
    /// cookie cannot see. An absent horizon is "unknown" and refuses, exactly
    /// as an absent cookie does. See
    /// <c>BPlusLeafGrain.PublishLeafExpiryHorizon</c>, and the arm
    /// <c>A_settled_read_is_refused_once_a_surfaced_row_has_expired</c>.
    /// </para>
    /// <para>
    /// <b>The pair is not exhaustive, and the residue is stated rather than
    /// implied.</b> A leaf holding uncommitted transactional writes resolves
    /// each one against the transaction registry while answering, and a
    /// decision outcome can flip as its tombstone expires by the clock: no
    /// writer runs, so the cookie holds, and that row was never surfaced, so it
    /// contributes nothing to the horizon. What bounds it is that the read path
    /// consults the registry at all only when <c>_pendingTx</c> is non-empty,
    /// and returns early otherwise, so the residue is confined to leaves with
    /// an in-flight transaction rather than being general. Closing it needs the
    /// decision horizon folded in at the same seam; this change does not
    /// attempt it. Claiming exhaustiveness here would be the same error this
    /// clause exists to correct, one axis further out.
    /// </para>
    /// <para>
    /// Equality, never ordering. The cookie's published contract is that it may
    /// be compared for equality only: equal means nothing has advanced, and any
    /// other outcome forces a refresh. It is not a version whose gaps carry
    /// meaning, and an activation is fenced so that a reactivated leaf cannot
    /// return to a value a previous activation published (issue #2151), which
    /// is what makes equality here safe from an ABA.
    /// </para>
    /// </summary>
    private static bool CanReuseSettledScanPageLeafRead(
        ScanPageLeafReadKey key,
        ScanPageLeafReadEntry entry)
        => entry.Read.IsCompletedSuccessfully
           && entry.RevisionAtIssue is { } issued
           && BPlusLeafGrain.TryGetLeafRevision(key.LeafId, out var current)
           && current == issued
           && BPlusLeafGrain.TryGetLeafExpiryHorizon(key.LeafId, out var horizon)
           && DateTimeOffset.UtcNow.Ticks < horizon;

    private void RegisterScanPageLeafRead(ScanPageLeafReadKey key, ScanPageLeafReadEntry entry)
    {
        lock (_scanPageLeafReadsGate)
        {
            PruneScanPageLeafReads();
            _scanPageLeafReads[key] = entry;
        }
    }

    /// <summary>
    /// Resolves a read's entry once it completes: a faulted or cancelled read
    /// is dropped, a successful one is retained for possible reuse. See
    /// <see cref="TryAttachScanPageLeafRead{TList}"/> for what retention does
    /// and does not permit - retention is not on its own a licence to serve.
    /// <para>
    /// This continuation is a housekeeping optimisation, not a correctness
    /// guard, and the distinction matters: it runs on
    /// <see cref="TaskScheduler.Default"/> and can still be pending when a
    /// retry looks the entry up, so it is not synchronously ordered with a
    /// reader and nothing may depend on it having run. The authoritative
    /// decision is the completion and cookie check in
    /// <see cref="TryAttachScanPageLeafRead{TList}"/>, which reads the task's
    /// own state and the leaf's own cookie, and therefore cannot be raced.
    /// </para>
    /// </summary>
    private void SettleScanPageLeafRead(ScanPageLeafReadKey key, ScanPageLeafReadEntry entry)
    {
        lock (_scanPageLeafReadsGate)
        {
            // Only ever act on the entry this continuation was created for: a
            // later read for the same key may already have replaced it.
            if (!_scanPageLeafReads.TryGetValue(key, out var current) || !ReferenceEquals(current, entry))
            {
                return;
            }

            // Issue #2786. A read that ran to completion is RETAINED rather
            // than dropped: its rows remain servable to a later identical walk
            // for as long as the leaf publishes the revision cookie it
            // published before the read was issued. Dropping it here would
            // leave nothing to reuse, and this file would behave exactly as it
            // did before, which is the state issue #2786 exists to change.
            //
            // A faulted or cancelled read is still dropped immediately. It
            // carries no rows to serve, and retaining it would convert a single
            // failure into a permanent one for every later walk asking the same
            // question.
            if (entry.Read.IsCompletedSuccessfully)
            {
                return;
            }

            _scanPageLeafReads.Remove(key);
        }
    }

    /// <summary>
    /// Drops every entry that can no longer serve anybody, and caps the number
    /// of settled entries retained for reuse.
    /// <para>
    /// In-flight entries are never dropped: evicting one would let the next
    /// attempt enqueue the duplicate this map exists to suppress.
    /// </para>
    /// <para>
    /// Issue #2786 changed what "no longer useful" means here. Before it, every
    /// completed entry was useless by definition and was swept. Now a completed
    /// entry whose leaf still publishes its issue-time revision cookie is
    /// servable, so the sweep drops only entries that are faulted, cancelled,
    /// or whose leaf has moved on - and a cap becomes necessary, because
    /// entries no longer drain on completion.
    /// </para>
    /// </summary>
    private void PruneScanPageLeafReads()
    {
        List<ScanPageLeafReadKey>? unusable = null;
        var retainedSettled = 0;

        foreach (var (key, entry) in _scanPageLeafReads)
        {
            if (!entry.Read.IsCompleted)
            {
                continue;
            }

            if (CanReuseSettledScanPageLeafRead(key, entry))
            {
                retainedSettled++;
                continue;
            }

            (unusable ??= []).Add(key);
        }

        if (unusable is not null)
        {
            foreach (var key in unusable)
            {
                _scanPageLeafReads.Remove(key);
            }
        }

        // Cap the retained-for-reuse population. Each distinct range and
        // predicate is its own key, so without a cap a walk sweeping a wide
        // keyspace would retain one settled entry per page for the life of the
        // activation. Only settled entries are eligible for this eviction; an
        // in-flight entry is never touched, for the reason below.
        var excess = retainedSettled - MaxRetainedSettledScanPageLeafReads;
        if (excess > 0)
        {
            List<ScanPageLeafReadKey>? evict = null;
            foreach (var (key, entry) in _scanPageLeafReads)
            {
                if (excess <= 0)
                {
                    break;
                }

                if (entry.Read.IsCompleted)
                {
                    (evict ??= []).Add(key);
                    excess--;
                }
            }

            if (evict is not null)
            {
                foreach (var key in evict)
                {
                    _scanPageLeafReads.Remove(key);
                }
            }
        }

        // Deliberately no eviction of in-flight entries, at any population.
        // Evicting one would let the next attempt enqueue behind it the
        // duplicate read this map exists to suppress - trading a bounded map
        // for the unbounded queue growth that is the defect. The in-flight
        // population is bounded in practice by the number of leaf reads a
        // single activation can have concurrently outstanding, each of which
        // resolves itself on completion.
    }

    /// <summary>
    /// Removes a read from the map regardless of its state. Used by the tests
    /// that assert the map's failure arms, and by nothing on the hot path.
    /// </summary>
    private void ForgetScanPageLeafRead(ScanPageLeafReadKey key)
    {
        lock (_scanPageLeafReadsGate)
        {
            _scanPageLeafReads.Remove(key);
        }
    }

    /// <summary>
    /// Drops every read this activation holds for one leaf, in flight or not,
    /// and reports how many were dropped. The stranded-leaf recovery
    /// (issue #3016) is its only caller.
    /// <para>
    /// <b>This is the one place an in-flight entry may be evicted, and the
    /// comment in <see cref="PruneScanPageLeafReads"/> forbidding it is not
    /// being overridden - its premise has failed.</b> That rule protects the
    /// case where the read will complete: evicting one there lets the next
    /// attempt enqueue a duplicate behind it, trading a bounded map for the
    /// unbounded queue growth coalescing exists to suppress. The recovery path
    /// is reached only after the same leaf has missed the ceiling on
    /// <see cref="StrandedLeafStallThreshold"/> consecutive attempts having
    /// read nothing on any of them, which is the evidence that this particular
    /// read is not going to complete. Once that holds, retaining the entry does
    /// not suppress a duplicate - there is no useful read to be duplicated -
    /// it guarantees that no attempt ever reaches the leaf again.
    /// </para>
    /// <para>
    /// Nothing is cancelled. The parked call keeps its place in the leaf's
    /// queue; dropping the entry only means no future caller will wait on it.
    /// </para>
    /// </summary>
    private int EvictScanPageLeafReads(GrainId leafId)
    {
        lock (_scanPageLeafReadsGate)
        {
            List<ScanPageLeafReadKey>? evict = null;
            foreach (var key in _scanPageLeafReads.Keys)
            {
                if (key.LeafId == leafId)
                {
                    (evict ??= []).Add(key);
                }
            }

            if (evict is null)
            {
                return 0;
            }

            foreach (var key in evict)
            {
                _scanPageLeafReads.Remove(key);
            }

            return evict.Count;
        }
    }

    /// <summary>
    /// The single recorder every leaf-read outcome goes through, including the
    /// zero prime, so that the primed series and the live series carry the same
    /// tag set by construction rather than by inspection.
    /// </summary>
    private void RecordScanPageLeafReadOutcome(long delta, KeyValuePair<string, object?> outcome) =>
        LatticeMetrics.ScanPageLeafReadOutcomes.Add(
            delta,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, MyShardIndex),
            outcome,
            LatticeTenantLabel.ForTree(TreeId));

    /// <summary>
    /// Publishes all three outcome arms at zero on first use, so that a zero is
    /// a measured absence rather than an absent measurement. Without it,
    /// <c>joined</c> and <c>served</c> are unreadable in exactly the case that
    /// matters most - a deployment where coalescing never fires looks identical
    /// to one where the counter was never wired.
    /// <para>
    /// <b>Every call site must sit above every early return on its path, and
    /// the primary one is the activation hook</b> (issue #2809). This is not
    /// style. The property the prime exists to buy is that an ABSENT series
    /// means one thing - the build does not carry the instrument - and a call
    /// site below a condition predicate destroys it, because absence then also
    /// means "the build carries it and the predicate was false". This counter
    /// shipped that way: the prime sat below
    /// <c>if (!scan.IsStallGuarded) return</c>, a predicate that is false for a
    /// whole process lifetime whenever the silo's response timeout is infinite,
    /// so the prime could simply never run and its absence proved nothing.
    /// </para>
    /// <para>
    /// <b>Hoisting inside the read path would not have been enough, and the
    /// distinction is the whole fix.</b> A prime at the top of
    /// <see cref="ReadLeafAsync"/> is still workload-gated: it needs some
    /// scan-page leaf read to have happened. The invariant that makes absence
    /// evidence is stronger than "above the return" - the prime must be
    /// reachable on a path that ANY deployment carrying the build executes,
    /// independent of workload. Only a lifecycle site satisfies that, so the
    /// primary call is the first statement of
    /// <c>ShardRootGrain.OnActivateAsync</c> and the series exists for every
    /// <c>(tree, shard)</c> that has activated, with no scan traffic at all.
    /// </para>
    /// <para>
    /// <b>Both sites are kept deliberately.</b> The activation call is what the
    /// readability claim rests on; the read-path call is what keeps "no outcome
    /// is ever recorded on an unprimed series" true of the read path in
    /// isolation, so the two are not redundant and neither is a fallback for
    /// the other. They are pinned by separate named arms in
    /// <c>ShardRootGrainScanPageLeafReadCoalescingTests.Priming.cs</c> so that
    /// reverting either one reddens exactly one test and names which site was
    /// lost.
    /// </para>
    /// <para>
    /// <b>Priming is correct here because this is a counter, and it would be
    /// wrong on a histogram.</b> A zero added to a counter is the identity, so
    /// it creates the series and changes no reading of it. A zero recorded on a
    /// <c>Histogram&lt;T&gt;</c> is a fabricated sample asserting the operation
    /// took no time, which is one of the answers such an instrument exists to
    /// choose between - so priming one destroys the measurement it was added to
    /// take. Do not carry this pattern across to a histogram.
    /// </para>
    /// </summary>
    private void PrimeScanPageLeafReadOutcomes()
    {
        if (_scanPageLeafReadOutcomesPrimed)
        {
            return;
        }

        _scanPageLeafReadOutcomesPrimed = true;
        RecordScanPageLeafReadOutcome(0, LatticeMetrics.OutcomeScanPageLeafReadIssuedTag);
        RecordScanPageLeafReadOutcome(0, LatticeMetrics.OutcomeScanPageLeafReadJoinedTag);
        RecordScanPageLeafReadOutcome(0, LatticeMetrics.OutcomeScanPageLeafReadServedTag);
    }
}
