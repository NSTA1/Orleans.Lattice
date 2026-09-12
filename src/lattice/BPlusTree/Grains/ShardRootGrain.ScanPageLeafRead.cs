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
        /// <summary>The read itself, in flight or settled.</summary>
        internal required Task Read { get; init; }

        /// <summary>
        /// When the read settled, or <see langword="null"/> while it is still
        /// in flight. Drives retention only; an in-flight read is joinable for
        /// as long as it runs.
        /// </summary>
        internal long? SettledTimestamp;
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
    /// Ceiling on retained leaf reads. A scan page walks a bounded chain and
    /// settled entries are evicted on the first access past their retention, so
    /// this is a backstop against an activation that issues many distinct reads
    /// without ever coming back for them, not a working limit.
    /// </summary>
    private const int MaxRetainedScanPageLeafReads = 64;

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
    /// <b>Retention is what makes it converge, and coalescing alone does
    /// not.</b> Coalescing fixes divergence: queue depth stops growing. It does
    /// not fix convergence, because the retry need not land inside the read's
    /// flight window. A read issued at t=0 that needs 40s is abandoned at the
    /// 25s ceiling, completes at t=40 having done all the work, and hands its
    /// rows to a caller that left 15s earlier; a retry arriving at t=60 finds
    /// nothing to attach to and starts a fresh 40s read that will also be cut
    /// at 25s. Every attempt resets elapsed read time to zero and the walk
    /// never advances. Retaining the settled result for one ceiling closes
    /// that: the first retry after completion is answered from the map, the
    /// continuation token advances, and it does so in bounded time regardless
    /// of the caller's backoff, which this shard does not control.
    /// </para>
    /// <para>
    /// <b>Confined to stall-guarded walks.</b> An unguarded walk cannot strand
    /// a read - nothing abandons it - so it has neither a duplicate to suppress
    /// nor a result to retain, and it takes the unmodified path. That keeps
    /// every behaviour added here on exactly the path whose defect it fixes.
    /// </para>
    /// <para>
    /// <b>A settled entry is evicted, never left to be attached to.</b> Both
    /// arms of <see cref="ForgetScanPageLeafRead"/> matter. A faulted read left
    /// in the map would be handed to every later caller forever, turning one
    /// failure into a permanent one with the same signature as the livelock
    /// this fixes; a successful one left past its retention would answer a
    /// caller with rows older than the contract allows.
    /// </para>
    /// </summary>
    private async Task<TList> ReadLeafAsync<TList>(
        ScanPageWalk scan,
        ScanPageLeafReadKey key,
        Func<Task<TList>> issue,
        Func<TList, TList> copy)
    {
        if (!scan.IsStallGuarded)
        {
            return await issue().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        }

        PrimeScanPageLeafReadOutcomes();

        var retention = scan.StallDuration;
        if (TryAttachScanPageLeafRead<TList>(key, retention, out var attached, out var settled))
        {
            RecordScanPageLeafReadOutcome(
                1,
                settled
                    ? LatticeMetrics.OutcomeScanPageLeafReadServedTag
                    : LatticeMetrics.OutcomeScanPageLeafReadJoinedTag);

            // Copied, never shared: the issuing walk owns the instance the leaf
            // returned and appends nothing to it, but handing the same list to
            // two walks would let one observe the other's enumeration. The copy
            // is shallow, which is the same depth the walk's own accumulation
            // already takes.
            return copy(await attached.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext));
        }

        var read = issue();
        var entry = new ScanPageLeafReadEntry { Read = read };
        RegisterScanPageLeafRead(key, entry, retention);
        RecordScanPageLeafReadOutcome(1, LatticeMetrics.OutcomeScanPageLeafReadIssuedTag);

        _ = read.ContinueWith(
            _ => SettleScanPageLeafRead(key, entry),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return await read.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
    }

    /// <summary>
    /// Finds a live read for <paramref name="key"/>, evicting one that has
    /// faulted or outlived <paramref name="retention"/> rather than returning
    /// it. <paramref name="settled"/> distinguishes a retained result from a
    /// read still in flight, so the two are measured separately.
    /// </summary>
    private bool TryAttachScanPageLeafRead<TList>(
        ScanPageLeafReadKey key,
        TimeSpan retention,
        out Task<TList> attached,
        out bool settled)
    {
        lock (_scanPageLeafReadsGate)
        {
            if (!_scanPageLeafReads.TryGetValue(key, out var entry))
            {
                attached = default!;
                settled = false;
                return false;
            }

            // A faulted or cancelled read is never handed on. Attaching to one
            // would convert a single failure into a permanent one.
            if (entry.Read.IsFaulted || entry.Read.IsCanceled)
            {
                _scanPageLeafReads.Remove(key);
                attached = default!;
                settled = false;
                return false;
            }

            if (entry.SettledTimestamp is { } at && Stopwatch.GetElapsedTime(at) > retention)
            {
                _scanPageLeafReads.Remove(key);
                attached = default!;
                settled = false;
                return false;
            }

            attached = (Task<TList>)entry.Read;
            settled = entry.SettledTimestamp is not null;
            return true;
        }
    }

    private void RegisterScanPageLeafRead(ScanPageLeafReadKey key, ScanPageLeafReadEntry entry, TimeSpan retention)
    {
        lock (_scanPageLeafReadsGate)
        {
            PruneScanPageLeafReads(retention);
            _scanPageLeafReads[key] = entry;
        }
    }

    /// <summary>
    /// Records that a read has settled, so retention runs from completion
    /// rather than from issue.
    /// <para>
    /// Only success is recorded here. Evicting a failed read is deliberately
    /// <em>not</em> done in this continuation, and the reason is testability as
    /// much as correctness: an eviction here and the guard in
    /// <see cref="TryAttachScanPageLeafRead{TList}"/> would mask each other, so
    /// neither could be shown to be load-bearing and one of them would be dead
    /// code nobody could prove was dead. The guard at the point of use is the
    /// one kept, because it is the only one synchronously ordered with a
    /// reader - this continuation runs on <see cref="TaskScheduler.Default"/>
    /// and can therefore still be pending when a retry looks the entry up.
    /// <see cref="PruneScanPageLeafReads"/> bounds anything never looked up
    /// again.
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

            if (entry.Read.IsCompletedSuccessfully)
            {
                entry.SettledTimestamp = Stopwatch.GetTimestamp();
            }
        }
    }

    /// <summary>
    /// Drops settled entries that have outlived their retention, and then, if
    /// the map is still at its ceiling, the oldest settled entry. In-flight
    /// entries are never dropped: evicting one would let the next attempt
    /// enqueue the duplicate this map exists to suppress.
    /// </summary>
    private void PruneScanPageLeafReads(TimeSpan retention)
    {
        List<ScanPageLeafReadKey>? expired = null;
        foreach (var (key, entry) in _scanPageLeafReads)
        {
            if (entry.Read.IsFaulted || entry.Read.IsCanceled)
            {
                (expired ??= []).Add(key);
                continue;
            }

            if (entry.SettledTimestamp is { } at && Stopwatch.GetElapsedTime(at) > retention)
            {
                (expired ??= []).Add(key);
            }
        }

        if (expired is not null)
        {
            foreach (var key in expired)
            {
                _scanPageLeafReads.Remove(key);
            }
        }

        if (_scanPageLeafReads.Count < MaxRetainedScanPageLeafReads)
        {
            return;
        }

        ScanPageLeafReadKey? oldest = null;
        long oldestAt = long.MaxValue;
        foreach (var (key, entry) in _scanPageLeafReads)
        {
            if (entry.SettledTimestamp is { } at && at < oldestAt)
            {
                oldest = key;
                oldestAt = at;
            }
        }

        if (oldest is { } evict)
        {
            _scanPageLeafReads.Remove(evict);
        }
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
