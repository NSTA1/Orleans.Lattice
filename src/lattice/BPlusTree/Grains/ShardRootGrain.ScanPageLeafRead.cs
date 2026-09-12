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
    /// <b>Why a completed read is never reused, however recently it
    /// completed.</b> The serialisability argument above is specific to a read
    /// still in flight and does not extend past its completion by even a
    /// moment: once the leaf's turn ends, later writes are ordered after the
    /// read, so handing its rows to a new caller returns a scan page that
    /// misses committed writes. An earlier revision of this file retained
    /// settled results for one ceiling, reasoning that the window was short.
    /// That was wrong, and wrong in kind rather than in degree - a scan that
    /// misses a committed write is incorrect at any window length, so there is
    /// no duration at which the trade becomes acceptable.
    /// </para>
    /// <para>
    /// <b>What that costs, and why it is still sufficient.</b> Coalescing alone
    /// fixes divergence outright: queue depth stops growing, so an attempt is
    /// no longer strictly worse than the one before it. It converges because
    /// the read stays in flight and successive retries keep attaching to the
    /// same one, so elapsed read time accumulates across attempts instead of
    /// resetting - whichever retry is attached when the read completes carries
    /// the rows, the continuation token advances, and the walk proceeds. The
    /// residual case is a retry that arrives after a read completed and before
    /// the next one is issued: it pays for a fresh read. That is slower, and it
    /// is correct, which is the right way round.
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
        if (!scan.IsStallGuarded)
        {
            return await issue().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        }

        PrimeScanPageLeafReadOutcomes();

        if (TryAttachScanPageLeafRead<TList>(key, out var attached))
        {
            RecordScanPageLeafReadOutcome(1, LatticeMetrics.OutcomeScanPageLeafReadJoinedTag);

            // Copied, never shared: the issuing walk owns the instance the leaf
            // returned and appends nothing to it, but handing the same list to
            // two walks would let one observe the other's enumeration. The copy
            // is shallow, which is the same depth the walk's own accumulation
            // already takes.
            return copy(await attached.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext));
        }

        var read = issue();
        var entry = new ScanPageLeafReadEntry { Read = read };
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
    /// evicting any that has completed rather than returning it.
    /// <para>
    /// <b>Only an in-flight read may be joined, and that restriction is a
    /// correctness requirement rather than a conservatism.</b> Joining an
    /// in-flight read is serializable: because the leaf is non-reentrant the
    /// read holds that leaf's turn for its whole duration, so every write is
    /// ordered strictly before or strictly after it and a joiner observes
    /// exactly what the original caller would have. A read that has already
    /// completed carries no such guarantee - writes may have committed between
    /// its execution and this lookup, so serving its rows would be a stale
    /// read, not a cheap one. An earlier revision of this file retained settled
    /// results for one ceiling on the argument that the window was short; the
    /// window's length is irrelevant, because a scan that misses a committed
    /// write is wrong at any duration.
    /// </para>
    /// <para>
    /// Completion is read from the task itself rather than from a flag set by a
    /// continuation. The continuation runs on <see cref="TaskScheduler.Default"/>
    /// and can still be pending when a retry looks up, so a flag would report
    /// "in flight" for a read that had already finished - which is precisely
    /// the stale-read case this guard exists to refuse.
    /// </para>
    /// </summary>
    private bool TryAttachScanPageLeafRead<TList>(
        ScanPageLeafReadKey key,
        out Task<TList> attached)
    {
        lock (_scanPageLeafReadsGate)
        {
            if (!_scanPageLeafReads.TryGetValue(key, out var entry))
            {
                attached = default!;
                return false;
            }

            // Completed in any manner - ran to completion, faulted, or
            // cancelled - is never handed on. Serving a completed success would
            // be a stale read; attaching to a completed failure would convert a
            // single failure into a permanent one.
            if (entry.Read.IsCompleted)
            {
                _scanPageLeafReads.Remove(key);
                attached = default!;
                return false;
            }

            attached = (Task<TList>)entry.Read;
            return true;
        }
    }

    private void RegisterScanPageLeafRead(ScanPageLeafReadKey key, ScanPageLeafReadEntry entry)
    {
        lock (_scanPageLeafReadsGate)
        {
            PruneScanPageLeafReads();
            _scanPageLeafReads[key] = entry;
        }
    }

    /// <summary>
    /// Drops a read from the map once it completes, in any manner. Nothing is
    /// retained past completion, because only an in-flight read may be joined -
    /// see <see cref="TryAttachScanPageLeafRead{TList}"/> for why.
    /// <para>
    /// This continuation is a housekeeping optimisation, not a correctness
    /// guard, and the distinction matters: it runs on
    /// <see cref="TaskScheduler.Default"/> and can still be pending when a
    /// retry looks the entry up, so it is not synchronously ordered with a
    /// reader and nothing may depend on it having run. The authoritative
    /// refusal is the completion check in
    /// <see cref="TryAttachScanPageLeafRead{TList}"/>, which reads the task's
    /// own state and therefore cannot be raced.
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

            _scanPageLeafReads.Remove(key);
        }
    }

    /// <summary>
    /// Drops every entry whose read has completed. In-flight entries are never
    /// dropped: evicting one would let the next attempt enqueue the duplicate
    /// this map exists to suppress.
    /// </summary>
    private void PruneScanPageLeafReads()
    {
        List<ScanPageLeafReadKey>? completed = null;
        foreach (var (key, entry) in _scanPageLeafReads)
        {
            if (entry.Read.IsCompleted)
            {
                (completed ??= []).Add(key);
            }
        }

        if (completed is not null)
        {
            foreach (var key in completed)
            {
                _scanPageLeafReads.Remove(key);
            }
        }

        // Deliberately no eviction past this point. Every entry that survives
        // the sweep above is still in flight, and evicting one of those would
        // let the next attempt enqueue behind it the duplicate read this map
        // exists to suppress - trading a bounded map for the unbounded queue
        // growth that is the defect. The map is bounded in practice by the
        // number of leaf reads a single activation can have concurrently in
        // flight, each of which removes itself on completion.
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
    }
}
