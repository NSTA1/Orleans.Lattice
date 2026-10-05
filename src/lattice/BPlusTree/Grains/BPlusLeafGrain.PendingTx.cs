using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-leaf in-memory pending-transaction map for the saga
/// reader-isolation primitive. Prepared mutations route here instead of
/// the visible projection until the saga's terminal mark
/// (<see cref="MutationKind.TxCommit"/> or
/// <see cref="MutationKind.TxAbort"/>) flips or drops them.
/// <para>
/// Strictly in-memory: under the WAL-as-sole-commit-point model the WAL
/// is the durable record, and the pending-tx map is rebuilt
/// deterministically on activation from the WAL replay. Reads filter
/// pending entries via a local hash lookup with zero RPC cost.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Cached empty outcome map returned by
    /// <see cref="SnapshotPendingForReadAsync"/> on the steady-state
    /// path where the leaf has never participated in a saga since
    /// activation. The vast majority of read fan-outs hit this path;
    /// sharing a single empty instance avoids one zero-content
    /// dictionary allocation per leaf per scan. Callers only ever do
    /// <c>TryGetValue</c> against the returned map - never mutate it -
    /// so it is safe to share the instance across calls and across
    /// leaves.
    /// </summary>
    private static readonly Dictionary<Guid, TxStatus> EmptyOutcomes = new();

    /// <summary>
    /// Cached empty pending-key map returned by
    /// <see cref="SnapshotPendingForReadAsync"/> on the steady-state
    /// path. Same rationale and safety contract as
    /// <see cref="EmptyOutcomes"/>.
    /// </summary>
    private static readonly Dictionary<string, (Guid txid, LwwValue<byte[]> value)> EmptyPendingKeys = new();

    /// <summary>
    /// Caps the capacity hint applied to the per-read pending-key map built
    /// by <see cref="SnapshotPendingForReadAsync"/>. The summed pending-tx
    /// bucket widths are an exact upper bound on the union, but the clamp
    /// keeps a pathologically wide pending set from over-allocating a map
    /// that is rebuilt on every scan-path read.
    /// </summary>
    private const int PendingReadKeyCapacityLimit = 4096;

    /// <summary>
    /// Keyed by <see cref="LatticeMutation.TransactionId"/> -&gt; key
    /// -&gt; the prepared <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/>. Entries here are
    /// invisible to readers until a matching terminal mark surfaces; on
    /// <see cref="MutationKind.TxCommit"/> every value is merged into
    /// the per-activation runtime entry cache via
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/>; on
    /// <see cref="MutationKind.TxAbort"/> every value is dropped.
    /// <para>
    /// Lazily allocated on the first prepared-mutation apply. The vast
    /// majority of leaves never participate in a saga, so an upfront
    /// allocation per activation would be pure waste - leaf activation
    /// density is the dominant memory-cost knob and the dict's empty
    /// footprint (~80 B) multiplied across thousands of activations is
    /// not free.
    /// </para>
    /// </summary>
    private Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>>? _pendingTx;

    /// <summary>
    /// Parallel side-map to <see cref="_pendingTx"/> recording, per
    /// <c>(transactionId, key)</c>, the typed CRDT delta and merge mode a
    /// prepared mutation carried (when it carried one - the common
    /// last-writer-wins prepared write leaves no entry here). On the saga's
    /// terminal <see cref="MutationKind.TxCommit"/> the drain folds the
    /// recorded delta into the leaf's current visible state via the matching
    /// primitive's <c>MergeDelta</c> instead of installing the prepared LWW
    /// value verbatim, so two clusters that write the same CRDT key through
    /// concurrent staged atomic writes converge by the per-replica typed-delta
    /// union rather than last-writer-wins of their merged states.
    /// <para>
    /// Only populated for prepared mutations whose
    /// <see cref="LatticeMergeMode"/> is a CRDT mode (not
    /// <see cref="LatticeMergeMode.LwwRegister"/>) and that carry a non-null
    /// delta payload; value-only sagas and single-writer LWW atomic writes
    /// never touch it, so their terminal drain is byte-for-byte unchanged.
    /// Strictly in-memory and rebuilt deterministically from WAL replay
    /// exactly like <see cref="_pendingTx"/> (the prepared WAL record carries
    /// both <see cref="WalRecord.Delta"/> and <see cref="WalRecord.Mode"/>).
    /// Lazily allocated on the first prepared CRDT-mode apply for the same
    /// activation-density rationale as <see cref="_pendingTx"/>.
    /// </para>
    /// </summary>
    private Dictionary<Guid, Dictionary<string, (byte[] Delta, LatticeMergeMode Mode)>>? _pendingTxDeltas;

    /// <summary>
    /// Per-(transaction, WAL partition) earliest offset of any prepared
    /// mutation recorded under that transaction id and partition pair.
    /// Populated when the replay coordinator drives
    /// <c>ILeafProjection.Apply</c> with a
    /// <see cref="LatticeApplyOffsetContext"/> scope active; left
    /// untouched on the foreground commit path (where there is no WAL
    /// offset to stamp). A single saga whose per-key writes hash to
    /// distinct WAL partitions registers one entry per partition so the
    /// per-partition projection-checkpoint clamp is computed against
    /// the correct partition's offset space; single-partition replay
    /// stamps partition 0 throughout, preserving the legacy clamp
    /// shape.
    /// <para>
    /// The minimum offset across entries for a given partition is the
    /// projection-checkpoint clamp floor for that partition -
    /// advancing the persisted checkpoint past
    /// <c>(min unresolved prepare offset for partition) - 1</c> would
    /// silently lose any prepare whose terminal mark has not yet
    /// replayed, so
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> clamps
    /// requested advances back to that floor.
    /// </para>
    /// <para>
    /// Lazily allocated on the first prepared-mutation apply that
    /// carries an ambient offset. The vast majority of leaves never
    /// participate in a saga or are not driven by the replay
    /// coordinator, so an upfront allocation per activation would be
    /// pure waste - see the rationale on <see cref="_pendingTx"/>.
    /// </para>
    /// </summary>
    private Dictionary<(Guid TransactionId, int Partition), long>? _pendingTxOffsets;

    /// <summary>
    /// Parallel side-map to <see cref="_pendingTx"/> recording, per
    /// <c>(transactionId, key)</c>, the atomic-batch membership (size, index)
    /// the prepared mutation was written under, when it carried one. A sweep
    /// that copies the bucket onto another shard replays it with the same
    /// membership (issue #4499), so the copy is indistinguishable from a
    /// dispatched prepare on the destination's write-ahead log. Strictly
    /// in-memory and rebuilt from WAL replay exactly like
    /// <see cref="_pendingTx"/>: the prepared WAL record carries both fields.
    /// </summary>
    private Dictionary<Guid, Dictionary<string, (int Size, int Index)>>? _pendingTxBatches;

    /// <summary>
    /// Idempotency dedup set. Populated as terminal marks apply or replay so a
    /// re-applied <see cref="MutationKind.TxCommit"/> /
    /// <see cref="MutationKind.TxAbort"/> for the same transaction id is
    /// a no-op rather than crashing on a missing pending bucket.
    /// Survives only as long as the activation. A new activation rebuilds it
    /// only for the terminal marks its own replay window covers: a replay that
    /// starts at a projection checkpoint past the mark, or at a WAL tail
    /// trimmed past it, leaves the transaction out, and the set is not carried
    /// in a leaf snapshot. It is also the orphan-guard input and the late-prepare
    /// refusal's first check (<see cref="IsLatePrepareForTerminalTransactionAsync"/>),
    /// which is why that refusal also asks the registry for a forwarded prepare
    /// this set does not recognise (issue #4445). Lazily allocated for the same
    /// reason as <see cref="_pendingTx"/>.
    /// </summary>
    private HashSet<Guid>? _recentlyTerminal;

    /// <summary>
    /// Set while the activation-replay self-terminalise sweep
    /// (<see cref="SelfTerminaliseResolvedPreparesAsync"/>) lands a decision
    /// through <see cref="ApplyTxCommit"/>. That sweep runs outside any
    /// <see cref="LatticeApplyOffsetContext"/> scope, but it drains a saga
    /// after pass 1 has absorbed every partition, exactly like a deferred
    /// pass-2 terminal, so it must stamp the drained values the replay way.
    /// Only ever set around a synchronous <see cref="ApplyTxCommit"/> call.
    /// </summary>
    private bool _replayTerminalStamping;

    /// <summary>
    /// Applies a saga commit on behalf of activation replay outside an apply
    /// scope, with replay stamping (see <see cref="_replayTerminalStamping"/>).
    /// </summary>
    private void ApplyReplayTxCommit(Guid transactionId)
    {
        _replayTerminalStamping = true;
        try
        {
            ApplyTxCommit(transactionId);
        }
        finally
        {
            _replayTerminalStamping = false;
        }
    }

    /// <summary>
    /// Whether this leaf has already applied the terminal for <paramref name="txid"/>,
    /// so a surviving pending bucket for it is a late-arriving shadow-forward orphan.
    /// This is the orphan-guard input to <see cref="AtomicVisibilityGate.ResolveKey"/>.
    /// </summary>
    private bool IsRecentlyTerminal(Guid txid) =>
        _recentlyTerminal is not null && _recentlyTerminal.Contains(txid);

    /// <summary>
    /// Whether the write being committed is a saga prepare for a transaction
    /// whose terminal has already been decided and settled, which makes it a
    /// late-arriving orphan: a source shard's shadow-forward of a prepare, or
    /// the retroactive pending-tx sweep, that trailed the saga's terminal to
    /// this leaf.
    /// <para>
    /// Such a prepare is refused before its WAL append rather than bucketed.
    /// The terminal already settled every key it carried here (a commit's
    /// committed-values backstop wrote the authoritative value, an abort
    /// needs nothing), and each saga issues exactly one terminal, so nothing
    /// would ever drain the bucket. Reads hide an orphan only while
    /// <see cref="IsRecentlyTerminal"/> remembers the transaction, and that
    /// memory is per-activation, so an orphan installed on an activation that
    /// does not remember the terminal surfaces as the committed value - a
    /// stale round over newer rows, a duplicate key in a count or scan, and
    /// reads that keep retrying a prepare they cannot settle.
    /// </para>
    /// <para>
    /// Two checks, in cost order. The first is the per-activation memory
    /// (issue #4385), which answers synchronously. It misses a terminal this
    /// leaf applied on an earlier activation - the replay window need not
    /// cover the terminal mark, because the projection checkpoint can advance
    /// past it and WAL GC can trim it - and it misses a saga that has decided
    /// but whose terminal has not reached this leaf yet, which a delayed or
    /// duplicated forward can outrun. A <b>forwarded</b> prepare
    /// (<see cref="LatticeForwardedPrepareContext"/>) the memory does not
    /// recognise is therefore checked against the saga's decision in the
    /// registry (issue #4445), and refused once the saga has decided: bucketed,
    /// it would be stamped on arrival, newer than any write acknowledged since,
    /// and the read gate would surface it as committed over them. Only a
    /// forwarded prepare can arrive after its saga decided: the saga
    /// coordinator's own prepare is acknowledged before the saga decides, so it
    /// pays no registry round trip.
    /// </para>
    /// <para>
    /// Refusing on a decision this leaf has not yet applied is safe for the
    /// same reason. A committed saga had every prepare acknowledged before it
    /// decided, so a forwarded prepare arriving after the decision duplicates
    /// one already delivered or is a sweep replay whose post-sweep cleanup
    /// applies the terminal with the value as its backstop; an aborted saga
    /// needs nothing. The txid is deliberately <b>not</b> recorded as terminal
    /// here: that set also dedups terminals, and the terminal this leaf may
    /// still receive must run its backstop.
    /// </para>
    /// </summary>
    private ValueTask<bool> IsLatePrepareForTerminalTransactionAsync()
    {
        if (!LatticePreparedContext.Current)
            return new ValueTask<bool>(false);

        var txid = LatticeTransactionContext.Current;
        if (txid == Guid.Empty)
            return new ValueTask<bool>(false);

        if (IsRecentlyTerminal(txid))
            return new ValueTask<bool>(true);

        return LatticeForwardedPrepareContext.Current
            ? IsForwardedPrepareForDecidedTransactionAsync(txid)
            : new ValueTask<bool>(false);
    }

    /// <summary>
    /// Asks the registry whether <paramref name="txid"/>'s saga has a terminal
    /// decision, for a forwarded prepare this activation has no memory of (see
    /// <see cref="IsLatePrepareForTerminalTransactionAsync"/>). The registry is
    /// the one the forwarder named (<see cref="LatticeForwardedPrepareContext.RegistryTreeId"/>):
    /// the logical tree, where the saga records its decision, which is not this
    /// leaf's own physical tree id once the tree has been resized. An
    /// <see cref="TxStatus.Indeterminate"/> answer is followed by the recorded
    /// verdict, because a decision masked by the retention window is still a
    /// decision. Fails open: a registry fault or any undecided answer buckets
    /// the prepare as before, since refusing a prepare the saga may still need
    /// would lose a write.
    /// </summary>
    private async ValueTask<bool> IsForwardedPrepareForDecidedTransactionAsync(Guid txid)
    {
        var treeId = LatticeForwardedPrepareContext.RegistryTreeId ?? state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return false;

        TxStatus status;
        try
        {
            var registry = TxRegistryRouting.GetRegistry(grainFactory, treeId, txid);
            status = await registry.GetStatusAsync(txid);
            if (status == TxStatus.Indeterminate)
                status = await registry.GetRecordedStatusAsync(txid);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var logger = ResolveLogger();
            if (logger is not null && logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug(
                    ex,
                    "Could not read the decision for saga '{TxId}' on tree '{TreeId}' before bucketing a forwarded "
                    + "prepare; bucketing it.",
                    txid,
                    treeId);
            }

            return false;
        }

        // The terminal may have landed while the registry call was in flight.
        return status is TxStatus.Committed or TxStatus.Aborted || IsRecentlyTerminal(txid);
    }

    /// <summary>
    /// Tracks per-saga which keys have already had the cross-migration
    /// LWW backstop applied. Keyed by transaction id; value is the set
    /// of keys whose backstop write has landed on this leaf.
    /// <para>
    /// Per-key (NOT per-saga) granularity is load-bearing for the
    /// shard-split + reshard chaos surface: two terminal deliveries to
    /// the same leaf can legitimately carry DIFFERENT
    /// <c>committedValues</c> subsets - e.g.
    /// </para>
    /// <list type="number">
    ///   <item><description>
    ///     <c>AtomicWriteGrain</c>'s direct fan-out to the destination
    ///     shard with the subset routed to that shard per the saga's
    ///     drift-corrected routing snapshot (typically the keys whose
    ///     slot has already migrated).
    ///   </description></item>
    ///   <item><description>
    ///     A source shard's transitive split-forward fan-out (via the
    ///     saga's <c>TerminalFanOutResolver.ResolveTransitiveAsync</c>
    ///     expansion of <c>TouchedShards</c>) reaching the same
    ///     destination with a DIFFERENT subset - the keys whose
    ///     prepare landed on the source pre-split but whose slot has
    ///     since migrated to this destination.
    ///   </description></item>
    /// </list>
    /// <para>
    /// A per-saga dedup (the prior shape) would observe (1) first, mark
    /// the saga "backstopped", and short-circuit (2)'s missing keys -
    /// leaving them stuck at the drained pre-saga value. The chaos
    /// pattern <c>split (pre=5, post=11)</c> on the reshard fixture
    /// reproduces this exactly: 5 keys (one source shard's worth)
    /// orphaned because their backstop arrived after another shard's
    /// subset already poisoned the txid's dedup marker.
    /// </para>
    /// <para>
    /// Lazily allocated for the same reason as <see cref="_pendingTx"/>.
    /// The inner <c>HashSet&lt;string&gt;</c> uses <see cref="StringComparer.Ordinal"/>
    /// for consistency with <see cref="Dictionary{TKey,TValue}"/>
    /// instances elsewhere in this file.
    /// </para>
    /// </summary>
    private Dictionary<Guid, HashSet<string>>? _backstoppedTerminals;

    /// <summary>
    /// The leaf clock at the moment each saga's terminal first landed here
    /// (drained its bucket, wrote its backstop, or found nothing to do). Every
    /// row the landing wrote is stamped at or below it, and every row written
    /// here afterwards is stamped above it, so a later delivery of the same
    /// terminal can tell a key the saga never reached on this leaf - one it
    /// must still backstop - from a key overwritten since.
    /// <para>
    /// A terminal is delivered more than once by design: a saga re-runs its
    /// broadcast whenever it resumes after the decision, and the split-forward
    /// channel delivers a second subset. Each repeat used to backstop every key
    /// the drained bucket no longer held, stamped above every row, so a repeat
    /// that arrived after newer writes resurrected the saga's older values over
    /// them. Lives as long as <see cref="_recentlyTerminal"/>; replay rebuilds
    /// it through <see cref="ApplyTxCommit"/>.
    /// </para>
    /// </summary>
    private Dictionary<Guid, Orleans.Lattice.HybridLogicalClock>? _terminalLandedClock;

    /// <summary>
    /// Records the leaf clock as <paramref name="transactionId"/>'s terminal
    /// landing point the first time its terminal lands here.
    /// </summary>
    private void RecordTerminalLanded(Guid transactionId) =>
        (_terminalLandedClock ??= new Dictionary<Guid, Orleans.Lattice.HybridLogicalClock>())
            .TryAdd(transactionId, state.State.Clock);

    /// <summary>
    /// Whether <paramref name="key"/>'s row was written here after
    /// <paramref name="transactionId"/>'s terminal first landed, so a repeat
    /// delivery must not backstop over it. A migrated row is never treated as
    /// newer: it carries its source's clock, not this leaf's, and the backstop
    /// exists to overwrite a migrated pre-saga value.
    /// </summary>
    private bool IsRowNewerThanTerminalLanding(Guid transactionId, string key) =>
        _terminalLandedClock is not null
        && _terminalLandedClock.TryGetValue(transactionId, out var landed)
        && Cache.TryGetRow(key, out var row)
        && !row.IsMigrated
        && row.Timestamp.CompareTo(landed) > 0;

    /// <summary>
    /// Records a prepared-phase per-key mutation in the pending-tx map.
    /// The entry is invisible to readers until a matching terminal mark
    /// flips or drops it. Idempotent under LWW: a re-applied prepare
    /// for the same <c>(txid, key)</c> uses
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/> so the
    /// strictly-greater HLC always wins.
    /// </summary>
    /// <param name="stampOriginal">
    /// Whether <paramref name="incoming"/> carries its prepare's original stamp
    /// (issue #4522; see <see cref="IsPrepareStampOriginal"/>). Defaults to
    /// unmarked, the pre-#4522 behaviour, so a path that cannot vouch for its
    /// stamp keeps the old drain.
    /// </param>
    private void AddPreparedMutation(
        Guid transactionId,
        string key,
        in LwwValue<byte[]> incoming,
        int capacityHint = 1,
        byte[]? delta = null,
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister,
        (int Size, int Index) batch = default,
        bool stampOriginal = false)
    {
        if (transactionId == Guid.Empty)
        {
            // A prepared mutation must carry a non-empty transaction id
            // so the matching terminal mark can find it; surface this
            // as a programmer error rather than silently leaking the
            // mutation into a never-flushed bucket.
            throw new InvalidOperationException(
                "A prepared mutation must carry a non-empty TransactionId. "
                + "The saga coordinator stamps the id via LatticeTransactionContext "
                + "before opening a LatticePreparedContext scope.");
        }

        var pending = _pendingTx ??= new Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>>();
        if (!pending.TryGetValue(transactionId, out var bucket))
        {
            // Presize to the caller-supplied saga slice size. A batched
            // prepare (CommitSetManyAsync) inserts `capacityHint` distinct
            // keys into a freshly-created bucket; sizing it up front elides
            // the per-saga 0->3->7->17 resize chain (and its intermediate
            // backing-array garbage) that grows the bucket key-by-key.
            // Single-key prepared writes pass the default hint of 1.
            bucket = new Dictionary<string, LwwValue<byte[]>>(capacityHint);
            pending[transactionId] = bucket;
        }

        if (bucket.TryGetValue(key, out var existing))
        {
            var merged = LwwValue<byte[]>.Merge(existing, incoming);
            bucket[key] = merged;
            // The mark follows the value the merge kept: a re-delivered prepare
            // that loses the merge leaves the surviving value's classification.
            if (merged.Timestamp.Equals(incoming.Timestamp))
                SetPrepareStampOriginal(transactionId, key, stampOriginal);
        }
        else
        {
            bucket[key] = incoming;
            SetPrepareStampOriginal(transactionId, key, stampOriginal);
        }

        // The leaf clock dominates every prepared stamp it holds (issue #4530):
        // a range delete stamps itself past the leaf clocks it covers, and must
        // sort above every prepare of a saga decided before it was issued. Every
        // installer already advances the clock past the stamp (a foreground or
        // override prepare through AdvanceClockOrOverride, a replayed one through
        // AdvanceProjectionClock); this keeps it true by construction for any
        // future path that installs a bucket.
        if (incoming.Timestamp > state.State.Clock)
            state.State.Clock = incoming.Timestamp;

        // CRDT-delta carry. A prepared mutation authored under a CRDT merge
        // mode rides its typed delta alongside the merged-state value; record
        // it in the parallel side-map so the terminal drain folds the delta
        // into the receiver's current visible state (the per-replica union)
        // instead of installing the prepared LWW value last-writer-wins.
        // LwwRegister-mode entries and value-only writes carry no delta and
        // leave the side-map untouched, so their terminal drain is unchanged.
        if (delta is not null && mode != LatticeMergeMode.LwwRegister)
        {
            var pendingDeltas = _pendingTxDeltas ??=
                new Dictionary<Guid, Dictionary<string, (byte[], LatticeMergeMode)>>();
            if (!pendingDeltas.TryGetValue(transactionId, out var deltaBucket))
            {
                deltaBucket = new Dictionary<string, (byte[], LatticeMergeMode)>(capacityHint);
                pendingDeltas[transactionId] = deltaBucket;
            }
            // Idempotent re-replay of the same (txid, key) overwrites with an
            // identical (delta, mode) pair; a later strictly-greater HLC
            // prepare for the same key is folded the same way (the typed
            // delta join is commutative, associative, and idempotent), so
            // last-write-here is safe under re-delivery.
            deltaBucket[key] = (delta, mode);
        }

        if (batch.Size > 0)
        {
            var pendingBatches = _pendingTxBatches ??= new Dictionary<Guid, Dictionary<string, (int, int)>>();
            if (!pendingBatches.TryGetValue(transactionId, out var batchBucket))
            {
                batchBucket = new Dictionary<string, (int, int)>(capacityHint);
                pendingBatches[transactionId] = batchBucket;
            }

            batchBucket[key] = batch;
        }

#if LATTICE_DIAG
        // DIAG: prepare landed on this leaf.
        DiagSink.Write($"[DIAG prepare] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} key={key} " +
            $"valRound={DiagDecodeRound(incoming.Value)} " +
            $"hlc={incoming.Timestamp} origin={incoming.OriginClusterId ?? "(local)"} " +
            $"clock={state.State.Clock}");
#endif

        // Strict atomic-visibility: bump the same-silo revision cookie
        // so a co-located LeafCacheGrain notices the new pending key
        // and refreshes its pending-key set on the next read. Without
        // this the cache could continue serving the pre-saga value
        // from its in-memory cache for the prepared key.
        BumpLocalRevision();

        // Record the earliest WAL offset of any prepare under this
        // transaction id, but only when an apply scope is active -
        // foreground commits author the WAL and have no offset to
        // stamp, so they leave _pendingTxOffsets untouched and the
        // checkpoint clamp degrades to a no-op for foreground-only
        // leaves.
        var ambientOffset = LatticeApplyOffsetContext.Current;
        if (ambientOffset is long offset)
        {
            // CurrentPartition is null on the legacy single-partition
            // apply scope; treat as partition 0 to preserve the
            // pre-multi-partition clamp shape.
            var ambientPartition = LatticeApplyOffsetContext.CurrentPartition ?? 0;
            var offsets = _pendingTxOffsets ??= new Dictionary<(Guid, int), long>();
            var offsetKey = (transactionId, ambientPartition);
            if (offsets.TryGetValue(offsetKey, out var existingOffset))
            {
                if (offset < existingOffset)
                {
                    offsets[offsetKey] = offset;
                }
            }
            else
            {
                offsets[offsetKey] = offset;
            }
        }
    }

    /// <summary>
    /// Flips every pending-tx entry under <paramref name="transactionId"/>
    /// into the visible projection via
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/>. The
    /// linearization point for the saga on this leaf - every reader
    /// observes either zero of the saga's keys or every one of them
    /// after this call returns. Idempotent: repeated applies for the
    /// same transaction id are no-ops via
    /// <see cref="_recentlyTerminal"/>.
    /// <para>
    /// <b>Foreground single-cluster path (no <c>OriginClusterId</c>
    /// stamped).</b> Re-stamps every drained value's
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Timestamp"/> with the leaf's current
    /// <c>state.State.Clock</c>. The re-stamp is the cure for the
    /// stuck-key cache delta failure: the cache's per-entry HLC filter
    /// (<c>lww.Timestamp &gt; callerClock</c>) would otherwise exclude
    /// the drained value when intervening foreground writes have
    /// advanced <c>callerClock</c> past the prepared value's original
    /// prepare-time HLC. Re-stamping with <c>state.State.Clock</c>
    /// (which advances on every prepare via
    /// <see cref="AdvanceClockOrOverride"/>) guarantees the drained
    /// value's <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Timestamp"/> is strictly greater
    /// than every <c>callerClock</c> the cache could have observed
    /// during the saga, because the prepare path no longer ticks
    /// <c>state.State.Version[ReplicaId]</c> (only intervening
    /// non-saga writes do), so <c>callerClock</c> at terminal-time
    /// refresh trails <c>state.State.Clock</c> by at least one
    /// prepare-tick.
    /// </para>
    /// <para>
    /// <b>Cross-cluster atomic-apply path (per-entry
    /// <c>OriginClusterId</c> stamped).</b> Preserves every drained
    /// value's <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Timestamp"/> verbatim. The source
    /// cluster's per-entry HLC is the authoritative ordering token for
    /// receiver-side LWW resolution and MUST NOT be clobbered by the
    /// local clock. The cache-delta-filter constraint that motivates
    /// the foreground re-stamp is intrinsic to HLC-based filtering
    /// across clock-skewed clusters and is accepted here; the cache's
    /// revision-bump path delivers these values via full snapshot
    /// reload rather than per-entry delta.
    /// </para>
    /// <para>
    /// The branch decision uses <see cref="Orleans.Lattice.Primitives.LwwValue{T}.OriginClusterId"/>
    /// - a deterministic, persisted signal stamped at prepare time
    /// from <see cref="LatticeOriginContext"/>. Because the flag is
    /// written into the WAL TxPrepare record's <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/>
    /// payload (see
    /// <see cref="BPlusLeafGrain.CommitSetAsync(string, byte[], long)"/>),
    /// foreground and replay observe the same value and therefore
    /// produce bit-identical projection states. Replay must NOT use
    /// <see cref="LatticeHlcOverrideContext"/> as the signal because
    /// that ambient is foreground-only.
    /// </para>
    /// <para>
    /// Replay determinism for the foreground branch: the replay
    /// coordinator drives <see cref="ILeafProjection.Apply"/> over the
    /// WAL in offset order, advancing <c>state.State.Clock</c> via
    /// <see cref="AdvanceProjectionClock"/> on every prior WAL entry.
    /// At terminal-replay time, <c>state.State.Clock</c> equals the
    /// max of all prior WAL <see cref="LatticeMutation.Timestamp"/>
    /// values, which matches what foreground saw when the terminal
    /// was originally appended - so foreground and replay produce
    /// bit-identical drained <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Timestamp"/>
    /// values. The WAL terminal entry itself stamps
    /// <see cref="HybridLogicalClock.Zero"/> by convention (saga-wide
    /// events have no per-key HLC), so we do not consult
    /// <see cref="LatticeMutation.Timestamp"/> for the re-stamp.
    /// </para>
    /// </summary>
    /// <param name="transactionId">The saga whose bucket to drain.</param>
    /// <param name="skipKeys">
    /// Bucket keys to leave undrained because the caller re-routes them to
    /// the leaf that now declares them; <see langword="null"/> drains the
    /// whole bucket.
    /// </param>
    private void ApplyTxCommit(Guid transactionId, IReadOnlySet<string>? skipKeys = null)
    {
        if (transactionId == Guid.Empty)
            return;

        // Fast-path: leaf never saw a prepared mutation. Record the
        // terminal so a late-arriving prepared mutation under the same
        // id does not silently leak, then exit without touching
        // _pendingTx (which may still be null).
        if (_pendingTx is null || !_pendingTx.Remove(transactionId, out var bucket))
        {
            RemovePendingTxOffsetsForTransaction(transactionId);
            (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
            RecordTerminalLanded(transactionId);
#if LATTICE_DIAG
            // DIAG: commit arrived on leaf with no bucket (fast-path).
            DiagSink.Write($"[DIAG commit-empty] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} clock={state.State.Clock}");
#endif
            return;
        }

#if LATTICE_DIAG
        // DIAG: commit will drain this bucket.
        {
            var keys = string.Join(",", bucket.Keys);
            DiagSink.Write($"[DIAG commit] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} bucket=[{keys}] clock={state.State.Clock}");
            foreach (var kvp in bucket)
            {
                var hasExisting = Cache.TryGetRow(kvp.Key, out var existing);
                DiagSink.Write($"[DIAG commit-key] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} key={kvp.Key} " +
                    $"prepared.Hlc={kvp.Value.Timestamp} " +
                    $"existing={(hasExisting ? $"hlc={existing.Timestamp},isMig={existing.IsMigrated}" : "(none)")}");
            }
        }
#endif

        // Drain the parallel CRDT-delta side-map for this transaction. A
        // null bucket (the common case) means no prepared key under this
        // saga carried a typed delta, so every drain below installs the
        // prepared LWW value verbatim exactly as before. When present, a
        // per-key (delta, mode) entry routes that key through the typed
        // fold instead.
        Dictionary<string, (byte[] Delta, LatticeMergeMode Mode)>? deltaBucket = null;
        _pendingTxDeltas?.Remove(transactionId, out deltaBucket);
        _pendingTxBatches?.Remove(transactionId);

        // Keys the caller re-routes to the leaf that declares them (a split
        // narrowed this leaf's span after their prepare landed) are never
        // drained here. The bucket is already detached from _pendingTx, so
        // removing them only narrows this drain.
        if (skipKeys is { Count: > 0 })
        {
            foreach (var key in skipKeys)
                bucket.Remove(key);
        }

        // Branch on the persisted OriginClusterId signal. See the
        // method's XML doc for the full rationale and the replay
        // determinism argument.
        var preserveTimestamps = false;
        foreach (var kvp in bucket)
        {
            if (!string.IsNullOrEmpty(kvp.Value.OriginClusterId))
            {
                preserveTimestamps = true;
                break;
            }
        }

        if (preserveTimestamps)
        {
            // Cross-cluster atomic apply: preserve per-entry source HLCs
            // verbatim. Advance state.State.Clock to the max of the
            // bucket's Timestamps so subsequent local reads observe a
            // monotonic clock. The bucket value carries IsMigrated=false
            // (prepared mutations are never migration imports), so the
            // merge in StoreEntry clears any stale migration marker
            // when this value wins.
            foreach (var kvp in bucket)
            {
                var toStore = kvp.Value;
                if (deltaBucket is not null
                    && deltaBucket.TryGetValue(kvp.Key, out var dm))
                {
                    // CRDT-mode prepared entry: fold the typed delta into
                    // this leaf's current visible state rather than installing
                    // the prepared merged-state value last-writer-wins. The
                    // fold is a join (commutative, associative, idempotent),
                    // so two clusters' concurrent staged writes to the same
                    // key converge on the per-replica union on both sides.
                    var folded = FoldPreparedCrdtDelta(kvp.Key, dm.Delta, dm.Mode);

                    // The folded value is the post-join full state, so it
                    // must DOMINATE the entry currently in the cache: unlike
                    // the verbatim-LWW path, preserving the source HLC here
                    // would let StoreEntry's LWW merge discard the join when
                    // the leaf's current value (e.g. this site's own
                    // foreground staged write) carries a higher HLC, stranding
                    // the two clusters at divergent values. Re-stamp with an
                    // HLC strictly past the max of the prepared source stamp,
                    // the existing entry's stamp, and the leaf clock so the
                    // join always lands. Replay determinism holds: the same
                    // ordered prepare/terminal stream reproduces the same
                    // cache snapshot and therefore the same stamp.
                    var dominateBase = kvp.Value.Timestamp;
                    if (Cache.TryGetRow(kvp.Key, out var existingCrdt)
                        && existingCrdt.Timestamp.CompareTo(dominateBase) > 0)
                    {
                        dominateBase = existingCrdt.Timestamp;
                    }
                    if (state.State.Clock.CompareTo(dominateBase) > 0)
                    {
                        dominateBase = state.State.Clock;
                    }
                    var foldStamp = new Orleans.Lattice.HybridLogicalClock
                    {
                        WallClockTicks = dominateBase.WallClockTicks,
                        Counter = dominateBase.Counter + 1,
                    };
                    toStore = kvp.Value with { Value = folded, Timestamp = foldStamp };
                }
                StoreEntry(kvp.Key, toStore);
                if (deltaBucket is not null && deltaBucket.TryGetValue(kvp.Key, out var dmMode))
                {
                    // CRDT commit: record the per-key merge mode after StoreEntry
                    // (whose byte-row write evicts any prior recorded mode) so a
                    // snapshot capture labels the committed key faithfully.
                    Cache.SetMergeMode(kvp.Key, dmMode.Mode);
                }
                AdvanceProjectionClock(toStore.Timestamp);
            }
        }
        else
        {
            // Foreground single-cluster: re-stamp with terminal-time Clock
            // for cache-delta-filter correctness.
            //
            // Cross-shard-migration LWW dominance (Fix M). Under an online
            // reshard, the destination leaf is freshly created and its
            // state.State.Clock starts near Zero, while the SOURCE leaf's
            // Entries[K] for a saga-touched key carries the HLC stamped at
            // a PRIOR saga's terminal-flip time on the source - a high
            // HLC reflecting the source leaf's cumulative tick history.
            // TreeShardSplitGrain.ForwardMovedSlotEntriesAsync ships those
            // entries verbatim via target.MergeManyAsync, so the
            // destination's Entries[K] inherits the source's high HLC
            // BEFORE the current saga's terminal drains the destination's
            // pending bucket. If we re-stamp with state.State.Clock
            // verbatim (the destination's low clock) and let StoreEntry
            // LWW-merge against the migrated value, the migrated value
            // WINS because its HLC dominates ours - silently overwriting
            // the saga's drained value with the pre-saga value the
            // migration carried. The chaos-suite "other=1" stuck-key
            // failure shape on Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard
            // reproduces this exactly: one key per reshard window stays
            // at an OLDER round's value across multiple subsequent
            // sagas because every drain on the destination loses LWW to
            // the migrated entry until the destination's clock organically
            // catches up.
            //
            // Fix: pre-scan the bucket for any existing Entries[K] whose
            // HLC dominates state.State.Clock, then Tick once past the
            // observed max. The single Tick is sufficient because the
            // migrated HLC is observed atomically here and the resulting
            // terminalStamp strictly dominates it via HLC.Tick's
            // strict-greater semantic.
            var replayStamping = _replayTerminalStamping || LatticeApplyOffsetContext.Current is not null;
            var baseTerminalStamp = replayStamping ? default : state.State.Clock;
            var anyUsesTerminalStamp = false;
            foreach (var kvp in bucket)
            {
                // A marked LWW prepare is applied at its own stamp (issue #4522,
                // below), never at terminalStamp, so it must not pull
                // terminalStamp: only unmarked keys and CRDT folds use it.
                if (IsMarkedLwwPrepare(transactionId, kvp.Key, deltaBucket))
                    continue;
                anyUsesTerminalStamp = true;

                if (Cache.TryGetRow(kvp.Key, out var preExisting))
                {
                    // Mirror the orphan-drain skip condition below:
                    // whose existing HLC dominates the prepared HLC will
                    // NOT be written, so its existing.Timestamp must not
                    // pull terminalStamp past where we need it for the
                    // keys we WILL write.
                    //
                    // Migration-provenance carve-out: a dominating
                    // preExisting whose value carries IsMigrated=true
                    // (stamped at MergeIntoState / MergeEntriesAsync
                    // import time) IS going to be written below, so
                    // its HLC MUST contribute to baseTerminalStamp -
                    // otherwise the drained stamp would lose LWW to
                    // the migrated entry's high HLC.
                    if (preExisting.Timestamp.CompareTo(kvp.Value.Timestamp) > 0
                        && !preExisting.IsMigrated)
                    {
#if LATTICE_DIAG
                        // DIAG: pre-scan skip - capture stuck-key signature.
                        DiagSink.Write($"[DIAG pre-scan-skip] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} key={kvp.Key} " +
                            $"existing.Hlc={preExisting.Timestamp} existing.IsMigrated={preExisting.IsMigrated} " +
                            $"existing.Origin={preExisting.OriginClusterId ?? "(local)"} " +
                            $"prepared.Hlc={kvp.Value.Timestamp} prepared.Origin={kvp.Value.OriginClusterId ?? "(local)"} " +
                            $"clock={state.State.Clock}");
#endif
                        continue;
                    }
                    if (preExisting.Timestamp.CompareTo(baseTerminalStamp) > 0)
                        baseTerminalStamp = preExisting.Timestamp;
                }

                if (replayStamping && kvp.Value.Timestamp.CompareTo(baseTerminalStamp) > 0)
                    baseTerminalStamp = kvp.Value.Timestamp;
            }
            // Replay stamping (activation replay and its self-terminalise sweep).
            // The foreground stamp is the leaf clock at the moment the terminal
            // arrived, and the deterministic-replay argument below assumes replay
            // sees that same clock. It does not when a terminal is deferred to
            // pass 2 of a multi-partition replay (or drained by the pass-2.5
            // self-terminalise sweep): by then pass 1 has absorbed every
            // partition, including the prepares of LATER sagas on the same keys,
            // so the leaf clock already sits above them. Stamping with it lifts
            // this (earlier) saga's drained values above those later prepares,
            // and when a later saga's terminal arrives the orphan-drain guard
            // below reads that as "a strictly-later saga already drained this
            // key" and discards the later saga's committed value - an acknowledged
            // batch that stays invisible on this leaf while every other leaf
            // shows it. So replay stamps from the keys it actually writes: just
            // past the higher of their existing rows and this bucket's own
            // prepares. That never exceeds the foreground stamp (the foreground
            // clock already dominated both), so every skip the guard makes on
            // the foreground path it still makes here, and a saga that prepared
            // after this one drained still out-ranks it.
            var maxBase = replayStamping || baseTerminalStamp.CompareTo(state.State.Clock) > 0
                ? baseTerminalStamp
                : state.State.Clock;
            // Counter-only bump past the higher of state.State.Clock and
            // baseTerminalStamp. The bump is load-bearing for cache-delta
            // visibility: ApplyTxTerminalAsync publishes the pre-bump
            // state.State.Clock as the new Version[ReplicaId] (see the
            // comment block around the call site), and the cache's
            // GetDeltaSinceAsync filter excludes any entry whose Timestamp
            // is <= the caller's last-observed Version[ReplicaId]. A
            // strictly-greater terminalStamp is therefore required for the
            // drained entries to be delivered on the next refresh.
            //
            // Replay determinism: HybridLogicalClock.Tick is non-deterministic
            // (it reads DateTimeOffset.UtcNow.Ticks, so foreground and
            // terminal-replay produce different WallClockTicks values). A
            // counter-only bump - construct a new HLC with the same
            // WallClockTicks and Counter+1 - is deterministic for a given
            // base. The foreground base is the leaf clock; the replay base is
            // derived from the written keys alone (see "Replay stamping"
            // above), so a replayed drain is stamped at or below the stamp the
            // foreground drain carried and never above any prepare that
            // arrived after it - which is the ordering the cross-saga LWW
            // dominance checks below rely on.
            var terminalStamp = new Orleans.Lattice.HybridLogicalClock
            {
                WallClockTicks = maxBase.WallClockTicks,
                Counter = maxBase.Counter + 1,
            };
            foreach (var kvp in bucket)
            {
                // CRDT-mode prepared entry: fold the typed delta into this
                // leaf's current visible state and skip the LWW orphan-drain
                // / migration-dominance guards entirely. Those guards exist to
                // stop an older LWW value clobbering a newer one; a typed-delta
                // fold is a join (commutative, associative, idempotent) and can
                // never lose information, so folding into whatever the leaf
                // currently holds is always safe and re-delivery-idempotent.
                // The fold still re-stamps with the deterministic terminalStamp
                // so the cache-delta filter delivers the folded value and
                // subsequent LWW reads stay monotonic.
                if (deltaBucket is not null
                    && deltaBucket.TryGetValue(kvp.Key, out var dmFg))
                {
                    var foldedFg = FoldPreparedCrdtDelta(kvp.Key, dmFg.Delta, dmFg.Mode);
                    StoreEntry(kvp.Key, kvp.Value with { Value = foldedFg, Timestamp = terminalStamp });
                    // CRDT commit: record the per-key merge mode after StoreEntry
                    // so a snapshot capture labels the committed key faithfully.
                    Cache.SetMergeMode(kvp.Key, dmFg.Mode);
                    continue;
                }

                // Issue #4522: a marked prepare carries its original stamp P, so
                // the saga's value is applied under last-writer-wins AT P. A row
                // stamped at or above P is a write acknowledged after the prepare
                // - migrated or not - and survives; otherwise the value is stored
                // at P, never at a fresh stamp that a later write forwarded in
                // after this drain would then lose to. Unmarked prepares keep the
                // pre-#4522 rule below, including the migrated-row carve-out.
                if (IsPrepareStampOriginal(transactionId, kvp.Key))
                {
                    if (!IsRowAtOrAboveOriginalStamp(kvp.Key, kvp.Value.Timestamp))
                        StoreAtOriginalStamp(kvp.Key, kvp.Value);
                    continue;
                }

                // Orphan-drain guard. Under an online reshard, a saga's
                // shadow-forwarded prepare can land on a destination
                // leaf AFTER the saga's terminal broadcast already
                // reached the same leaf via the cross-migration LWW
                // backstop path (which writes Entries directly with no
                // bucket to flip). A second terminal for the same saga
                // - typically a duplicate via the late-refetch loop in
                // AtomicWriteGrain.BroadcastTerminalsAsync - observes
                // the orphan bucket with alreadyFlipped=false and
                // would drain it here. Re-stamping the drained value
                // with the current state.State.Clock unconditionally
                // dominates ANY prior Entries timestamp via LWW.Merge,
                // so a strictly-later saga that has ALREADY drained
                // the same key (Entries[K] = V_{newer}) gets silently
                // overwritten by this saga's (now-stale) V_{older}.
                // The orphan's prepared HLC (kvp.Value.Timestamp) is
                // the saga's source-time stamp captured at PREPARE
                // time on the source shard - strictly less than the
                // destination's terminal-time stamp for a strictly-
                // later saga that touched the same key. So if Entries
                // already holds a timestamp dominating the prepared
                // HLC, this drain is logically obsolete and must be
                // skipped to preserve the cross-saga LWW ordering.
                // Replay determinism is preserved: the same HLC
                // comparison runs against the same Entries snapshot
                // during WAL replay, producing the same skip decision.
                // This is the write-side complement to the read-side
                // orphan-pending guard in GetWithPendingAsync; both
                // are needed because the orphan can manifest either
                // as a surviving pending bucket (read-side path) or
                // as an already-drained-but-stale Entries write
                // (write-side path).
                //
                // Migration-provenance carve-out: the inverse race
                // also exists. Under a cross-shard reshard, a saga's
                // shadow-forwarded prepare can land on a freshly-
                // created destination leaf BEFORE
                // TreeShardSplitGrain.ForwardMovedSlotEntriesAsync
                // ships the source's high-HLC entries to the
                // destination via target.MergeManyAsync. The pending
                // bucket on the destination then carries a LOW
                // prepared HLC (the destination's low clock at
                // prepare-arrival time), and migration subsequently
                // imports the source's HIGH migrated HLC into
                // Entries. When the saga's terminal arrives, this
                // guard would see `existing.Timestamp > prepared.Timestamp`
                // and skip the drain - silently discarding the
                // current saga's authoritative value in favour of
                // the pre-saga migrated value. The IsMigrated flag
                // on the existing value distinguishes the two shapes:
                // when the dominating existing entry came from a
                // migration, the drain proceeds; when it came from a
                // strictly-later sibling-saga drain (IsMigrated=false),
                // the drain is correctly skipped. See LwwValue.IsMigrated
                // for the discriminator's full semantics.
                if (Cache.TryGetRow(kvp.Key, out var existing)
                    && existing.Timestamp.CompareTo(kvp.Value.Timestamp) > 0
                    && !existing.IsMigrated)
                {
#if LATTICE_DIAG
                    // DIAG: drain-loop skip - capture stuck-key signature.
                    DiagSink.Write($"[DIAG drain-skip] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} key={kvp.Key} " +
                        $"existing.Hlc={existing.Timestamp} existing.IsMigrated={existing.IsMigrated} " +
                        $"existing.Origin={existing.OriginClusterId ?? "(local)"} " +
                        $"prepared.Hlc={kvp.Value.Timestamp} prepared.Origin={kvp.Value.OriginClusterId ?? "(local)"} " +
                        $"clock={state.State.Clock} terminalStamp={terminalStamp}");
#endif
                    continue;
                }
                // The prepared value carries IsMigrated=false (default
                // - prepared mutations are never migration imports);
                // the re-stamp preserves that, so StoreEntry's merge
                // naturally clears any stale migration marker.
                var restamped = kvp.Value with { Timestamp = terminalStamp };
                StoreEntry(kvp.Key, restamped);
            }
            // terminalStamp is used only by unmarked LWW keys and CRDT folds; a
            // bucket of marked LWW prepares stores every value at its own stamp
            // and advances nothing.
            if (anyUsesTerminalStamp)
                AdvanceProjectionClock(terminalStamp);
        }

        RemovePendingTxOffsetsForTransaction(transactionId);
        // Durable, per-key record of what this terminal settled here without a
        // marked prepare stamp (issue #4545). A marked last-writer-wins key is
        // stored at its stamp P, so any stamped marker for it is released by the
        // read gate's self-check; every other key needs the witness. Recorded
        // before the classification is forgotten. Keys re-routed to the leaf
        // that declares them were removed from the bucket above.
        RecordTerminalWitness(
            transactionId,
            bucket.Keys,
            key => preserveTimestamps || !IsMarkedLwwPrepare(transactionId, key, deltaBucket));
        ForgetPrepareStampClassification(transactionId);
        (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
        RecordTerminalLanded(transactionId);

        // Bump the same-silo revision cookie so a co-located
        // LeafCacheGrain notices both that the pending bucket has
        // drained AND that Entries now carries the post-saga values,
        // and refreshes its own state on the next read.
        BumpLocalRevision();
    }

    /// <summary>
    /// Folds a prepared CRDT mutation's typed <paramref name="delta"/> into
    /// this leaf's current visible state for <paramref name="key"/> under the
    /// given <paramref name="mode"/>, returning the re-serialised post-fold
    /// state bytes. Loads the existing visible value (or an empty primitive
    /// when the key is absent / tombstoned), deserialises it via the
    /// registered <see cref="CrdtShape"/>, applies the delta through the
    /// primitive's instance <c>MergeDelta</c>, and re-serialises. The fold is
    /// the terminal-commit complement of the producer-side
    /// <see cref="ApplyCrdtDeltaAsync"/> path and uses the same type-erased
    /// shape registry, so OrSet / PnCounter / VersionVector / MvRegister /
    /// OrFlag / RwFlag / Sequence resolve through the global closed-shape
    /// descriptors and OrMap through the per-tree registration.
    /// </summary>
    private byte[] FoldPreparedCrdtDelta(string key, byte[] delta, LatticeMergeMode mode)
    {
        var treeId = RequireBoundTreeId(key, mode, "the prepared CRDT-mode terminal-commit fold");
        var registry = ResolveCrdtShapeRegistry();
        var shape = registry.TryGet(treeId, mode)
            ?? throw new LatticeCrdtShapeNotRegisteredException(
                "No CrdtShape is registered for tree '"
                + treeId
                + "' at mode '"
                + mode
                + "'. A prepared CRDT-mode atomic write cannot fold its typed "
                + "delta on the terminal commit without a shape descriptor; "
                + "register the OR-Map pair via ISiloBuilder.AddOrMapShape<TKey, TValue>(treeName) "
                + "for OR-Map trees (closed-shape modes resolve through the global fallback).",
                treeId);

        // Strip any version envelope from the durable delta before deserialising
        // (version-agnostic; identity when no versioning is active) so the fold
        // sees the raw typed-CRDT body. See the determinism remarks on
        // ILatticeEnvelopeCodec: the same durable bytes strip to the same body on
        // every replay, so the terminal-commit fold stays byte-identical.
        //
        // The stored state is stripped by the same contract just below. Both halves
        // are required: stripping only the delta hands the shape an enveloped state
        // whose 0xFE magic fails a JSON decode at byte zero, and because every retry
        // re-decodes the same bytes the row becomes permanently unwritable while
        // still reading back cleanly.
        var typedDelta = shape.DeserializeDelta(StripDeltaForFold(delta));
        object typedState;
        if (Cache.TryGetRow(key, out var existing)
            && !existing.IsTombstone
            && existing.Value is { Length: > 0 } existingBytes)
        {
            typedState = shape.DeserializeState(StripStateForFold(existingBytes));
        }
        else
        {
            typedState = shape.CreateEmpty();
        }
        shape.MergeDelta(typedState, typedDelta);
        return shape.SerializeState(typedState);
    }

    /// <summary>
    /// Joins a full CRDT <paramref name="incomingState"/> into this leaf's current
    /// visible state for <paramref name="key"/> under <paramref name="mode"/>,
    /// returning the re-serialised joined state: the state-based complement of
    /// <see cref="FoldPreparedCrdtDelta"/> for a caller that holds a whole state
    /// rather than a typed delta. The existing value (or an empty primitive when
    /// the key is absent or tombstoned) and the incoming state are both stripped
    /// of any version envelope, deserialised through the registered
    /// <see cref="CrdtShape"/>, and merged with <see cref="CrdtShape.MergeStates"/>,
    /// which is commutative, associative and idempotent, so a re-delivered state
    /// joins to the same bytes. The result is written back unenveloped, exactly as
    /// the fold's output is. The caller chooses the stamp.
    /// <para>
    /// The terminal backstop uses it to install a saga's committed CRDT value
    /// without discarding contributions the row gained after the stage-time
    /// snapshot the value was computed from (issue #4611).
    /// </para>
    /// </summary>
    private byte[] JoinCrdtStateIntoRow(string key, LatticeMergeMode mode, byte[] incomingState)
    {
        var treeId = RequireBoundTreeId(key, mode, "the CRDT state join");
        var shape = ResolveCrdtShapeRegistry().TryGet(treeId, mode)
            ?? throw new LatticeCrdtShapeNotRegisteredException(
                "No CrdtShape is registered for tree '"
                + treeId
                + "' at mode '"
                + mode
                + "'. A committed CRDT value cannot be joined into the row without a "
                + "shape descriptor; register the OR-Map pair via "
                + "ISiloBuilder.AddOrMapShape<TKey, TValue>(treeName) for OR-Map trees "
                + "(closed-shape modes resolve through the global fallback).",
                treeId);

        var joined = Cache.TryGetRow(key, out var existing)
            && !existing.IsTombstone
            && existing.Value is { Length: > 0 } existingBytes
                ? shape.DeserializeState(StripStateForFold(existingBytes))
                : shape.CreateEmpty();
        shape.MergeStates(joined, shape.DeserializeState(StripStateForFold(incomingState)));
        return shape.SerializeState(joined);
    }

    /// <summary>
    /// Drops every pending-tx entry under <paramref name="transactionId"/>
    /// without ever making it visible to readers - the saga's
    /// prepare-phase writes are undone in a single linearization step.
    /// Idempotent.
    /// </summary>
    private void ApplyTxAbort(Guid transactionId)
    {
        if (transactionId == Guid.Empty)
            return;

        Dictionary<string, LwwValue<byte[]>>? abortedBucket = null;
        var hadPending = _pendingTx is not null && _pendingTx.Remove(transactionId, out abortedBucket);
        ForgetPrepareStampClassification(transactionId);
        // Drop the parallel CRDT-delta side-map entry for this saga so an
        // aborted prepared CRDT write leaks no folded contribution; the
        // staged delta never became visible, so the abort discards it exactly
        // as it discards the pending LWW value.
        _pendingTxDeltas?.Remove(transactionId);
        _pendingTxBatches?.Remove(transactionId);
        RemovePendingTxOffsetsForTransaction(transactionId);
        (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
        // The discarded keys keep (abort) or already carry (a late orphan behind a
        // landed terminal) the value the decision implies (issue #4545).
        if (abortedBucket is not null)
            RecordTerminalWitness(transactionId, abortedBucket.Keys);

#if LATTICE_DIAG
        // DIAG: abort entry.
        DiagSink.Write($"[DIAG abort] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} hadPending={hadPending} clock={state.State.Clock}");
#endif

        // Bump the same-silo revision cookie so a co-located
        // LeafCacheGrain refreshes its pending-key set and stops
        // delegating reads for keys this aborted saga had prepared.
        if (hadPending)
            BumpLocalRevision();
    }

    /// <summary>
    /// Returns <c>true</c> if any pending-tx entry under any transaction
    /// id covers <paramref name="key"/>. Used by the read-path filter
    /// to hide saga prepare-phase writes from concurrent readers
    /// without a per-call RPC. O(pending-txs) - bounded by the small
    /// cardinality of in-flight sagas and the concurrent saga rate;
    /// returns immediately when the pending-tx map has never been
    /// allocated (the steady state for every leaf that has not
    /// participated in a saga since activation).
    /// <para>
    /// Strict atomic-visibility note: this is the cheap presence test;
    /// callers must NOT use it as the read-path verdict by itself.
    /// When it returns <c>true</c> the caller dials back through
    /// <see cref="ResolvePendingStatusAsync"/> (single-key paths) or
    /// <see cref="SnapshotPendingForReadAsync"/> (scan paths) to
    /// consult the per-tree <see cref="ITxRegistryGrain"/> for the
    /// recorded saga outcome. The registry's recorded decision is the
    /// single tree-wide linearization point - without it, a reader
    /// landing on this leaf during the post-commit-decision /
    /// pre-terminal-fan-out window would observe the saga's prepared
    /// keys as hidden while a sibling leaf had already flipped them
    /// visible (a split view).
    /// </para>
    /// </summary>
    private bool IsKeyPending(string key)
    {
        if (_pendingTx is null || _pendingTx.Count == 0)
            return false;

        foreach (var bucket in _pendingTx.Values)
        {
            if (bucket.ContainsKey(key))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Synchronously locates the pending-tx entry for <paramref name="key"/>
    /// (if any) and outputs the owning transaction id and prepared
    /// value. Returns <c>false</c> on the steady-state path where the
    /// pending-tx map is empty or the key has no prepared mutation.
    /// When <c>true</c>, callers MUST consult
    /// <see cref="ResolvePendingStatusAsync"/> with the returned txid
    /// before serving the read - this method does not look at the
    /// per-tree TxRegistry.
    /// <para>
    /// O(pending-txs); bounded by in-flight saga cardinality. When two
    /// independent sagas have prepared the same key (which can happen
    /// after a shard split's retroactive sweep installs a prepare for
    /// a saga whose terminal then arrives only at the source shard,
    /// leaving an orphan on the destination, while a later saga
    /// prepares the same key against the destination), the bucket
    /// with the strictly-greater <see cref="HybridLogicalClock"/>
    /// timestamp wins this lookup. The newest prepare always
    /// represents the saga whose terminal is most likely to be
    /// pending or recently delivered, so preferring it minimises
    /// stale-read exposure when an orphaned older prepare lingers in
    /// the pending map. Idempotent re-replays of the same
    /// <c>(txid, key)</c> use the same timestamp and produce a fixed
    /// point under this tie-break. The newest bucket is only the
    /// starting point: when another bucket also covers the key, the
    /// read paths resolve the key against the bucket that decides it
    /// (<see cref="SelectPendingForKeyAsync"/>), because a newer
    /// in-flight prepare must not shadow an older one whose saga has
    /// committed.
    /// </para>
    /// </summary>
    private bool TryFindPendingForKey(string key, out Guid txid, out LwwValue<byte[]> pendingValue)
    {
        txid = Guid.Empty;
        pendingValue = default;
        if (_pendingTx is null || _pendingTx.Count == 0)
            return false;

        var found = false;
        foreach (var (id, bucket) in _pendingTx)
        {
            if (!bucket.TryGetValue(key, out var value))
                continue;

            if (!found || value.Timestamp.CompareTo(pendingValue.Timestamp) > 0)
            {
                txid = id;
                pendingValue = value;
                found = true;
            }
        }
        return found;
    }

    /// <summary>
    /// Asynchronously resolves the recorded outcome for
    /// <paramref name="txid"/> via the per-tree
    /// <see cref="ITxRegistryGrain"/>. This is the read-path dial-back
    /// that lets a leaf serving a key with a pending-tx entry decide
    /// whether to surface the prepared (post-saga) value, hide the
    /// key, or fall through to the pre-saga value in the runtime
    /// entry cache.
    /// <para>
    /// Returns <see cref="TxStatus.InFlight"/> on degenerate inputs
    /// (empty txid or unknown tree id) - the strict-isolation default: the
    /// prepared value stays invisible and the reader falls through to the
    /// pre-saga value. (For an unknown tree id the scan path,
    /// <see cref="SnapshotPendingForReadAsync"/>, answers
    /// <see cref="TxStatus.Indeterminate"/> instead, which hides the key.)
    /// </para>
    /// <para>
    /// Issue #2215: a registry call that fails in transport throws
    /// <see cref="LatticeTransactionOutcomeUnavailableException"/> carrying
    /// the tree, <paramref name="key"/> and <paramref name="txid"/>, rather
    /// than the raw transport exception and never a guessed status. There is
    /// deliberately no retry here - the caller owns retry policy.
    /// </para>
    /// </summary>
    private async ValueTask<TxStatus> ResolvePendingStatusAsync(Guid txid, string? key = null)
    {
        if (txid == Guid.Empty) return TxStatus.InFlight;

        // Linearizable-scan fast path: when the lattice-level fan-out
        // has stamped a per-scan registry snapshot via
        // LatticeRegistrySnapshotContext, use the snapshot's recorded
        // status (or InFlight when absent) so this single-key dial-back
        // shares the same registry view as any sibling leaf scan in
        // the same fan-out.
        var ambient = LatticeRegistrySnapshotContext.Current;
        if (ambient is not null)
        {
            return ambient.TryGetValue(txid, out var ambientStatus) ? ambientStatus : TxStatus.InFlight;
        }

        var treeId = state.State.TreeId;

        // Issue #3641: the lattice-level fan-out has no single decision view,
        // so resolving this prepare at this leaf's own moment could tear the
        // read against a sibling leaf. Fail closed rather than resolve.
        if (LatticeRegistrySnapshotContext.IsUnavailable)
        {
            throw LatticeTransactionOutcomeUnavailableException.Create(treeId ?? string.Empty, key, 1, [txid], null);
        }

        if (string.IsNullOrEmpty(treeId)) return TxStatus.InFlight;
        try
        {
            return await TxRegistryRouting
                .GetRegistry(grainFactory, treeId, txid)
                .GetStatusAsync(txid);
        }
        catch (Exception ex) when (TxRegistryTransportFault.IsTransportFailure(ex))
        {
            throw LatticeTransactionOutcomeUnavailableException.Create(treeId, key, 1, [txid], ex);
        }
    }

    /// <summary>
    /// The terminal-intent twin of <see cref="ResolvePendingStatusAsync"/> for
    /// the activation self-terminalise sweep, which APPLIES the answer (issue
    /// #4485). It asks <see cref="ITxRegistryGrain.GetStatusForTerminalAsync"/>,
    /// which returns a terminal verdict only once it is durably recorded on the
    /// registry, and never under a snapshot capture's decision gate for a saga
    /// not decided before it - so this sweep cannot land a terminal a capture's
    /// decision snapshot does not account for. Ignores the read-path ambient
    /// snapshot contexts: the sweep is not a read.
    /// </summary>
    private async ValueTask<TxStatus> ResolvePendingStatusForTerminalAsync(Guid txid)
    {
        if (txid == Guid.Empty) return TxStatus.InFlight;
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId)) return TxStatus.InFlight;
        return await TxRegistryRouting
            .GetRegistry(grainFactory, treeId, txid)
            .GetStatusForTerminalAsync(txid);
    }

    /// <summary>
    /// Issue #2190. Self-terminalises every saga prepare still resident in
    /// <c>_pendingTx</c> after activation-time replay whose saga the per-tree
    /// <see cref="ITxRegistryGrain"/> reports as terminally decided, by applying
    /// that decision locally through the ordinary <see cref="ApplyTxCommit"/> /
    /// <see cref="ApplyTxAbort"/> path.
    /// <para>
    /// The defect it closes: a saga can finish - or be abandoned - without a
    /// terminal ever reaching a bucket-holding leaf (an empty-txid write, a
    /// shutdown-refused shard, the saga's late-pickup participant re-fetch
    /// exhausting its bounded rounds, or a decision expiring under a slow
    /// sweep). The prepare then stays resident indefinitely. When it is not
    /// durably recorded - which since issue #2183 happens only when the durable
    /// ledger (issue #2165) is disabled, because a resident prepare is now
    /// recorded even past the ledger's cap - its offset clamps the incremental
    /// flush ceiling (<see cref="MinUnresolvedPrepareOffsetForPartition"/>) one
    /// below itself, so the projection
    /// checkpoint cannot advance, the checkpoint pins the coverage-gated WAL GC,
    /// and the next activation re-reads the identical prepare and banks nothing.
    /// When it is recorded, the ceiling advances but the record holds the
    /// persisted leaf row open for as long as the prepare stays resident.
    /// Either way the residue is self-perpetuating: nothing time-, count- or
    /// registry-driven
    /// removes a resident prepare, and the only removers are the two terminal
    /// paths, which by hypothesis never fire because the terminal never arrives.
    /// </para>
    /// <para>
    /// Why the decision is APPLIED and not merely consulted. Resolving the
    /// registry proves the saga's DECISION was made; it does not prove the
    /// EFFECT landed on this leaf - the prepare is resident precisely because
    /// its write has not yet been drained into <c>Entries</c>. Freeing the clamp
    /// on resolvability alone would advance the checkpoint past a prepare whose
    /// committed write was never applied, and a later cold replay resuming past
    /// that checkpoint would silently lose it. Instead this LANDS the effect:
    /// <see cref="ApplyTxCommit"/> drains the prepared bucket into <c>Entries</c>
    /// (or <see cref="ApplyTxAbort"/> discards it), and
    /// <see cref="RemovePendingTxOffsetsForTransaction"/> then releases BOTH the
    /// in-memory offset clamp and the durable ledger record. The clamp lifts as a
    /// CONSEQUENCE of the effect landing, never instead of it, so the end state
    /// matches the terminal having arrived and no acknowledged write is dropped.
    /// </para>
    /// <para>
    /// Trigger. This runs during activation replay - after pass 2 has drained
    /// every deferred terminal and before the final checkpoint reconciliation -
    /// NOT on the read path. Piggybacking resolution on a read would make the
    /// self-heal load-dependent, so a cold leaf serving no reads would never run
    /// it; running it once per activation heals on the very activations the pin
    /// is forcing.
    /// </para>
    /// <para>
    /// Idempotency. <see cref="ApplyTxCommit"/> / <see cref="ApplyTxAbort"/> are
    /// idempotent and record the txid in <c>_recentlyTerminal</c>; a real
    /// terminal that later arrives for the same saga observes no resident bucket
    /// and is a no-op redelivery (or re-asserts the already-durable committed
    /// value through the per-key backstop). A grain's turn-based scheduling means
    /// no terminal RPC interleaves with this loop.
    /// </para>
    /// <para>
    /// Retention boundary (issue #2190 design question 3). A decision whose
    /// <c>TxDecisionRetention</c> tombstone TTL has elapsed reads back as
    /// <see cref="TxStatus.Indeterminate"/> for as long as the registry still
    /// stores its row. This sweep is not a read, so it then asks for the
    /// recorded verdict (<see cref="ITxRegistryGrain.GetRecordedStatusAsync"/>,
    /// through <see cref="SelfTerminaliseFromRecordedStatusAsync"/>) and applies
    /// it; a recorded read that fails or finds no terminal row leaves the
    /// prepare resident for a later activation. Only once the row has been
    /// pruned - or at once under a zero retention - does the decision read back
    /// as <see cref="TxStatus.InFlight"/>, and such a prepare is left resident
    /// with the clamp preserved exactly as before this change: a safe no-op,
    /// never an advance on an unresolvable prepare. Because the pin forces
    /// frequent re-activation, a
    /// freshly orphaned prepare is normally resolved on its first post-decision
    /// activation, well inside the retention window.
    /// </para>
    /// <para>
    /// Registry-failure containment. The per-txid resolution is an RPC to the
    /// <see cref="ITxRegistryGrain"/>, which can time out or fault under the same
    /// load that produces the pin. A resolution failure is contained per txid and
    /// degrades to the pre-heal behaviour - the prepare stays resident, its clamp
    /// stands, and the heal is retried on a later activation - rather than
    /// failing activation, which the host would retry straight back into the same
    /// timeout and so keep the self-heal from ever running under the load it
    /// exists to clear. The containment covers only the resolution: a fault from
    /// the <see cref="ApplyTxCommit"/> / <see cref="ApplyTxAbort"/> effect is a
    /// genuine projection-correctness failure and still propagates, and
    /// cooperative cancellation is never swallowed.
    /// </para>
    /// </summary>
    private async Task SelfTerminaliseResolvedPreparesAsync(CancellationToken cancellationToken)
    {
        if (_pendingTx is null || _pendingTx.Count == 0)
            return;

        // Snapshot the resident txids first: ApplyTxCommit / ApplyTxAbort mutate
        // _pendingTx, so iterating its live key collection would throw.
        var residentTxids = new List<Guid>(_pendingTx.Keys);

        foreach (var txid in residentTxids)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Resolve the saga's decision against the per-tree registry. Only a
            // terminal decision authorises self-terminalising. InFlight - which
            // is also what a decision the registry has already pruned reads
            // back as - leaves the prepare resident and the clamp intact;
            // Indeterminate (a stored decision aged out of retention) is
            // resolved from the recorded row below.
            //
            // ResolvePendingStatusAsync issues an RPC to the ITxRegistryGrain,
            // which can time out or fault when the registry is under load - the
            // same pressure that produces this pin. Containing that fault PER
            // TXID degrades a resolution failure to the pre-heal behaviour: the
            // prepare stays resident, its flush-ceiling clamp stands, and the
            // heal is retried on a later activation. Letting it escape would
            // instead fail the whole activation, which the host retries straight
            // back into the same timeout - making the self-heal unable to run
            // under exactly the load it exists to clear, and taking a leaf that
            // previously activated-but-pinned offline. Only the RESOLUTION is
            // contained: a fault from the ApplyTxCommit / ApplyTxAbort effect
            // below is a genuine projection-correctness failure and still
            // propagates, and cooperative cancellation is never swallowed.
            TxStatus decision;
            try
            {
                decision = await ResolvePendingStatusForTerminalAsync(txid);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var logger = ResolveLogger();
                if (logger is not null && logger.IsEnabled(LogLevel.Debug))
                {
                    logger.LogDebug(
                        ex,
                        "Self-terminalise could not resolve saga '{TxId}' against the registry during activation "
                        + "replay for tree '{TreeId}'; leaving the prepare resident with its flush-ceiling clamp in "
                        + "place and retrying the heal on a later activation.",
                        txid,
                        state.State.TreeId);
                }

                continue;
            }

            switch (decision)
            {
                case TxStatus.Committed:
                    ApplyReplayTxCommit(txid);
                    break;
                case TxStatus.Aborted:
                    ApplyTxAbort(txid);
                    break;
                case TxStatus.Indeterminate:
                    // The registry holds a decision it will no longer report on
                    // the read path - typically the tombstone outlived
                    // TxDecisionRetention while this leaf was down. The read
                    // path is right to hide the key, but this sweep is not a
                    // read: the prepare is work this leaf already owns and is
                    // still holding open, and leaving it resident forever
                    // pins the flush ceiling and leaks the bucket. Ask for the
                    // recorded row explicitly.
                    //
                    // A failure here (older registry, unreachable grain) leaves
                    // the prepare exactly as it was, which is the same outcome
                    // as the resolve failure handled above, so the sweep simply
                    // retries on a later activation.
                    await SelfTerminaliseFromRecordedStatusAsync(txid);
                    break;
            }
        }
    }

    /// <summary>
    /// Retention-mask bypass for the self-terminalisation sweep: asks the
    /// registry for the physically recorded verdict behind an
    /// <see cref="TxStatus.Indeterminate"/> answer and applies it locally.
    /// Silently gives up when the registry cannot answer, leaving the prepare
    /// resident for a later sweep.
    /// </summary>
    private async ValueTask SelfTerminaliseFromRecordedStatusAsync(Guid txid)
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId)) return;

        TxStatus recorded;
        try
        {
            recorded = await TxRegistryRouting
                .GetRegistry(grainFactory, treeId, txid)
                .GetRecordedStatusAsync(txid);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var logger = ResolveLogger();
            if (logger is not null && logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug(
                    ex,
                    "Self-terminalise could not read the recorded outcome for saga '{TxId}' on tree '{TreeId}'; "
                    + "leaving the prepare resident and retrying the heal on a later activation.",
                    txid,
                    treeId);
            }

            return;
        }

        switch (recorded)
        {
            case TxStatus.Committed:
                ApplyReplayTxCommit(txid);
                break;
            case TxStatus.Aborted:
                ApplyTxAbort(txid);
                break;
        }
    }

    /// <summary>
    /// Captures a snapshot of the leaf's current pending-tx state for
    /// a scan-path read: the per-key pending entries plus a single
    /// batched call to the per-tree <see cref="ITxRegistryGrain"/>
    /// resolving every referenced txid's recorded outcome.
    /// <para>
    /// Returns empty maps in the steady-state path where the leaf has
    /// no pending-tx activity, so the scan loop's post-snapshot work
    /// degenerates to dictionary lookups against the empty
    /// <c>pendingKeys</c> map (cheap, no extra allocations beyond two
    /// empty Dictionary instances).
    /// </para>
    /// <para>
    /// On the saga-active path, makes exactly one RPC per scan
    /// regardless of how many keys the scan visits - the batched
    /// registry call collapses N per-key dial-backs into one round
    /// trip. Callers iterate the runtime entry cache as usual and,
    /// for each key found in <c>pendingKeys</c>, branch on
    /// the resolved outcome through
    /// <see cref="AtomicVisibilityGate.ResolveKey"/>:
    /// <see cref="TxStatus.Committed"/> surfaces the prepared value,
    /// <see cref="TxStatus.Indeterminate"/> hides the key, and
    /// <see cref="TxStatus.InFlight"/> / <see cref="TxStatus.Aborted"/>
    /// fall through to the pre-saga cache value. (An in-flight saga
    /// falling through rather than hiding is the strict-isolation
    /// contract: the prepared value is invisible until the registry
    /// records a commit, so the reader sees the last committed one.)
    /// </para>
    /// </summary>
    private async ValueTask<(
        Dictionary<Guid, TxStatus> outcomes,
        Dictionary<string, (Guid txid, LwwValue<byte[]> value)> pendingKeys)>
        SnapshotPendingForReadAsync()
    {
        if (_pendingTx is null || _pendingTx.Count == 0)
        {
            return (EmptyOutcomes, EmptyPendingKeys);
        }

        var txids = new List<Guid>(_pendingTx.Count);
        // Summing the per-saga bucket widths is an exact upper bound on the
        // union's key count, and an exact count in the dominant single-saga
        // case. Concurrent sagas overwhelmingly touch disjoint keys, so the
        // bound stays tight; the clamp keeps a pathologically wide pending
        // set from over-allocating this per-read map.
        var pendingBound = 0;
        foreach (var bucket in _pendingTx.Values)
            pendingBound += bucket.Count;
        var pendingKeys = new Dictionary<string, (Guid, LwwValue<byte[]>)>(
            Math.Min(pendingBound, PendingReadKeyCapacityLimit));
        // Keys covered by more than one saga's bucket. Resolved below, once the
        // outcomes are known, through AtomicVisibilityGate.SelectDecidingPrepare;
        // first-bucket-wins let a long-undecided saga shadow a committed one.
        // Captured here, before the registry await, so the selection reads the
        // same pending set the rest of this snapshot does.
        Dictionary<string, List<(Guid txid, LwwValue<byte[]> value)>>? contested = null;
        foreach (var (txid, bucket) in _pendingTx)
        {
            txids.Add(txid);
            foreach (var (key, value) in bucket)
            {
                if (pendingKeys.TryAdd(key, (txid, value)))
                    continue;

                contested ??= new Dictionary<string, List<(Guid, LwwValue<byte[]>)>>(StringComparer.Ordinal);
                if (!contested.TryGetValue(key, out var candidates))
                {
                    candidates = [pendingKeys[key]];
                    contested[key] = candidates;
                }

                candidates.Add((txid, value));
            }
        }

        // A lone bucket that the committed row already supersedes cannot decide
        // the key whatever its saga's outcome, so it is dropped before any outcome
        // is resolved; when nothing else is pending the read needs no registry
        // view at all.
        DropSupersededSinglePrepares(pendingKeys, contested);
        if (pendingKeys.Count == 0)
        {
            return (EmptyOutcomes, EmptyPendingKeys);
        }

        // Linearizable-scan fast path: when the lattice-level fan-out
        // has stamped a per-scan registry snapshot via
        // LatticeRegistrySnapshotContext, every leaf in the scan must
        // share that exact view of registry decisions - otherwise the
        // registry's InFlight→Committed transition can fall mid-fan-out
        // and produce a split observation across leaves. Use the
        // ambient and skip the per-leaf registry RPC entirely.
        // Decisions not in the ambient default to InFlight (consistent
        // with "decision not yet recorded as of this snapshot's
        // wall-clock moment").	
        var ambient = LatticeRegistrySnapshotContext.Current;
        if (ambient is not null)
        {
            var filtered = new Dictionary<Guid, TxStatus>(txids.Count);
            foreach (var t in txids)
            {
                filtered[t] = ambient.TryGetValue(t, out var s) ? s : TxStatus.InFlight;
            }
            SelectContestedPrepares(pendingKeys, contested, filtered);
            return (filtered, pendingKeys);
        }

        // Issue #3641: no single decision view exists for this fan-out, so the
        // leaf must not resolve its prepares at its own moment. Hand back the
        // unavailable sentinel; ResolveReadOutcome throws the typed exception
        // only if the read actually reaches a prepared key, so a read whose
        // range holds no prepare still completes.
        if (LatticeRegistrySnapshotContext.IsUnavailable)
        {
            SelectContestedPrepares(pendingKeys, contested, UnavailableOutcomes);
            return (UnavailableOutcomes, pendingKeys);
        }

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
        {
            // Defensive: no tree id means we cannot consult the registry, so we
            // do not know these sagas' outcomes and must not claim to. Report
            // Indeterminate, which the visibility gate hides. InFlight would
            // have been wrong for the stated intent: it falls through to the
            // pre-saga value rather than hiding, so the comment's promise to
            // keep the prepared keys hidden until activation completes its
            // tree-id stamp was not what the code did.
            var hidden = new Dictionary<Guid, TxStatus>(txids.Count);
            foreach (var t in txids) hidden[t] = TxStatus.Indeterminate;
            SelectContestedPrepares(pendingKeys, contested, hidden);
            return (hidden, pendingKeys);
        }

        Dictionary<Guid, TxStatus> outcomes;
        try
        {
            outcomes = await TxRegistryFanOut.GetStatusManyAsync(
                grainFactory, treeId, txids);
        }
        catch (Exception ex) when (TxRegistryTransportFault.IsTransportFailure(ex))
        {
            // Issue #2215: the scan-path registry fetch translates a transport
            // failure into the same typed, retryable exception as the
            // single-key path. The single-snapshot discipline is unchanged;
            // only the error type is.
            throw LatticeTransactionOutcomeUnavailableException.Create(
                treeId, key: null, pendingKeys.Count, txids, ex);
        }

        SelectContestedPrepares(pendingKeys, contested, outcomes);
        return (outcomes, pendingKeys);
    }

    /// <summary>
    /// Re-points every key covered by more than one saga's bucket at the bucket
    /// that decides its visibility under <paramref name="outcomes"/>, using
    /// <see cref="AtomicVisibilityGate.SelectDecidingPrepare"/>, and drops the key
    /// from <paramref name="pendingKeys"/> when no bucket decides, so the read
    /// serves the committed row. A no-op on the steady-state path where no key is
    /// contested, and under the <see cref="UnavailableOutcomes"/> sentinel, where
    /// the read throws on reaching any prepared key whichever bucket it holds.
    /// </summary>
    private void SelectContestedPrepares(
        Dictionary<string, (Guid txid, LwwValue<byte[]> value)> pendingKeys,
        Dictionary<string, List<(Guid txid, LwwValue<byte[]> value)>>? contested,
        Dictionary<Guid, TxStatus> outcomes)
    {
        if (contested is null || ReferenceEquals(outcomes, UnavailableOutcomes))
            return;

        var view = new TxDecisionView(outcomes);
        foreach (var (key, candidates) in contested)
        {
            var chosen = SelectDecidingCandidate(key, candidates, view);
            if (chosen < 0)
                pendingKeys.Remove(key);
            else
                pendingKeys[key] = candidates[chosen];
        }
    }

    /// <summary>
    /// Applies <see cref="AtomicVisibilityGate.SelectDecidingPrepare"/> to the
    /// buckets covering <paramref name="key"/>, resolving each saga's outcome
    /// through <paramref name="view"/>, the orphan guard through this leaf's
    /// terminal record, and supersession through the same comparison the commit
    /// drain's orphan-drain guard makes in <see cref="ApplyTxCommit"/>. Returns
    /// <c>-1</c> when no bucket decides.
    /// </summary>
    private int SelectDecidingCandidate(
        string key, List<(Guid txid, LwwValue<byte[]> value)> candidates, TxDecisionView view)
    {
        var resolved = new PreparedCandidate[candidates.Count];
        for (var i = 0; i < candidates.Count; i++)
        {
            var (txid, value) = candidates[i];
            var superseded = IsPrepareSupersededByRow(key, txid, value);
            resolved[i] = new PreparedCandidate(view.Resolve(txid), IsRecentlyTerminal(txid), superseded, value.Timestamp);
        }

        return AtomicVisibilityGate.SelectDecidingPrepare(resolved);
    }

    /// <summary>
    /// Whether this leaf's committed row for <paramref name="key"/> already
    /// supersedes saga <paramref name="txid"/>'s prepared <paramref name="value"/>,
    /// as the exact complement of the commit drain's install condition in
    /// <see cref="ApplyTxCommit"/>:
    /// <list type="bullet">
    /// <item><description>
    /// A marked prepare (issue #4522) is applied at its own stamp P only over a
    /// row stamped below P, so any row stamped at or above P supersedes it,
    /// migrated or not.
    /// </description></item>
    /// <item><description>
    /// An unmarked prepare keeps the pre-#4522 drain: a newer, non-migrated row
    /// means the drain skips the prepare, so the prepare can never become the
    /// key's visible value whatever its saga's outcome.
    /// </description></item>
    /// </list>
    /// A CRDT-delta prepare is folded into the row rather than LWW-merged, so the
    /// drain never skips it and no row supersedes it.
    /// </summary>
    private bool IsPrepareSupersededByRow(string key, Guid txid, in LwwValue<byte[]> value)
    {
        if (!Cache.TryGetRow(key, out var row))
            return false;

        if (_pendingTxDeltas is not null
            && _pendingTxDeltas.TryGetValue(txid, out var deltas)
            && deltas.ContainsKey(key))
            return false;

        if (IsPrepareStampOriginal(txid, key))
            return row.Timestamp.CompareTo(value.Timestamp) >= 0;

        if (row.IsMigrated)
            return false;

        return row.Timestamp.CompareTo(value.Timestamp) > 0;
    }

    /// <summary>
    /// Drops from <paramref name="pendingKeys"/> every key covered by a single
    /// saga's bucket that this leaf's committed row already supersedes (see
    /// <see cref="IsPrepareSupersededByRow"/>), so the read serves the row. Keys
    /// covered by more than one bucket are left to
    /// <see cref="SelectContestedPrepares"/>, which applies the same supersession
    /// through <see cref="AtomicVisibilityGate.SelectDecidingPrepare"/>.
    /// <para>
    /// A single bucket is superseded when its saga's terminal never reached this
    /// leaf while later writes to the key did: a saga parked by a silo restart
    /// after its commit was recorded, whose terminal was lost on the way here
    /// (a resize's shadow-forwarded copy, say). Resolving that lone bucket
    /// through <see cref="AtomicVisibilityGate.ResolveKey"/> surfaced its
    /// committed prepare - an older round - over the newer rows every later saga
    /// drained, so the leaf served a stale round while sibling leaves served the
    /// current one.
    /// </para>
    /// </summary>
    private void DropSupersededSinglePrepares(
        Dictionary<string, (Guid txid, LwwValue<byte[]> value)> pendingKeys,
        Dictionary<string, List<(Guid txid, LwwValue<byte[]> value)>>? contested)
    {
        List<string>? superseded = null;
        foreach (var (key, (txid, value)) in pendingKeys)
        {
            if (contested is not null && contested.ContainsKey(key))
                continue;

            if (IsPrepareSupersededByRow(key, txid, value))
                (superseded ??= []).Add(key);
        }

        if (superseded is null)
            return;

        foreach (var key in superseded)
            pendingKeys.Remove(key);
    }

    /// <summary>
    /// Single-key counterpart of <see cref="SelectContestedPrepares"/>: given the
    /// newest bucket <see cref="TryFindPendingForKey"/> found for
    /// <paramref name="key"/>, returns the bucket that decides the key's
    /// visibility when more than one saga has prepared it, together with that
    /// saga's resolved outcome. When no bucket decides, the outcome is
    /// <see cref="TxStatus.InFlight"/>, which the visibility gate resolves to the
    /// committed row. Each candidate's outcome is resolved through
    /// <see cref="ResolvePendingStatusAsync"/>, so an ambient fan-out snapshot is
    /// honoured exactly as on the single-bucket path. When only one bucket covers
    /// the key the outcome is <see langword="null"/> and the caller resolves it as
    /// before, with no allocation and no extra await - unless the committed row
    /// already supersedes that bucket, when the outcome is
    /// <see cref="TxStatus.InFlight"/> so the read serves the row.
    /// </summary>
    private async ValueTask<(Guid txid, LwwValue<byte[]> value, TxStatus? status)> SelectPendingForKeyAsync(
        string key, Guid newestTxid, LwwValue<byte[]> newestValue)
    {
        List<(Guid txid, LwwValue<byte[]> value)>? candidates = null;
        if (_pendingTx is { Count: > 1 })
        {
            foreach (var (id, bucket) in _pendingTx)
            {
                if (id == newestTxid || !bucket.TryGetValue(key, out var value))
                    continue;

                candidates ??= [(newestTxid, newestValue)];
                candidates.Add((id, value));
            }
        }

        if (candidates is null)
        {
            // A lone bucket the committed row supersedes cannot decide the key
            // (see DropSupersededSinglePrepares); InFlight resolves to the row.
            return IsPrepareSupersededByRow(key, newestTxid, newestValue)
                ? (newestTxid, newestValue, TxStatus.InFlight)
                : (newestTxid, newestValue, null);
        }

        var statuses = new Dictionary<Guid, TxStatus>(candidates.Count);
        foreach (var (txid, _) in candidates)
        {
            statuses[txid] = await ResolvePendingStatusAsync(txid, key);
        }

        var chosen = SelectDecidingCandidate(key, candidates, new TxDecisionView(statuses));
        return chosen < 0
            ? (newestTxid, newestValue, TxStatus.InFlight)
            : (candidates[chosen].txid, candidates[chosen].value, statuses[candidates[chosen].txid]);
    }
    /// <summary>
    /// Sentinel outcome map returned by <see cref="SnapshotPendingForReadAsync"/>
    /// under a <see cref="LatticeRegistrySnapshotContext.IsUnavailable"/>
    /// ambient (issue #3641). Identified by reference and never mutated.
    /// </summary>
    private static readonly Dictionary<Guid, TxStatus> UnavailableOutcomes = new(0);

    /// <summary>
    /// Resolves the scan-path outcome of <paramref name="txid"/> from the
    /// <paramref name="outcomes"/> map <see cref="SnapshotPendingForReadAsync"/>
    /// returned, through the shared <see cref="TxDecisionView"/> rule. Throws
    /// <see cref="LatticeTransactionOutcomeUnavailableException"/> when the map is
    /// the <see cref="UnavailableOutcomes"/> sentinel: the read reached a prepared
    /// key it has no single decision view to resolve against.
    /// </summary>
    private TxStatus ResolveReadOutcome(Dictionary<Guid, TxStatus> outcomes, Guid txid, int pendingKeyCount)
    {
        if (ReferenceEquals(outcomes, UnavailableOutcomes))
        {
            throw LatticeTransactionOutcomeUnavailableException.Create(
                state.State.TreeId ?? string.Empty, key: null, pendingKeyCount, [txid], null);
        }

        return new TxDecisionView(outcomes).Resolve(txid);
    }

    /// <summary>
    /// Pending-transaction count snapshot for tests. Not on any
    /// public surface.
    /// </summary>
    internal int PendingTransactionCount => _pendingTx?.Count ?? 0;

    /// <summary>
    /// Recently-terminal count snapshot for tests. Not on any
    /// public surface.
    /// </summary>
    internal int RecentlyTerminalCount => _recentlyTerminal?.Count ?? 0;

    /// <summary>
    /// Test-only seam exposing the per-partition unresolved-prepare
    /// clamp floor used by the projection-checkpoint advance gate.
    /// Returns the same value as the internal
    /// <see cref="MinUnresolvedPrepareOffsetForPartition"/>; preserved
    /// as a distinct entry-point so test assertions remain stable
    /// against a future rename of the production accessor.
    /// </summary>
    internal long? MinUnresolvedPrepareOffsetForPartitionForTest(int partition)
        => MinUnresolvedPrepareOffsetForPartition(partition);

    /// <summary>
    /// Test hook: buckets a prepared write (a tombstone when
    /// <paramref name="value"/> is <see langword="null"/>) for
    /// <paramref name="transactionId"/> the way a prepared Set or Delete does,
    /// but without <see cref="IsLatePrepareForTerminalTransactionAsync"/>, so a test
    /// can stand up an orphan bucket - a pending bucket for a transaction whose
    /// terminal this leaf has already applied. A live prepared write can no
    /// longer produce that state; activation replay still can, and the orphan
    /// read and discard guards defend it.
    /// </summary>
    internal void PlantPreparedMutationForTest(Guid transactionId, string key, byte[]? value)
    {
        var stamp = AdvanceClockOrOverride();
        AddPreparedMutation(
            transactionId,
            key,
            value is null ? LwwValue<byte[]>.Tombstone(stamp) : LwwValue<byte[]>.Create(value, stamp));
    }

    /// <summary>
    /// Returns the minimum WAL offset across every unresolved
    /// pending-tx prepare on this leaf that was recorded under
    /// <paramref name="partition"/> and whose replay work is not durably
    /// recorded, or <c>null</c> when there is no such prepare. This is the
    /// single source of the prepare clamp at both checkpoint clamp sites -
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> (which clamps
    /// the requested advance to <c>min(requested, value - 1)</c>) and the
    /// replay flush ceiling in <c>TryFlushRecoveredCeilingAsync</c> - so
    /// crash recovery never advances past a prepare it would need to re-read.
    /// O(pending-txs); returns immediately when the offset map has never been
    /// allocated (the steady state for foreground-driven leaves).
    /// <para>
    /// Both filters are load-bearing, and a whole-leaf minimum over every
    /// buffered offset is NOT an equivalent substitute (issue #2469, which
    /// removed a dead accessor of that shape):
    /// </para>
    /// <list type="bullet">
    /// <item><description><b>Per partition.</b> WAL partitions have disjoint
    /// offset spaces, so an unresolved prepare on partition <c>Q</c> says
    /// nothing about partition <c>P</c>. A cross-partition minimum would pin
    /// every partition's checkpoint behind one partition's in-flight saga.
    /// </description></item>
    /// <item><description><b>Durably recorded prepares are skipped (issue
    /// #2165).</b> A prepare whose mutation is in the durable replay-work
    /// ledger no longer needs a WAL re-read to rebuild the pending-tx map, so
    /// clamping on it only produces a self-perpetuating checkpoint pin: the
    /// checkpoint cannot advance, it pins the coverage-gated WAL GC, and the
    /// next activation re-reads the identical prepare and banks nothing.
    /// </description></item>
    /// </list>
    /// </summary>
    internal long? MinUnresolvedPrepareOffsetForPartition(int partition)
    {
        if (_pendingTxOffsets is null || _pendingTxOffsets.Count == 0)
            return null;

        long min = long.MaxValue;
        var seen = false;
        foreach (var ((_, p), offset) in _pendingTxOffsets)
        {
            if (p != partition)
                continue;

            // Issue #2165. A prepare whose mutation is durably recorded no
            // longer requires a re-read to rebuild _pendingTx, so it must not
            // clamp the checkpoint. Skipping it here covers BOTH clamp sites -
            // the pass-1 ceiling in TryFlushRecoveredCeilingAsync and the
            // independent clamp inside SetCheckpointOffsetAsync - so the two
            // cannot disagree about which prepares are covered and the fix
            // cannot be silently undone by the second one.
            if (IsUnresolvedReplayWorkRecorded(p, offset))
                continue;

            seen = true;
            if (offset < min)
                min = offset;
        }
        return seen ? min : null;
    }

    /// <summary>
    /// Removes every per-partition pending-tx offset entry recorded
    /// under <paramref name="transactionId"/>. Called from
    /// <see cref="ApplyTxCommit"/> / <see cref="ApplyTxAbort"/> at
    /// terminal-replay time. A single saga whose per-key writes hashed
    /// across multiple WAL partitions has one entry per partition;
    /// this helper removes all of them so the per-partition clamp
    /// frees up correctly on every partition once the saga terminates.
    /// </summary>
    private void RemovePendingTxOffsetsForTransaction(Guid transactionId)
    {
        // Issue #2165. The saga's durable replay records are released at the
        // same moment its in-memory clamp is, and unconditionally: the records
        // outlive the activation that wrote them, so a terminal replaying in a
        // LATER activation finds no _pendingTxOffsets entry to remove yet must
        // still clear the ledger. Returning early on an empty offset map would
        // strand the record forever and leak the leaf's state row.
        ResolveUnresolvedReplayWorkForTransaction(transactionId);

        if (_pendingTxOffsets is null || _pendingTxOffsets.Count == 0)
            return;
        List<(Guid, int)>? toRemove = null;
        foreach (var key in _pendingTxOffsets.Keys)
        {
            if (key.TransactionId == transactionId)
            {
                (toRemove ??= new List<(Guid, int)>()).Add(key);
            }
        }
        if (toRemove is null)
            return;
        foreach (var key in toRemove)
        {
            _pendingTxOffsets.Remove(key);
        }
    }

    /// <inheritdoc />
    public async Task<List<string>> GetPendingKeysAsync()
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        if (_pendingTx is null || _pendingTx.Count == 0)
            return new List<string>();

        // De-duplicate keys across pending tx buckets - two independent
        // sagas could (rarely) prepare the same key. Set is then
        // materialised into a List for the wire shape.
        var unique = new HashSet<string>(StringComparer.Ordinal);
        foreach (var bucket in _pendingTx.Values)
        {
            foreach (var key in bucket.Keys)
                unique.Add(key);
        }
        return new List<string>(unique);
    }

    /// <inheritdoc />
    public async Task<List<PendingMutationSnapshot>> GetPendingMutationsForSlotsAsync(int[] sortedMovedSlots, int virtualShardCount)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        ArgumentNullException.ThrowIfNull(sortedMovedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        // Steady-state fast path: no pending bucket (the vast majority
        // of leaves never participate in a saga) or an empty
        // moved-slots array means no work to do. Return an empty list
        // without allocating any further state.
        if (_pendingTx is null || _pendingTx.Count == 0 || sortedMovedSlots.Length == 0)
            return new List<PendingMutationSnapshot>();

        var result = new List<PendingMutationSnapshot>();
        foreach (var (txid, bucket) in _pendingTx)
        {
            // Per-saga WAL offset (if any) for the snapshot's
            // WalOffset field. Foreground commits leave this map
            // untouched; the value is surfaced for diagnostics only
            // and is 0 when unstamped. Under multi-partition replay a
            // saga can register one offset per partition; for the
            // diagnostic field we report the minimum across all
            // recorded partitions for the saga (the earliest WAL
            // observation of the saga's prepare on this leaf).
            long walOffset = 0;
            if (_pendingTxOffsets is not null)
            {
                long min = long.MaxValue;
                var seen = false;
                foreach (var ((tx, _), offset) in _pendingTxOffsets)
                {
                    if (tx != txid)
                        continue;
                    seen = true;
                    if (offset < min)
                        min = offset;
                }
                if (seen)
                    walOffset = min;
            }

            foreach (var (key, value) in bucket)
            {
                var slot = ShardMap.GetVirtualSlot(key, virtualShardCount);
                if (Array.BinarySearch(sortedMovedSlots, slot) < 0)
                    continue;

                // Carry the prepared mutation's typed CRDT delta + merge
                // mode (when the parallel side-map recorded one) so the
                // destination leaf's retroactive replay reconstructs the
                // fold state and the resharded prepared CRDT entry still
                // converges by the per-replica union on its terminal
                // commit. A plain LWW prepared write has no side-map entry
                // and snapshots Delta=null / Mode=LwwRegister.
                byte[]? delta = null;
                var mode = LatticeMergeMode.LwwRegister;
                if (_pendingTxDeltas is not null
                    && _pendingTxDeltas.TryGetValue(txid, out var deltaBucket)
                    && deltaBucket.TryGetValue(key, out var dm))
                {
                    delta = dm.Delta;
                    mode = dm.Mode;
                }

                var membership = _pendingTxBatches is not null
                    && _pendingTxBatches.TryGetValue(txid, out var batchBucket)
                    && batchBucket.TryGetValue(key, out var b)
                    ? b
                    : default;

                result.Add(new PendingMutationSnapshot
                {
                    TransactionId = txid,
                    Key = key,
                    Value = value.IsTombstone ? null : value.Value,
                    Timestamp = value.Timestamp,
                    IsTombstone = value.IsTombstone,
                    ExpiresAtTicks = value.ExpiresAtTicks,
                    OriginClusterId = value.OriginClusterId,
                    VectorClock = value.VectorClock,
                    WalOffset = walOffset,
                    Delta = delta,
                    Mode = mode,
                    AtomicBatchSize = membership.Size,
                    AtomicBatchIndex = membership.Index,
                    StampIsOriginal = IsPrepareStampOriginal(txid, key),
                });
            }
        }

        return result;
    }

    /// <summary>
    /// Whether a stranded prepared value with no committed-values payload can
    /// be re-delivered to the leaf that declares its key through the
    /// cross-migration backstop. A tombstone or an expiring value cannot be
    /// expressed that way, so such a key keeps the pre-#4335 local drain. A
    /// CRDT-delta prepare can: its bucketed value is the staged full state, and
    /// the declaring leaf, which inherits this leaf's tree binding and so resolves
    /// the same merge mode, joins it into its row (issue #4611). A delta is
    /// recorded only when that mode resolves to a CRDT.
    /// </summary>
    private static bool IsForwardablePreparedValue(in LwwValue<byte[]> prepared) =>
        prepared.Value is not null && !prepared.IsTombstone && prepared.ExpiresAtTicks == 0;

    /// <inheritdoc />
    public async Task ApplyTxTerminalAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues = null)
    {
        await AwaitReplayBarrierAsync();

        if (transactionId == Guid.Empty)
            return;

#if LATTICE_DIAG
        // DIAG terminal-leaf-apply: fires at the very entry of the
        // per-leaf terminal handler, BEFORE the _recentlyTerminal dedup
        // and the pending/backstop path selection. Pairs with the
        // shard-side terminal-recv emission so the saga's full fan-out
        // ordering (per-leaf wall-clock timing, dedup state, backstop
        // payload presence) can be reconstructed from the trace.
        var diagHadPending = _pendingTx is not null && _pendingTx.ContainsKey(transactionId);
        var diagAlreadyFlipped = _recentlyTerminal is not null && _recentlyTerminal.Contains(transactionId);
        DiagSink.Write($"[DIAG terminal-leaf-apply] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} committed={committed} hadPending={diagHadPending} alreadyFlipped={diagAlreadyFlipped} committedValuesCount={committedValues?.Count ?? 0} committedKeys=[{(committedValues is null ? "<null>" : string.Join(",", committedValues.Keys))}]");
#endif

        // Capture the bucket reference up-front. ApplyTxCommit/ApplyTxAbort
        // remove the bucket from _pendingTx, so we need the snapshot here to
        // compute the per-key backstop set below before the flip path mutates
        // _pendingTx. The reference into the bucket dictionary remains valid
        // after Remove (we only need to read its keys).
        Dictionary<string, LwwValue<byte[]>>? bucket = null;
        if (_pendingTx is not null && _pendingTx.TryGetValue(transactionId, out var existingBucket))
            bucket = existingBucket;
        var hadPending = bucket is not null;

        var alreadyFlipped = _recentlyTerminal is not null && _recentlyTerminal.Contains(transactionId);

        // Per-key backstop set: every key in committedValues that is
        // (a) NOT already covered by this leaf's pending bucket (the
        // pending-flip path will surface those values), AND
        // (b) NOT already backstopped under this transaction id by a
        // prior terminal delivery (per-key dedup, not per-saga).
        //
        // Per-key dedup is load-bearing: two terminal deliveries to the
        // same leaf can legitimately carry DIFFERENT committedValues
        // subsets - the AtomicWriteGrain direct fan-out routes by
        // current-routing per shard, while the saga's transitive
        // split-forward fan-out (TerminalFanOutResolver) reaches the
        // same destination via the source shard's earlier
        // MovedAwaySlots migration record. A per-saga dedup observes
        // one subset first, marks the saga backstopped, and short-
        // circuits the OTHER subset's missing keys - leaving them
        // stuck at the drained pre-saga value. The chaos pattern
        // `split (pre=5, post=11)` on the reshard fixture reproduces
        // this exactly: 5 keys (one source shard's worth) orphaned
        // because their backstop arrived after another shard's subset
        // already poisoned the txid's dedup marker.
        List<KeyValuePair<string, byte[]>>? missingKeys = null;
        var hasBackstopPayload = committed && committedValues is { Count: > 0 };
        HashSet<string>? alreadyBackstoppedKeys = null;
        if (_backstoppedTerminals is not null && (hasBackstopPayload || hadPending))
            _backstoppedTerminals.TryGetValue(transactionId, out alreadyBackstoppedKeys);

        // Stranded prepared keys: bucket keys this leaf no longer declares
        // because a leaf split narrowed its span after the prepare landed.
        // The split moves only committed rows to the sibling and leaves the
        // donor's bucket in place, so draining those keys here would store
        // them outside the donor's span - a second row for the key in the
        // shard's chain (over-count) that also breaks the ordered chain walk
        // a scan resumes over (missed keys). They are excluded from the local
        // drain and re-routed as a backstop to the leaf that declares them,
        // which is also what replay does: a prepare outside the declared
        // span is never bucketed on replay (issue #4335).
        HashSet<string>? strandedPrepared = null;
        if (hadPending && committed && !alreadyFlipped && HasDeclaredSpan)
        {
            foreach (var (key, prepared) in bucket!)
            {
                if (DeclaresKey(key))
                    continue;
                var hasCommittedValue = committedValues is not null && committedValues.ContainsKey(key);
                if (!hasCommittedValue && !IsForwardablePreparedValue(prepared))
                    continue;
                (strandedPrepared ??= new HashSet<string>(StringComparer.Ordinal)).Add(key);
            }
        }

        if (hasBackstopPayload)
        {
            foreach (var kvp in committedValues!)
            {
                if (bucket is not null && bucket.ContainsKey(kvp.Key)
                    && (strandedPrepared is null || !strandedPrepared.Contains(kvp.Key)))
                    continue;
                if (alreadyBackstoppedKeys is not null && alreadyBackstoppedKeys.Contains(kvp.Key))
                    continue;
                if (alreadyFlipped && IsRowNewerThanTerminalLanding(transactionId, kvp.Key))
                    continue;
                (missingKeys ??= []).Add(kvp);
            }
        }

        if (strandedPrepared is not null)
        {
            foreach (var key in strandedPrepared)
            {
                if (committedValues is not null && committedValues.ContainsKey(key))
                    continue;
                if (alreadyBackstoppedKeys is not null && alreadyBackstoppedKeys.Contains(key))
                    continue;
                (missingKeys ??= []).Add(new KeyValuePair<string, byte[]>(key, bucket![key].Value!));
            }
        }

        // Issue #4522: the original prepare stamp of each backstop key that has
        // one - carried by the terminal delivery, or, for a stranded prepared
        // key, the marked bucket's own stamp. Such a key is applied under
        // last-writer-wins at that stamp, so a write acknowledged after the
        // prepare survives; a key without one keeps the pre-#4522 fresh stamp.
        // Captured before the drain below discards the bucket's classification.
        var missingStamps = CollectBackstopOriginalStamps(transactionId, missingKeys, bucket, strandedPrepared, out var missingMigrated);

        // Hot-path short-circuit: a duplicate terminal delivery with
        // nothing new to do. The flip side already ran (alreadyFlipped),
        // and either there is no backstop payload, or every payload key
        // is already covered (in the bucket - which is null on the
        // alreadyFlipped path - or in the per-key backstopped set).
        if (MigrationTerminalCore.IsNoOpRedelivery(alreadyFlipped, hadPending, missingKeys is not null))
            return;

        // Span admission (issue #4335): a missing key this leaf does not
        // declare - routed here by a descent that predates a split's
        // separator, or a stranded prepared key - is re-delivered as a
        // backstop to the leaf that declares it rather than stored here,
        // where it would duplicate that leaf's row in the shard's chain.
        Dictionary<GrainId, Dictionary<string, byte[]>>? forwarded = null;
        if (missingKeys is { Count: > 0 } && HasDeclaredSpan)
        {
            List<KeyValuePair<string, byte[]>>? local = null;
            foreach (var kvp in missingKeys)
            {
                if (TryResolveSpanForwardTarget(kvp.Key, out var target, out var failOpen))
                {
                    forwarded ??= new Dictionary<GrainId, Dictionary<string, byte[]>>();
                    if (!forwarded.TryGetValue(target, out var subset))
                    {
                        subset = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                        forwarded[target] = subset;
                    }
                    subset[kvp.Key] = kvp.Value;
                    continue;
                }

                if (failOpen != SpanFailOpenReason.None)
                    RecordSpanFailOpenCommit(failOpen, SpanWriteOrigin.Merge);
                (local ??= []).Add(kvp);
            }

            if (forwarded is not null)
                missingKeys = local;
        }

        // Forwarded before any local state changes, so a failed forward
        // fails this delivery whole and its retry recomputes the same set.
        // Each target applies the subset through this same backstop path,
        // which dedups per key, and the chain invariant keeps every hop
        // moving away from this leaf.
        if (forwarded is not null)
        {
            var forwards = new Task[forwarded.Count];
            var f = 0;
            foreach (var (target, subset) in forwarded)
            {
                // Carry each forwarded key's original stamp (and only those),
                // so the declaring leaf applies it at that stamp too.
                using (LatticeOriginalPrepareStampContext.With(SelectStamps(missingStamps, subset.Keys)))
                {
                    forwards[f++] = grainFactory.GetGrain<IBPlusLeafGrain>(target).ApplyTxTerminalAsync(transactionId, committed: true, subset);
                }
            }
            await Task.WhenAll(forwards);
        }

        // Pending-flip path: drain the bucket into Entries (commit) or
        // drop it without surfacing (abort). Zero leaf I/O - the WAL is
        // the recovery source for the flipped entries.
        //
        // Three sub-paths based on `alreadyFlipped`:
        //
        // (1) `!alreadyFlipped, committed`: normal commit. Drain the
        //     bucket into Entries via ApplyTxCommit. Tick Version so
        //     a co-located LeafCacheGrain notices the new saga state.
        //
        // (2) `!alreadyFlipped, !committed`: normal abort. Discard the
        //     bucket via ApplyTxAbort without surfacing prepared values.
        //
        // (3) `alreadyFlipped` (either commit or abort): the saga's
        //     terminal has ALREADY landed on this leaf, having written
        //     the correct values via flip-drain or per-key backstop.
        //     A bucket present now means a TreeShardSplitGrain
        //     retroactive sweep replayed a source-leaf prepare snapshot
        //     to this destination AFTER the saga's commit broadcast had
        //     already reached it (via BFS-with-fullBackstop through the
        //     source's MovedAwaySlots). The orphan bucket carries the
        //     PREPARE-TIME value, which can be many saga rounds older
        //     than the current Entries[K] state. Draining it would
        //     stamp a stale value with a fresh HLC tick, causing
        //     readers to observe an old saga's value in place of the
        //     current one (the chaos signature `unknown-round
        //     (other=N)` reproduces this exactly). The correct action
        //     is to DISCARD the orphan bucket without surfacing - the
        //     original terminal's backstop already wrote the correct
        //     value, so the bucket is pure dead weight that would
        //     otherwise pin the pending-key read-path until the txid
        //     was evicted from the registry retention window.
        if (hadPending)
        {
            // Bucket disposition on terminal delivery is the shared, dependency-
            // free MigrationTerminalCore rule so the production apply path and the
            // Coyote reshard model execute one identical decision (see #1591).
            // DiscardOrphan (terminal already landed) is the write-side orphan
            // guard; DiscardAborted is a normal abort; both discard the bucket.
            var bucketAction = MigrationTerminalCore.DecideBucketAction(hadPending, alreadyFlipped, committed);
            if (bucketAction is MigrationTerminalBucketAction.DiscardOrphan
                or MigrationTerminalBucketAction.DiscardAborted)
            {
                ApplyTxAbort(transactionId);
            }
            else if (bucketAction == MigrationTerminalBucketAction.DrainCommit)
            {
                // Publish Version[ReplicaId] as the *pre-drain* Clock
                // value, then let ApplyTxCommit's counter-only bump push
                // Clock (and every drained Entries[K].Timestamp) one
                // counter unit ahead. The LeafCacheGrain stores
                // Version[ReplicaId] as its saved callerClock on each
                // refresh and excludes entries whose Timestamp is <=
                // callerClock from the next delta - so Version[ReplicaId]
                // must be strictly less than every drained Timestamp for
                // the just-flipped saga's values to be delivered.
                //
                // The previous shape - state.State.Version.Tick(ReplicaId)
                // BEFORE ApplyTxCommit - read DateTimeOffset.UtcNow.Ticks
                // and pumped Version[ReplicaId] forward to wall-clock-now,
                // while state.State.Clock only advanced via
                // AdvanceProjectionClock at prepare time. After enough
                // saga commits the two clocks drifted by tens of
                // milliseconds (Version ahead of Clock), and the cache's
                // `lww.Timestamp > callerClock` filter silently dropped
                // the drained values on every refresh - manifesting as
                // the chaos-test "unknown-round (other=N)" stale-cache
                // signature on Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard.
                //
                // Foreground-only: the replay path inherits the
                // ILeafProjection.Apply convention of not advancing
                // Version, so this branch is skipped on replay and the
                // foreground/replay symmetry holds because every drained
                // Entries[K].Timestamp is reconstructed bit-identically
                // from the same counter-only bump (see the matching
                // comment block in ApplyTxCommit).
                ApplyTxCommit(transactionId, strandedPrepared);
                // Publish state.State.Clock AFTER the commit: ApplyTxCommit
                // does the counter-only bump that lifts state.State.Clock
                // (and every drained Entries[K].Timestamp) one counter unit
                // ahead of the pre-publish snapshot. Publishing the
                // post-commit Clock keeps Version[ReplicaId] equal to the
                // highest stamp any drained entry actually carries - which
                // is exactly the cache filter's reference value.
                var postCommitVersion = state.State.Clock;
                if (postCommitVersion.CompareTo(state.State.Version.GetClock(ReplicaId)) > 0)
                {
                    state.State.Version.Entries[ReplicaId] = postCommitVersion;
                }
            }
            else
            {
                ApplyTxAbort(transactionId);
            }
        }

        // Per-key cross-migration LWW backstop. Fires on the commit
        // path for every committedValues key that the bucket did not
        // cover and the per-key dedup set did not already cover.
        // Stamp every backstop entry with the SAME Tick(state.State.Clock)
        // value: HLC.Tick guarantees strict-greater ordering against any
        // pre-saga drained value already in Entries, so LWW.Merge
        // resolves in favour of the backstop.
        //
        // After the loop we MUST publish the backstop stamp into
        // Version[ReplicaId] via PublishVersionAdvance(stamp) (see
        // below). An earlier shape skipped the publication on the
        // hadPending=false branch on the theory that "the cache is not
        // tracking this leaf as a pending source for this saga." That
        // reasoning was incorrect: the same-tree LeafCacheGrain is the
        // primary read path for every key in the runtime entry cache,
        // and its RefreshAsync fast path (GetDeltaSinceAsync ->
        // DominatesOrEquals) short-circuits when the cache's saved
        // _version equals Version[ReplicaId]. If the backstop write
        // does not lift Version[ReplicaId], the cache continues
        // serving the previous value (commonly a freshly-imported
        // IsMigrated=true pre-saga snapshot) indefinitely. A backstop terminal landing on a destination
        // leaf whose only prior write was a cross-leaf migration
        // import never advanced Version, so the cache pinned the
        // migrated pre-saga value through every subsequent saga
        // round.
        //
        // Publication is safe under concurrent reads because
        // PublishVersionAdvance is a strict-greater-only conditional
        // assignment of a single dictionary entry (no allocation, no
        // structural mutation of the dictionary's shape); concurrent
        // dictionary reads on the keyed slot see either the old or
        // the new value, both of which are correctness-preserving
        // (the cache's filter is monotone in either direction).
        //
        // Each missing-key write is durably committed by appending a
        // LatticeMutation { Kind = Set, IsBackstop = true, ... } to the
        // per-shard WAL via ICommitLogWriter - the same primitive every
        // other foreground commit on this leaf uses under the
        // WAL-as-sole-commit-point invariant. The WAL append is the
        // durability point; the in-memory projection update
        // (StoreEntry) happens immediately after under the same shared
        // HLC tick so a co-located reader sees the value before the
        // next dequeue. Crash recovery rebuilds Entries from the WAL
        // via the per-shard activation-time replay path. The legacy
        // standalone state-row persist that used to follow this loop
        // is gone - every leaf foreground commit now obeys the
        // WAL-as-sole-commit-point invariant.
        if (missingKeys is { Count: > 0 })
        {
            // Cross-shard-migration LWW dominance (Fix M, backstop variant).
            // See the foreground-drain branch above for the full rationale.
            // The same race that affects the pending-flip restamp affects
            // the pure-backstop path: a destination leaf whose freshly-
            // minted state.State.Clock is below the migrated value's HLC
            // would stamp the backstop write with Tick(state.State.Clock),
            // which the LWW.Merge inside StoreEntry resolves AGAINST when
            // the existing Entries[K] carries a higher HLC from migration.
            // Pre-advance baseClock past any existing entry for the
            // missing keys before Ticking so the backstop strictly
            // dominates the migrated pre-saga value.
            // Issue #4611: on a tree whose merge mode resolves to a CRDT, a
            // backstop value is a full CRDT state computed from a stage-time
            // snapshot, so it is joined into the row rather than installed
            // last-writer-wins, which would discard every contribution the row
            // gained after that snapshot. The drain folds the delta under the
            // same condition. Every such key is stamped above its row below.
            var backstopMode = ResolveMergeMode();
            var joinCrdtState = backstopMode != LatticeMergeMode.LwwRegister;
            var baseClock = state.State.Clock;
            foreach (var kvp in missingKeys)
            {
                if (!joinCrdtState && missingStamps is not null && missingStamps.ContainsKey(kvp.Key))
                    continue;
                if (Cache.TryGetRow(kvp.Key, out var preExisting)
                    && preExisting.Timestamp.CompareTo(baseClock) > 0)
                {
                    baseClock = preExisting.Timestamp;
                }
            }
            var stamp = Orleans.Lattice.HybridLogicalClock.Tick(baseClock);
            var anyFreshStamp = false;
            var origin = LatticeOriginContext.Current;
            var vc = LatticeVectorClockContext.Current;
            var writer = ResolveCommitLogWriter();
            var treeId = state.State.TreeId ?? string.Empty;
            var shardIndex = state.State.ShardIndex ?? 0;
            var maintenance = LatticeMaintenanceContext.Current;

            foreach (var kvp in missingKeys)
            {
                // Issue #4522 rule (d): a key with an original prepare stamp P is
                // installed only over no row or a row stamped below P, and AT P.
                // A row at or above P is a write acknowledged after the prepare
                // and stands; the key is still recorded as backstopped below.
                var keyStamp = stamp;
                var migrated = false;
                var installed = kvp.Value;
                if (joinCrdtState)
                {
                    // A join cannot overwrite a later write, so no original stamp
                    // applies; it is stored at the fresh stamp, which strictly
                    // dominates the row it already contains (StoreEntry is LWW).
                    installed = JoinCrdtStateIntoRow(kvp.Key, backstopMode, kvp.Value);
                    anyFreshStamp = true;
                }
                else if (missingStamps is not null && missingStamps.TryGetValue(kvp.Key, out var originalStamp))
                {
                    if (IsRowAtOrAboveOriginalStamp(kvp.Key, originalStamp))
                        continue;
                    keyStamp = originalStamp;
                    migrated = missingMigrated is not null && missingMigrated.Contains(kvp.Key);
                    state.State.Clock = Orleans.Lattice.HybridLogicalClock.Merge(state.State.Clock, originalStamp);
                }
                else
                {
                    anyFreshStamp = true;
                }

                if (writer is not null)
                {
                    var entry = new WalRecord
                    {
                        TreeId = treeId,
                        Op = MutationKind.Set,
                        Key = kvp.Key,
                        Value = installed,
                        Timestamp = keyStamp,
                        IsTombstone = false,
                        ExpiresAtTicks = 0,
                        OriginClusterId = origin,
                        VectorClock = vc,
                        TransactionId = transactionId,
                        Category = maintenance,
                        IsPrepared = false,
                        IsBackstop = true,
                        ShardIndex = shardIndex,
                        IsMigrated = migrated,
                        // A joined state carries no delta, so the encoder keeps its
                        // Value and replay installs the joined state at this stamp.
                        Mode = joinCrdtState ? backstopMode : LatticeMergeMode.LwwRegister,
                    };

                    // Emit the WAL append on the LeafWriteDuration
                    // histogram tagged `kind=backstop` so operators can
                    // size cross-migration backstop traffic against
                    // ordinary writes on the same instrument. The tag
                    // dimension is additive - emissions on this
                    // histogram from the projection-checkpoint flush
                    // path carry no `kind` tag and remain
                    // distinguishable as the steady-state state-row
                    // path (now scoped to projection-checkpoint flushes
                    // only).
                    var walStartTicks = Stopwatch.GetTimestamp();
                    try
                    {
                        await writer.AppendAsync(entry);
                    }
                    finally
                    {
                        var elapsedMs = (Stopwatch.GetTimestamp() - walStartTicks) * 1000.0 / Stopwatch.Frequency;
                        LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, MetricTreeId),
                            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "backstop"),
                            LatticeTenantLabel.ForTree(treeId));
                    }
                }

                var value = new Primitives.LwwValue<byte[]>
                {
                    Value = installed,
                    Timestamp = keyStamp,
                    OriginClusterId = origin,
                    VectorClock = vc,
                    IsMigrated = migrated,
                };
                StoreEntry(kvp.Key, value);
                if (joinCrdtState)
                    Cache.SetMergeMode(kvp.Key, backstopMode);
                // A backstop at a fresh stamp, or at an original stamp minted
                // on this shard, is a non-migration write: any prior
                // migration-provenance marker for this key is now stale and
                // must be cleared so a subsequent saga's orphan-drain guard
                // does not mistake it for a migration import. One stored at
                // an original stamp carried from another shard is on that
                // shard's clock lineage, so it stays migrated and a later
                // migration import competes with it by last-writer-wins
                // (issue #4564).
            }

            if (anyFreshStamp)
                AdvanceProjectionClock(stamp);
            // Lift Version[ReplicaId] to the backstop stamp so the
            // co-located LeafCacheGrain's next RefreshAsync observes
            // a non-empty delta containing the just-stamped backstop
            // entries. Without this the cache's DominatesOrEquals
            // fast path returns the empty singleton and the cache
            // serves whatever value was in _cache before the backstop
            // (commonly an IsMigrated=true pre-saga snapshot from an
            // earlier cross-leaf migration import) indefinitely. See
            // the multi-paragraph rationale block at the head of the
            // backstop branch above.
            //
            // The hadPending=true commit branch already published
            // state.State.Clock post-commit; re-publishing here is
            // additive (the guard is strict-greater) and load-bearing
            // when the same terminal carries BOTH a flippable bucket
            // AND missing-key backstops (the bucket flip publishes
            // the post-flip Clock, but the backstop is stamped AFTER
            // and produces a strictly-greater stamp).
            var publishedStamp = anyFreshStamp ? stamp : state.State.Clock;
            if (publishedStamp.CompareTo(state.State.Version.GetClock(ReplicaId)) > 0)
            {
                state.State.Version.Entries[ReplicaId] = publishedStamp;
            }
            BumpLocalRevision();

            // Record the keys we just backstopped so a SUBSEQUENT
            // delivery (carrying possibly a different subset) skips
            // these via the alreadyBackstoppedKeys check above without
            // re-stamping Entries. Per-key dedup is the load-bearing
            // invariant - a per-txid marker would short-circuit a
            // legitimate sibling subset arriving later.
            _backstoppedTerminals ??= new Dictionary<Guid, HashSet<string>>();
            if (!_backstoppedTerminals.TryGetValue(transactionId, out var perTxBackstopped))
            {
                perTxBackstopped = new HashSet<string>(StringComparer.Ordinal);
                _backstoppedTerminals[transactionId] = perTxBackstopped;
            }
            foreach (var kvp in missingKeys)
            {
                perTxBackstopped.Add(kvp.Key);
                // A backstop that carried the prepare's stamp P installed the key
                // at P, which the read gate's self-check recognises; only a
                // backstop without one needs the witness (issue #4545).
                if (missingStamps is null || !missingStamps.ContainsKey(kvp.Key))
                    RecordTerminalWitness(transactionId, kvp.Key);
            }
        }

        // Mark the saga's pending-flip dedup. _backstoppedTerminals is
        // populated above only when a backstop write actually landed,
        // keyed per-key so future deliveries with different subsets
        // continue to do real work for keys they haven't covered yet.
        (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
        RecordTerminalLanded(transactionId);

        // Clear any destination-side shadow marker installed by the
        // split coordinator for this saga. Once the terminal has been
        // applied here, Entries[K] reflects the authoritative
        // post-saga state (drained pending, backstopped commit, or
        // unchanged on abort), so the migrated-entry guard in the
        // read path has nothing left to gate. The clear is unconditional
        // on the (committed, aborted) axis - both terminate the saga's
        // visibility window for this leaf, and any shadow marker
        // installed for a different saga's txid is untouched.
        ClearSagaShadow(transactionId);

        // Forward the projection-hash delta from any drained pending
        // bucket and / or per-key backstop writes to the parent
        // internal node so the chained subtree fold stays current.
        // No-op when this terminal landed on the abort path or
        // alreadyFlipped short-circuit branch (no StoreEntry calls
        // ran, so _digestDirty is still false). Saga terminal is a
        // structural event - bypass the c2-xxviii coalescing window.
        await PublishDigestUpwardInlineAsync();
    }

    /// <summary>
    /// Destination-side shadow markers installed by the split
    /// coordinator naming, for each key whose virtual slot is
    /// migrating into this leaf, the in-flight source-side sagas
    /// whose prepared mutations touched that key. The read path
    /// consults this map whenever it is about to surface an
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.IsMigrated"/>=<c>true</c> value, and
    /// raises <see cref="StaleShardRoutingException"/> for any
    /// shadowing saga that the registry has flipped to
    /// <see cref="TxStatus.Committed"/> but whose backstop terminal
    /// has not yet reached this leaf - so the
    /// <c>LatticeGrain</c> deadline-bounded retry loop re-fans once
    /// the backstop arrives. In-flight and aborted sagas are
    /// strict-isolation-correct on the migrated pre-saga value and
    /// pass through.
    /// <para>
    /// Lazily allocated - the steady-state path (no active split
    /// touching this leaf) leaves it null and incurs zero overhead
    /// in the read hot path beyond a single null check. Cleared
    /// per-saga by <see cref="ApplyTxTerminalAsync"/> when the
    /// saga's terminal lands on this leaf, so the per-saga footprint
    /// is bounded by saga lifetime.
    /// </para>
    /// </summary>
    private Dictionary<string, HashSet<Guid>>? _shadowedSagas;

    /// <summary>
    /// The marked original prepare stamp P of each shadow marker that was
    /// installed with one, per key then per saga (issue #4545). A marker absent
    /// from this map has no known P and keeps the pre-#4545 read gate; one
    /// present here is released by the read gate once the row it guards is
    /// stamped at or above P (<see cref="ShadowedMigrationReadGuard.RowIncorporatesMarkedPrepare"/>),
    /// so a marker this leaf will never see a terminal for - carried here by a
    /// leaf split, or installed after a reactivation forgot the terminal - can
    /// no longer gate the key until the decision ages out. Activation-scoped and
    /// cleared alongside <see cref="_shadowedSagas"/>.
    /// </summary>
    private Dictionary<string, Dictionary<Guid, HybridLogicalClock>>? _shadowMarkerStamps;

    /// <inheritdoc />
    public async Task MarkSagaShadowAsync(Guid transactionId, IReadOnlyList<string> keys)
    {
        await AwaitReplayBarrierAsync();

        ArgumentNullException.ThrowIfNull(keys);
        if (transactionId == Guid.Empty)
            throw new ArgumentException("Transaction id must be non-empty.", nameof(transactionId));

        if (keys.Count == 0)
            return;

        // A marker for a key this leaf's terminal for the saga already settled
        // guards nothing here, and it would no longer be cleared: the terminal
        // that clears it has come and gone. Left in place it is copied by the
        // next leaf split onto a sibling that never sees the terminal, where it
        // gates the key until the decision ages out (issue #4545). The witness
        // is per key and durable, so this holds across a reactivation and on a
        // sibling that inherited it; a key of the same saga whose committed
        // value has not reached this leaf yet is still marked.
        var carriesStamps = LatticeOriginalPrepareStampContext.HasStamps;
        await EnsureTerminalWitnessHydratedAsync();
        foreach (var key in keys)
        {
            if (string.IsNullOrEmpty(key) || IsTerminalWitnessed(transactionId, key))
                continue;
            _shadowedSagas ??= new Dictionary<string, HashSet<Guid>>(StringComparer.Ordinal);
            if (!_shadowedSagas.TryGetValue(key, out var sagas))
            {
                sagas = new HashSet<Guid>();
                _shadowedSagas[key] = sagas;
            }
            sagas.Add(transactionId);

            if (carriesStamps && LatticeOriginalPrepareStampContext.TryGetStamp(key, out var prepareStamp))
            {
                RecordShadowMarkerStamp(key, transactionId, prepareStamp);
            }
        }
    }

    /// <summary>
    /// Records the marked prepare stamp <paramref name="prepareStamp"/> for the
    /// marker on <paramref name="key"/> under <paramref name="transactionId"/>,
    /// and merges this leaf's clock past it, so every write this leaf
    /// acknowledges from now on is stamped above it (property H) and releases
    /// the marker. Two installs naming different stamps keep the higher one,
    /// which can only make the release later.
    /// </summary>
    private void RecordShadowMarkerStamp(string key, Guid transactionId, HybridLogicalClock prepareStamp)
    {
        _shadowMarkerStamps ??= new Dictionary<string, Dictionary<Guid, HybridLogicalClock>>(StringComparer.Ordinal);
        if (!_shadowMarkerStamps.TryGetValue(key, out var bySaga))
        {
            bySaga = new Dictionary<Guid, HybridLogicalClock>();
            _shadowMarkerStamps[key] = bySaga;
        }

        bySaga[transactionId] = bySaga.TryGetValue(transactionId, out var existing) && existing.CompareTo(prepareStamp) > 0
            ? existing
            : prepareStamp;
        state.State.Clock = HybridLogicalClock.Merge(state.State.Clock, prepareStamp);
    }

    /// <summary>
    /// The marked prepare stamp of the marker on <paramref name="key"/> under
    /// <paramref name="transactionId"/>, or <see langword="null"/> when it was
    /// installed without one.
    /// </summary>
    private HybridLogicalClock? ShadowMarkerStamp(string key, Guid transactionId) =>
        _shadowMarkerStamps is not null
            && _shadowMarkerStamps.TryGetValue(key, out var bySaga)
            && bySaga.TryGetValue(transactionId, out var stamp)
            ? stamp
            : null;

    /// <summary>
    /// Removes <paramref name="transactionId"/> from every key's
    /// shadow set, prunes any empty sets, and releases the map when
    /// it falls empty. Invoked by <see cref="ApplyTxTerminalAsync"/>
    /// on every terminal application regardless of decision, so the
    /// guard has a bounded lifetime tied to saga progress.
    /// </summary>
    private void ClearSagaShadow(Guid transactionId)
    {
        if (_shadowMarkerStamps is not null)
        {
            List<string>? emptyStampKeys = null;
            foreach (var (key, bySaga) in _shadowMarkerStamps)
            {
                if (bySaga.Remove(transactionId) && bySaga.Count == 0)
                    (emptyStampKeys ??= new List<string>()).Add(key);
            }
            if (emptyStampKeys is not null)
            {
                foreach (var key in emptyStampKeys)
                    _shadowMarkerStamps.Remove(key);
            }
            if (_shadowMarkerStamps.Count == 0)
                _shadowMarkerStamps = null;
        }

        if (_shadowedSagas is null || _shadowedSagas.Count == 0) return;

        List<string>? emptyKeys = null;
        foreach (var (key, sagas) in _shadowedSagas)
        {
            if (sagas.Remove(transactionId) && sagas.Count == 0)
            {
                emptyKeys ??= new List<string>();
                emptyKeys.Add(key);
            }
        }
        if (emptyKeys is not null)
        {
            foreach (var key in emptyKeys)
                _shadowedSagas.Remove(key);
        }
        if (_shadowedSagas.Count == 0)
            _shadowedSagas = null;
    }

    /// <summary>
    /// Returns <c>true</c> when a destination-side shadow marker is
    /// installed for <paramref name="key"/>, returning the captured
    /// txid set. The read path uses this signal to decide whether to
    /// consult the registry for a shadow-routing decision on an
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.IsMigrated"/>=<c>true</c> value.
    /// </summary>
    private bool TryGetShadowedSagas(string key, out HashSet<Guid> sagas)
    {
        if (_shadowedSagas is not null && _shadowedSagas.TryGetValue(key, out var s) && s.Count > 0)
        {
            sagas = s;
            return true;
        }
        sagas = null!;
        return false;
    }

    /// <summary>
    /// Decides whether the read path is safe to surface an
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.IsMigrated"/>=<c>true</c> value for a
    /// key that carries a destination-side shadow marker. Resolves
    /// every shadowing saga's <see cref="TxStatus"/> through the
    /// per-tree registry (or the ambient
    /// <see cref="LatticeRegistrySnapshotContext"/> snapshot when one
    /// is in scope) and applies the per-saga rule:
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="TxStatus.InFlight"/> / <see cref="TxStatus.Aborted"/>:
    ///     the migrated pre-saga value is the strict-isolation-correct
    ///     answer, the saga is safe to pass through.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="TxStatus.Committed"/> with backstop already
    ///     applied (txid in <see cref="_recentlyTerminal"/>): the
    ///     value in <c>Entries[K]</c> is now post-saga and safe to
    ///     serve.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="TxStatus.Committed"/> without backstop: serving
    ///     the migrated pre-saga value would violate atomic visibility
    ///     against any sibling leaf whose backstop has already landed.
    ///     Returns <c>false</c> so the caller raises
    ///     <see cref="StaleShardRoutingException"/>.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="TxStatus.Indeterminate"/>: resolved exactly as
    ///     <see cref="TxStatus.Committed"/> is. Passing through would
    ///     assert the saga did not commit, which is precisely what an
    ///     indeterminate reading does not know; with the backstop
    ///     already applied the projected value is correct either way,
    ///     and without it the read gates.
    ///   </description></item>
    /// </list>
    /// <para>
    /// A committed or indeterminate saga whose terminal has not landed here is
    /// still served when its marker carries the saga's marked original prepare
    /// stamp P and <paramref name="rowStamp"/> is at or above it (issue #4545):
    /// the row is then the saga's own value or a later write, never the pre-saga
    /// value the gate exists to hide.
    /// </para>
    /// </summary>
    private async ValueTask<bool> IsShadowedReadSafeAsync(string key, HybridLogicalClock rowStamp, HashSet<Guid> sagas)
    {
        await EnsureTerminalWitnessHydratedAsync();
        foreach (var txid in sagas)
        {
            var status = await ResolvePendingStatusAsync(txid);
            // Per-saga safety is the shared, dependency-free
            // ShadowedMigrationReadGuard rule (see #1591): a committed saga is safe
            // only once its terminal has settled this key here (the durable,
            // per-key witness, issue #4545), or once the row is known to
            // incorporate its marked prepare; otherwise the migrated pre-saga
            // value would tear atomic visibility against a backstopped sibling.
            // Per key, not per saga: the saga's terminal may have reached this
            // leaf for another key while this key's committed value is still on
            // its way.
            var terminalApplied = IsTerminalWitnessed(txid, key);
            var incorporated = ShadowedMigrationReadGuard.RowIncorporatesMarkedPrepare(
                rowStamp, ShadowMarkerStamp(key, txid));
            if (!ShadowedMigrationReadGuard.IsSagaSafe(status, terminalApplied, incorporated))
                return false;
        }
        return true;
    }

    #if LATTICE_DIAG
    /// <summary>
    /// DIAG: decode the round prefix from a chaos-test value of
    /// shape <c>v-NNN-II</c>, where <c>NNN</c> is the round and
    /// <c>II</c> is the key index. Returns <c>-1</c> for any value
    /// that doesn't match the test's format.
    /// </summary>
    private static int DiagDecodeRound(byte[]? value)
    {
        // Mirror DiagSink.DecodeRound: 'v-NNN' is 5 bytes; the
        // previous Length < 6 guard rejected every legal value.
        if (value is null || value.Length < 3) return -1;
        if (value[0] != (byte)'v' || value[1] != (byte)'-') return -1;
        int round = 0;
        for (int i = 2; i < value.Length; i++)
        {
            var c = value[i];
            if (c < (byte)'0' || c > (byte)'9') return -1;
            round = round * 10 + (c - (byte)'0');
        }
        return round;
    }
#endif
}
