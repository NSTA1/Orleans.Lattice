using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Saga coordinator for atomic multi-key writes. One grain activation
/// per batch, keyed by <c>{treeId}/{operationId}</c>. Applies each write
/// sequentially, persists progress after every step, and compensates
/// previously-committed keys if a step throws. Crash recovery is driven by a
/// keepalive reminder registered at saga start and unregistered on completion.
/// <para>
/// Compensation relies on LWW: rewriting the pre-saga value (or tombstoning an
/// absent key) with a freshly-ticked <c>HybridLogicalClock</c> wins over the
/// partial write. Readers may observe a brief partial-visibility window during
/// execution and during compensation; this is inherent to the saga pattern.
/// </para>
/// <para>
/// <b>Retention cleanup.</b> After the saga reaches a terminal state
/// (<c>Completed</c>), the grain registers a one-shot retention reminder
/// (<c>atomic-write-retention</c>) configured by
/// <see cref="LatticeOptions.AtomicWriteRetention"/> (default 48h). When the
/// reminder fires, the grain clears its persisted state, unregisters the
/// reminder, and deactivates. This lets a client that re-issues the same
/// <c>operationId</c> within the retention window observe the original
/// outcome (idempotent re-invocation), while guaranteeing that saga state
/// does not leak forever. Set the option to
/// <see cref="Timeout.InfiniteTimeSpan"/> to disable retention cleanup.
/// </para>
/// </summary>
internal sealed class AtomicWriteGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<AtomicWriteGrain> logger,
    [PersistentState("atomic-write", LatticeOptions.StorageProviderName)]
    IPersistentState<AtomicWriteState> state)
    : TtlGrain<AtomicWriteGrain>(context, reminderRegistry, logger), IAtomicWriteGrain
{
    private const string KeepaliveReminderName = "atomic-write-keepalive";
    private const string RetentionReminderName = "atomic-write-retention";
    private const int MaxRetriesPerStep = 1;

    /// <summary>
    /// Wall-clock budget for stale-routing retries on grain calls that
    /// face a topology change (shard split, online resize, or reshard)
    /// mid-saga. The retry loops in <c>PrepareAsync</c> and
    /// <c>MarkOneShardAsync</c> refresh routing once per
    /// <see cref="StaleShardRoutingException"/> /
    /// <see cref="StaleTreeRoutingException"/> throw and re-issue the
    /// call against the freshly-resolved owner until success or the
    /// deadline elapses. Bounded by wall-clock rather than attempt count
    /// because the worst-case storm under a 4-to-8 reshard or an
    /// online-resize alias swap can generate more sequential map/alias
    /// updates than any reasonable fixed budget - an earlier
    /// 4-attempt budget surfaced
    /// <c>Stale*RoutingException</c> from
    /// <c>ReshardTopologyTests.Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard</c>
    /// and <c>ResizeTopologyTests.Continuous_reader_observes_zero_or_all_keys_through_mid_saga_resize</c>
    /// under CI load. The 60-second ceiling matches the chaos-test
    /// driver timeout so a runaway topology storm still terminates with
    /// the original stale-routing throw rather than hanging the saga.
    /// </summary>
    private static readonly TimeSpan StaleRoutingRetryBudget = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Composite grain key (<c>{treeId}/{operationId}</c>); used for logging.
    /// </summary>
    private string OperationKey => GrainContext.GrainId.Key.ToString()!;

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl()
    {
        var treeId = state.State.TreeId;
        var options = string.IsNullOrEmpty(treeId)
            ? optionsMonitor.CurrentValue
            : optionsMonitor.Get(treeId);
        return options.AtomicWriteRetention;
    }

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "Atomic-write saga {OperationKey}: retention window expired; clearing state.",
            OperationKey);
        await state.ClearStateAsync();
    }

    /// <inheritdoc />
    protected override async Task OnOtherReminderAsync(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;

        switch (state.State.Phase)
        {
            case AtomicWritePhase.Prepare:
            case AtomicWritePhase.Execute:
            case AtomicWritePhase.Compensate:
                try
                {
                    await RunSagaAsync();
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex,
                        "Atomic-write saga {OperationKey} failed on reminder-driven resume.",
                        OperationKey);
                }
                break;
            case AtomicWritePhase.Completed:
            case AtomicWritePhase.NotStarted:
                await UnregisterKeepaliveAsync();
                this.DeactivateOnIdle();
                break;
        }
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(string treeId, List<KeyValuePair<string, byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entries);

        // Empty batch: fast success, no saga work, no reminder needed.
        if (entries.Count == 0) return;

#if LATTICE_DIAG
        var swExec = System.Diagnostics.Stopwatch.StartNew();
        DiagSink.Write($"[DIAG saga-execute-enter] op={OperationKey} tree={treeId} entriesCount={entries.Count} phase={state.State.Phase}");
        try
        {
#endif

        // Caller-supplied idempotency keys: if the same operationId is
        // re-submitted with a different key set, reject it rather than
        // silently replaying the original persisted entries. Only
        // meaningful when a prior call has already seeded the fingerprint;
        // null fingerprint (legacy state or fresh saga) skips the check
        // and proceeds through the normal path below.
        if (state.State.Phase != AtomicWritePhase.NotStarted
            && state.State.KeyFingerprint is { } persistedFingerprint)
        {
            var incomingFingerprint = ComputeKeyFingerprint(entries);
            if (!CryptographicOperations.FixedTimeEquals(persistedFingerprint, incomingFingerprint))
            {
                throw new InvalidOperationException(
                    $"Atomic-write operation '{OperationKey}' was previously submitted with a different key set; " +
                    "reuse of a caller-supplied operationId requires the exact same set of keys.");
            }
        }

        // Idempotent re-entry: if a prior call has already completed this saga,
        // the client simply sees success again.
        if (state.State.Phase == AtomicWritePhase.Completed)
        {
            await TryThrowFailureAsync();
            return;
        }

        // Fresh saga - validate inputs, register the keepalive reminder first
        // (so a crash mid-Prepare still has a reminder-driven recovery path),
        // and then capture pre-saga state.
        if (state.State.Phase == AtomicWritePhase.NotStarted)
        {
            ValidateInputs(entries);
#if LATTICE_DIAG
            var swRegKa = System.Diagnostics.Stopwatch.StartNew();
            DiagSink.Write($"[DIAG saga-register-keepalive-enter] op={OperationKey} tree={treeId}");
#endif
            await RegisterKeepaliveAsync();
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG saga-register-keepalive-exit] op={OperationKey} tree={treeId} elapsedMs={swRegKa.Elapsed.TotalMilliseconds:F0}");
            var swPrep = System.Diagnostics.Stopwatch.StartNew();
            DiagSink.Write($"[DIAG saga-prepare-enter] op={OperationKey} tree={treeId} entriesCount={entries.Count}");
#endif
            await PrepareAsync(treeId, entries);
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG saga-prepare-exit] op={OperationKey} tree={treeId} elapsedMs={swPrep.Elapsed.TotalMilliseconds:F0} phase={state.State.Phase}");
#endif
        }

        await RunSagaAsync();
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG saga-execute-exit] op={OperationKey} tree={treeId} entriesCount={entries.Count} elapsedMs={swExec.Elapsed.TotalMilliseconds:F0} phase={state.State.Phase}");
        }
        catch (Exception ex)
        {
            DiagSink.Write($"[DIAG saga-execute-throw] op={OperationKey} tree={treeId} entriesCount={entries.Count} elapsedMs={swExec.Elapsed.TotalMilliseconds:F0} phase={state.State.Phase} ex={ex.GetType().Name} msg={ex.Message.Replace(System.Environment.NewLine, " | ")}");
            throw;
        }
#endif
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() =>
        Task.FromResult(
            state.State.Phase == AtomicWritePhase.NotStarted ||
            state.State.Phase == AtomicWritePhase.Completed);

    /// <summary>
    /// Validates the batch: non-null values and no duplicate keys.
    /// </summary>
    private static void ValidateInputs(List<KeyValuePair<string, byte[]>> entries)
    {
        var seen = new HashSet<string>(entries.Count, StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            if (entry.Key is null)
                throw new ArgumentException("Atomic write batch contains a null key.", nameof(entries));
            if (entry.Value is null)
                throw new ArgumentException(
                    $"Atomic write batch contains a null value for key '{entry.Key}'.", nameof(entries));
            if (!seen.Add(entry.Key))
                throw new ArgumentException(
                    $"Atomic write batch contains duplicate key '{entry.Key}'.", nameof(entries));
        }
    }

    /// <summary>
    /// Computes a SHA-256 fingerprint over the batch's sorted key set plus
    /// its entry count. Reordering the same keys produces the same
    /// fingerprint; changing any key (or the count) changes it. Values
    /// are not hashed - a caller retrying the same logical operation with
    /// slightly-different serialized payloads for the same keys is treated
    /// as an idempotent retry, not a mismatch.
    /// </summary>
    internal static byte[] ComputeKeyFingerprint(List<KeyValuePair<string, byte[]>> entries)
    {
        var sortedKeys = new string[entries.Count];
        for (int i = 0; i < entries.Count; i++) sortedKeys[i] = entries[i].Key;
        Array.Sort(sortedKeys, StringComparer.Ordinal);

        return ComputeKeyFingerprintCore(sortedKeys);
    }

    private static byte[] ComputeKeyFingerprintCore(string[] sortedKeys)
    {
        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lenBuf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lenBuf, sortedKeys.Length);
        sha.AppendData(lenBuf);

        // Per-key UTF-8 encoding scratch buffer. A single stackalloc is
        // hoisted out of the loop (CA2014: stackalloc inside a loop risks
        // unbounded stack growth on a long input). The buffer is reused
        // across every key whose worst-case UTF-8 length fits in the
        // 256-byte bound; pathologically long keys fall back to an
        // ArrayPool rental for that single iteration. The previous shape
        // (`Encoding.UTF8.GetBytes(key)` per key) allocated a fresh
        // transient byte[] per key, which dominated the saga prepare
        // allocation profile at 1.8% of total bytes (213 KB / op at
        // batch=16, concurrency=64) before this change.
        const int StackScratchBytes = 256;
        Span<byte> stackScratch = stackalloc byte[StackScratchBytes];
        foreach (var key in sortedKeys)
        {
            var maxBytes = Encoding.UTF8.GetMaxByteCount(key.Length);
            byte[]? rented = null;
            Span<byte> scratch = maxBytes <= StackScratchBytes
                ? stackScratch[..maxBytes]
                : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(maxBytes)).AsSpan(0, maxBytes);
            try
            {
                var written = Encoding.UTF8.GetBytes(key, scratch);
                BinaryPrimitives.WriteInt32LittleEndian(lenBuf, written);
                sha.AppendData(lenBuf);
                sha.AppendData(scratch[..written]);
            }
            finally
            {
                if (rented is not null)
                    System.Buffers.ArrayPool<byte>.Shared.Return(rented);
            }
        }
        return sha.GetHashAndReset();
    }

    /// <summary>
    /// Captures pre-saga values for every key via
    /// <see cref="IShardRootGrain.GetRawEntryAsync"/> so <c>ExpiresAtTicks</c>
    /// metadata is preserved for compensation. Already-expired entries are
    /// treated as absent (matching public read semantics) so compensation
    /// will restore an "absent" outcome rather than resurrect a stale value.
    /// <para>
    /// Routing is resolved once via the public <see cref="ILattice.GetRoutingAsync"/>
    /// hook (which returns routing metadata only, no CRDT internals) and the
    /// saga then addresses <see cref="IShardRootGrain"/> directly. This keeps
    /// the raw <see cref="LwwEntry"/> traffic on guarded internal grain
    /// interfaces - it never crosses the public <see cref="ILattice"/> surface.
    /// A <see cref="StaleShardRoutingException"/> from an adaptive shard split
    /// triggers a one-shot routing refresh and retry for the affected key.
    /// </para>
    /// </summary>
    private async Task PrepareAsync(string treeId, List<KeyValuePair<string, byte[]>> entries)
    {
        // Class B snapshot/restore: PrepareAsync mutates ~15 fields in place
        // before the terminal persist. A failure on that persist must revert
        // EVERY mutated field so the ExecuteAsync L168 NotStarted-only Prepare
        // branch re-runs PrepareAsync on retry from the same activation. The
        // standard pattern (capture previous values, try persist, catch
        // restore) is applied around the L393 WriteStateAsync.
        var prevPhase = state.State.Phase;
        var prevTreeId = state.State.TreeId;
        var prevEntries = state.State.Entries;
        var prevPreValues = state.State.PreValues;
        var prevNextIndex = state.State.NextIndex;
        var prevRetriesOnCurrentStep = state.State.RetriesOnCurrentStep;
        var prevFailureMessage = state.State.FailureMessage;
        var prevKeyFingerprint = state.State.KeyFingerprint;
        var prevTransactionId = state.State.TransactionId;
        var prevDelta = state.State.Delta;
        var prevVectorClock = state.State.VectorClock;
        var prevAtomicBatchSize = state.State.AtomicBatchSize;
        var prevSagaStartedAtTicks = state.State.SagaStartedAtTicks;
        var prevTouchedShards = state.State.TouchedShards;

        state.State.Phase = AtomicWritePhase.Prepare;
        state.State.TreeId = treeId;
        state.State.Entries = entries;
        state.State.PreValues = new List<AtomicPreValue>(entries.Count);
        state.State.NextIndex = 0;
        state.State.RetriesOnCurrentStep = 0;
        state.State.FailureMessage = null;
        state.State.KeyFingerprint ??= ComputeKeyFingerprint(entries);
        if (state.State.TransactionId == Guid.Empty)
        {
            state.State.TransactionId = Guid.NewGuid();
        }

        // Capture caller's ambient author-delta carry once, on the first
        // Prepare. On a reminder-driven replay (no caller context) the
        // persisted field is reused verbatim, mirroring the
        // KeyFingerprint / TransactionId capture-once pattern.
        if (state.State.Delta is null)
        {
            var deltaCarry = LatticeDeltaContext.Current;
            if (deltaCarry is not null)
            {
                state.State.Delta = deltaCarry;
            }
        }

        // Capture caller's ambient vector-clock frontier once, on the
        // first Prepare. On a reminder-driven replay (no caller
        // context) the persisted value is reused verbatim, mirroring
        // the KeyFingerprint / TransactionId / Delta capture-once
        // pattern. The guard exists for symmetry with the parallel
        // capture blocks; a persisted-null value is observably
        // indistinguishable from "never captured" because reminder
        // re-entry has no ambient context either.
        if (state.State.VectorClock is null)
        {
            state.State.VectorClock = LatticeVectorClockContext.Current;
        }

        // Capture batch size once, on the first Prepare, from the
        // submitted entries' Count. The Count is the canonical sibling
        // count a remote receiver reads back off
        // LatticeMutation.AtomicBatchSize to detect when every entry
        // of a batch has arrived at its receiver-side staging buffer.
        // Capture-once mirrors the surrounding KeyFingerprint /
        // TransactionId / DeltaKind / VectorClock pattern; the guard
        // is "default = 0" rather than "is null" because the slot is
        // an int. A persisted-zero value on a reminder-driven replay
        // is indistinguishable from "never captured", and the saga's
        // own pre-validation rejects zero-entry batches before reaching
        // PrepareAsync, so a legitimate capture is always non-zero.
        if (state.State.AtomicBatchSize == 0)
        {
            state.State.AtomicBatchSize = entries.Count;
        }

        // Saga start tick capture-once. Stamps the wall-clock UTC tick
        // on the first PrepareAsync entry so the
        // orleans.lattice.atomic_write.duration histogram emitted on
        // CompleteSagaAsync reflects true end-to-end saga cost,
        // including any time spent suspended across silo restarts.
        // Zero is the "never captured" sentinel; a reminder-driven
        // replay that finds a non-zero value preserves the original
        // start tick rather than restamping it.
        if (state.State.SagaStartedAtTicks == 0)
        {
            state.State.SagaStartedAtTicks = DateTimeOffset.UtcNow.UtcTicks;
        }

        var lattice = grainFactory.GetGrain<ILattice>(treeId);
#if LATTICE_DIAG
        var swRouting = System.Diagnostics.Stopwatch.StartNew();
        DiagSink.Write($"[DIAG saga-prepare-routing-enter] op={OperationKey} tree={treeId}");
#endif
        // forceRefresh:true at saga entry: the saga's TouchedShards
        // capture, per-key SetAsync routing, and BroadcastTerminalsAsync
        // drift correction all derive from this initial snapshot. A
        // stale activation-cached map here would seed the saga with the
        // pre-reshard physical shard set, and even though the broadcast
        // pass re-resolves with forceRefresh:true, the per-key SetAsync
        // loop in ExecutePhaseAsync routes through the public
        // ILattice surface against the LatticeGrain's own cache - if
        // that cache is also stale, some keys land on shards that have
        // since lost ownership of their slot, leaving the pending-tx
        // bucket on the wrong physical shard. Starting the saga with a
        // freshly-forced cache invalidation propagates the new map to
        // every subsequent ILattice call from this activation and so
        // anchors the saga to the post-migration topology.
        var routing = await lattice.GetRoutingAsync(forceRefresh: true);
#if LATTICE_DIAG
        DiagSink.Write($"[DIAG saga-prepare-routing-exit] op={OperationKey} tree={treeId} physicalTree={routing.PhysicalTreeId} elapsedMs={swRouting.Elapsed.TotalMilliseconds:F0}");
#endif
        var nowTicks = DateTimeOffset.UtcNow.UtcTicks;

        // Touched-shard set capture. Populated from the routing
        // snapshot's Map.Resolve(key) per entry, deduplicated and
        // sorted for stable persistence ordering. Drives the
        // post-execute terminal broadcast loop: one
        // AppendTxTerminalAsync call per distinct physical shard,
        // never per key. A migration that remaps slots between
        // capture and broadcast is handled in BroadcastTerminalsAsync
        // via StaleShardRoutingException / StaleTreeRoutingException
        // retry, which re-resolves the owner against a fresh routing
        // snapshot.
        var touched = new HashSet<int>();
        foreach (var entry in entries)
        {
            touched.Add(routing.Map.Resolve(entry.Key));
        }
        var touchedSorted = new List<int>(touched);
        touchedSorted.Sort();
        state.State.TouchedShards = touchedSorted;

        // Pre-saga value capture: group keys by their routed shard, then
        // issue ONE batched GetRawEntriesAsync RPC per shard in parallel.
        // The earlier shape was a 16-iteration sequential per-entry
        // foreach that paid one cross-grain Task allocation per entry
        // (16 in the microbench atomic batch); the batched shape pays
        // one Task allocation per distinct touched shard (1 in the
        // single-shard microbench, up to 4 in the 4-shards variant).
        // Stale-routing retry is per-shard with the same wall-clock
        // budget as the original loop, since a topology change must be
        // re-resolved against a fresh snapshot regardless of fan-out
        // shape. The per-shard call returns a list aligned by index
        // with its input keys list, so the scatter back into PreValues
        // tracks (key -> original entry index) explicitly.
        var preValuesArray = new AtomicPreValue[entries.Count];
        var shardBuckets = new Dictionary<int, List<(string Key, int Index)>>(touched.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            var key = entries[i].Key;
            var shardIndex = routing.Map.Resolve(key);
            if (!shardBuckets.TryGetValue(shardIndex, out var bucket))
            {
                bucket = new List<(string, int)>();
                shardBuckets[shardIndex] = bucket;
            }
            bucket.Add((key, i));
        }

        var capturePending = new List<Task>(shardBuckets.Count);
        foreach (var (shardIndex, bucket) in shardBuckets)
        {
            capturePending.Add(CaptureShardAsync(lattice, routing, shardIndex, bucket, preValuesArray, nowTicks));
        }
        // Mutate the routing snapshot reference inside the per-shard
        // helper is unnecessary: GetRoutingAsync inside the helper
        // hands back a fresh snapshot only on a stale-routing throw,
        // and the outer loop here does not consult routing again.
#if LATTICE_DIAG
        var swCapture = System.Diagnostics.Stopwatch.StartNew();
        DiagSink.Write($"[DIAG saga-prepare-capture-enter] op={OperationKey} tree={treeId} buckets={shardBuckets.Count}");
#endif
        await Task.WhenAll(capturePending);
#if LATTICE_DIAG
        DiagSink.Write($"[DIAG saga-prepare-capture-exit] op={OperationKey} tree={treeId} elapsedMs={swCapture.Elapsed.TotalMilliseconds:F0}");
#endif

        // Materialise the array into the persisted list in input order.
        state.State.PreValues = new List<AtomicPreValue>(preValuesArray.Length);
        for (int i = 0; i < preValuesArray.Length; i++)
        {
            state.State.PreValues.Add(preValuesArray[i]);
        }

        state.State.Phase = AtomicWritePhase.Execute;
#if LATTICE_DIAG
        var swPersist = System.Diagnostics.Stopwatch.StartNew();
        DiagSink.Write($"[DIAG saga-prepare-persist-enter] op={OperationKey} tree={treeId}");
#endif
        try
        {
            await WriteSagaStateAsync("prepare");
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG saga-prepare-persist-exit] op={OperationKey} tree={treeId} elapsedMs={swPersist.Elapsed.TotalMilliseconds:F0}");
#endif
        }
        catch
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG saga-prepare-persist-fail] op={OperationKey} tree={treeId} elapsedMs={swPersist.Elapsed.TotalMilliseconds:F0}");
#endif
            state.State.Phase = prevPhase;
            state.State.TreeId = prevTreeId;
            state.State.Entries = prevEntries;
            state.State.PreValues = prevPreValues;
            state.State.NextIndex = prevNextIndex;
            state.State.RetriesOnCurrentStep = prevRetriesOnCurrentStep;
            state.State.FailureMessage = prevFailureMessage;
            state.State.KeyFingerprint = prevKeyFingerprint;
            state.State.TransactionId = prevTransactionId;
            state.State.Delta = prevDelta;
            state.State.VectorClock = prevVectorClock;
            state.State.AtomicBatchSize = prevAtomicBatchSize;
            state.State.SagaStartedAtTicks = prevSagaStartedAtTicks;
            state.State.TouchedShards = prevTouchedShards;
            throw;
        }

        // Bulk pre-register the saga's pre-computed touched-shard set
        // with the per-tree TxRegistry as a single linearised write.
        // This collapses the N per-shard RegisterParticipantAsync
        // writes that ShardRootGrain.RecordAffectedLeafIfPreparedAsync
        // would otherwise issue (one WriteStateAsync per touched shard,
        // serialised through the per-tree registry's single activation)
        // into one WriteStateAsync per saga. The per-shard path stays
        // in place as the drift-correction safety net for keys whose
        // routing flips between Prepare and Execute, and short-circuits
        // to a no-op for any slot this bulk call has already populated.
        //
        // Best-effort: a registry RPC failure here does NOT fail the
        // saga. The per-shard RegisterParticipantAsync calls run from
        // each ShardRootGrain's prepare-phase write and remain the
        // authoritative population path; the bulk call is purely a
        // write-coalescing optimisation. Swallowing the throw keeps
        // saga semantics identical to the pre-bulk implementation
        // when the registry is transiently unreachable.
        if (state.State.TransactionId != Guid.Empty && touchedSorted.Count > 0)
        {
            try
            {
                var registry = grainFactory.GetGrain<ITxRegistryGrain>(treeId);
                await registry.RegisterParticipantsAsync(state.State.TransactionId, touchedSorted);
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG saga-prepare-bulk-register-exit] op={OperationKey} tx={state.State.TransactionId} shards={touchedSorted.Count}");
#endif
            }
            catch (Exception ex)
            {
                Logger.LogDebug(
                    ex,
                    "Atomic-write saga {OperationKey}: bulk participant pre-register failed; falling back to per-shard registration (saga unaffected).",
                    OperationKey);
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG saga-prepare-bulk-register-fail] op={OperationKey} tx={state.State.TransactionId} shards={touchedSorted.Count} ex={ex.GetType().Name}");
#endif
            }
        }
    }

    /// <summary>
    /// Per-shard pre-saga capture helper used by <see cref="PrepareAsync"/>.
    /// Issues a single <see cref="IShardRootGrain.GetRawEntriesAsync"/>
    /// call for every key in <paramref name="bucket"/>, scatters the
    /// response into <paramref name="preValues"/> at each entry's
    /// original input index, and retries on a stale-routing throw with
    /// the same deadline-bounded budget the prior per-entry loop used.
    /// <paramref name="initialRouting"/> is the snapshot the caller
    /// already fetched once at the head of <see cref="PrepareAsync"/>;
    /// the helper reuses it on the first attempt and only re-fetches
    /// after a stale-routing throw, preserving the routing-refresh
    /// accounting the prior per-entry loop established (one fetch at
    /// PrepareAsync start + one per stale-throw + one in
    /// BroadcastTerminalsAsync's drift-correction pass).
    /// </summary>
    private async Task CaptureShardAsync(
        ILattice lattice,
        RoutingInfo initialRouting,
        int shardIndex,
        List<(string Key, int Index)> bucket,
        AtomicPreValue[] preValues,
        long nowTicks)
    {
        // The catch blocks unconditionally fire so the original
        // stale-routing throw surfaces to the caller once the wall-clock
        // budget elapses; a when-filter on the catch could race against
        // the loop condition and exit silently.
        var deadline = DateTime.UtcNow + StaleRoutingRetryBudget;
        var routing = initialRouting;
#if LATTICE_DIAG
        var swCapShard = System.Diagnostics.Stopwatch.StartNew();
        int capAttempts = 0;
        DiagSink.Write($"[DIAG capture-shard-enter] op={OperationKey} shard={shardIndex} physTree={routing.PhysicalTreeId} keys={bucket.Count}");
#endif
        while (true)
        {
            var keys = new List<string>(bucket.Count);
            foreach (var (key, _) in bucket) keys.Add(key);

            List<LwwEntry?>? raws = null;
            try
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>(
                    $"{routing.PhysicalTreeId}/{shardIndex}");
#if LATTICE_DIAG
                capAttempts++;
                var swCall = System.Diagnostics.Stopwatch.StartNew();
                DiagSink.Write($"[DIAG capture-shard-call-enter] op={OperationKey} shard={shardIndex} physTree={routing.PhysicalTreeId} attempt={capAttempts}");
#endif
                raws = await shard.GetRawEntriesAsync(keys);
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG capture-shard-call-exit] op={OperationKey} shard={shardIndex} physTree={routing.PhysicalTreeId} attempt={capAttempts} elapsedMs={swCall.Elapsed.TotalMilliseconds:F0}");
#endif
            }
            catch (StaleShardRoutingException)
            {
                // Adaptive shard split / reshard remapped at least one
                // slot in this bucket. Refresh routing and re-bucket
                // remaining keys against the new map: a refresh alone
                // is not enough because the per-bucket call addresses
                // the OLD shardIndex, which still owns only a subset
                // (or none) of the bucket's keys after migration. The
                // forceRefresh:true overload is required - the
                // LatticeGrain is a StatelessWorker with per-activation
                // cached routing whose private invalidation hooks only
                // fire on the grain's own internal stale-routing
                // catches; an external caller cannot otherwise force a
                // cache refresh, so a plain GetRoutingAsync() retry
                // would spin against the same stale map indefinitely.
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG capture-shard-stale-shard] op={OperationKey} shard={shardIndex} physTree={routing.PhysicalTreeId} attempt={capAttempts} elapsedMs={swCapShard.Elapsed.TotalMilliseconds:F0} deadlineRemainMs={(deadline - DateTime.UtcNow).TotalMilliseconds:F0}");
#endif
                if (DateTime.UtcNow >= deadline) throw;
                routing = await lattice.GetRoutingAsync(forceRefresh: true);
                var rebucketed = new Dictionary<int, List<(string Key, int Index)>>();
                foreach (var entry in bucket)
                {
                    var newOwner = routing.Map.Resolve(entry.Key);
                    if (!rebucketed.TryGetValue(newOwner, out var list))
                        rebucketed[newOwner] = list = new List<(string, int)>();
                    list.Add(entry);
                }
                // Fast path: every key still routes to the same shard
                // we just queried (so the throw came from an alias
                // swap or a transient inconsistency, not a real
                // migration of this bucket's keys). Retry against the
                // refreshed physical tree id without recursing.
                if (rebucketed.Count == 1 && rebucketed.ContainsKey(shardIndex))
                    continue;
                // Migration: fan out the bucket across its new owners.
                // Each sub-bucket recursively goes through the same
                // stale-routing recovery loop, so cascading splits
                // converge in O(splits) per affected key.
                var pending = new List<Task>(rebucketed.Count);
                foreach (var (newShardIdx, subBucket) in rebucketed)
                    pending.Add(CaptureShardAsync(lattice, routing, newShardIdx, subBucket, preValues, nowTicks));
                await Task.WhenAll(pending);
                return;
            }
            catch (StaleTreeRoutingException)
            {
                // Tree alias was swapped mid-saga (online resize /
                // reshard); refresh routing and retry against the new
                // physical tree. shardIndex is preserved because alias
                // swap does not remap virtual slots, only the physical
                // tree's storage suffix. Same force-refresh rationale
                // as the StaleShardRoutingException catch above.
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG capture-shard-stale-tree] op={OperationKey} shard={shardIndex} physTree={routing.PhysicalTreeId} attempt={capAttempts} elapsedMs={swCapShard.Elapsed.TotalMilliseconds:F0} deadlineRemainMs={(deadline - DateTime.UtcNow).TotalMilliseconds:F0}");
#endif
                if (DateTime.UtcNow >= deadline) throw;
                routing = await lattice.GetRoutingAsync(forceRefresh: true);
                continue;
            }

            // Success: scatter raws into preValues at each entry's
            // original input index. raws is aligned by index with
            // the keys list we built above, which itself preserves
            // bucket order.
            for (int i = 0; i < bucket.Count; i++)
            {
                var raw = raws![i];
                var existed = raw is not null
                    && !raw.Value.IsTombstone
                    && !raw.Value.ToLwwValue().IsExpired(nowTicks);
                preValues[bucket[i].Index] = new AtomicPreValue
                {
                    Key = bucket[i].Key,
                    Value = existed ? raw!.Value.Value : null,
                    Existed = existed,
                    ExpiresAtTicks = existed ? raw!.Value.ExpiresAtTicks : 0,
                    OriginClusterId = existed ? raw!.Value.OriginClusterId : null,
                    VectorClock = existed ? raw!.Value.VectorClock : null,
                };
            }
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG capture-shard-exit] op={OperationKey} shard={shardIndex} attempts={capAttempts} elapsedMs={swCapShard.Elapsed.TotalMilliseconds:F0}");
#endif
            return;
        }
    }

    /// <summary>
    /// Broadcasts the saga's terminal mark
    /// (<see cref="MutationKind.TxCommit"/> on
    /// <paramref name="committed"/> = <see langword="true"/>;
    /// <see cref="MutationKind.TxAbort"/> otherwise) to every physical
    /// shard the prepare phase touched, by calling
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync(System.Guid, bool, System.Threading.CancellationToken)"/>
    /// once per shard.
    /// <para>
    /// The shard set is read from the persisted
    /// <see cref="AtomicWriteState.TouchedShards"/>. If that list is
    /// empty (legacy saga state from before the field existed, or a
    /// reactivation after a crash that pre-dated this field) the
    /// touched shard set is reconstructed by re-resolving every entry
    /// against a freshly fetched routing snapshot and persisted in
    /// place so subsequent retries skip the recomputation.
    /// </para>
    /// <para>
    /// Each per-shard call is wrapped in a one-shot retry on
    /// <see cref="StaleShardRoutingException"/> (a slot moved between
    /// prepare and broadcast) and
    /// <see cref="StaleTreeRoutingException"/> (a tree alias was
    /// swapped mid-saga, e.g. by online resize). The retry refreshes
    /// the routing snapshot via <see cref="ILattice.GetRoutingAsync"/>
    /// and re-broadcasts under the new physical tree id; re-delivery
    /// to a shard that already saw the terminal is a no-op via the
    /// leaf-side recently-terminal dedup, so retries on the happy
    /// path are safe too. Per-shard calls run in parallel because
    /// each shard's append is independent.
    /// </para>
    /// </summary>
    private async Task BroadcastTerminalsAsync(bool committed)
    {
        var transactionId = state.State.TransactionId;
        if (transactionId == Guid.Empty)
        {
            // Defensive: Saga should never reach broadcast without a
            // minted transaction id. If it does (legacy persisted
            // state, or a code path that bypassed StampTransactionId),
            // there is no per-shard linearization point to mark, so
            // skip the broadcast. The saga still completes - the
            // worst case is that prepared writes (if any) remain
            // bucketed in the leaves' pending-tx maps until they
            // age out of replay or the operator manually drops them.
            Logger.LogWarning(
                "Atomic-write saga {OperationKey}: skipping terminal broadcast - no transaction id is set on persisted state.",
                OperationKey);
            return;
        }

#if LATTICE_DIAG
        DiagSink.Write($"[DIAG broadcast-entry] op={OperationKey} tx={transactionId} committed={committed} initialTouched=[{string.Join(",", state.State.TouchedShards)}] entriesCount={state.State.Entries.Count}");
#endif

        // Reconstruct the touched-shard set on legacy state where the
        // slot was never populated. Persist the reconstruction so a
        // subsequent crash-resume can skip the rebuild.
        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
        string? physicalTreeId = null;
        // Hoist the routing snapshot so the drift-correction pass and
        // the per-shard backstop-dict computation below reuse the same
        // GetRoutingAsync fetch. A second fetch would double-bill the
        // routing-refresh budget tracked by AtomicWriteGrainTests.StaleRouting
        // and obscures the contract that the broadcast pass takes
        // exactly one routing snapshot per non-trivial saga.
        RoutingInfo? routing = null;
        if (state.State.TouchedShards.Count == 0 && state.State.Entries.Count > 0)
        {
            routing = await lattice.GetRoutingAsync(forceRefresh: true);
            physicalTreeId = routing.PhysicalTreeId;
            var touched = new HashSet<int>();
            foreach (var entry in state.State.Entries)
            {
                touched.Add(routing.Map.Resolve(entry.Key));
            }
            var sorted = new List<int>(touched);
            sorted.Sort();
            // Class B snapshot/restore: a failure on this persist must revert
            // TouchedShards so the next reconstruction pass re-derives the set
            // rather than iterating the dirty in-memory list during the
            // remainder of this activation's broadcast.
            var prevTouchedShards = state.State.TouchedShards;
            state.State.TouchedShards = sorted;
            try
            {
                await WriteSagaStateAsync("broadcast-touched-init");
            }
            catch
            {
                state.State.TouchedShards = prevTouchedShards;
                throw;
            }
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG broadcast-reconstructed] op={OperationKey} tx={transactionId} touched=[{string.Join(",", state.State.TouchedShards)}]");
#endif
        }
        else if (state.State.Entries.Count > 0)
        {
            // Routing-drift correction: TouchedShards was captured from
            // the routing snapshot at the start of PrepareAsync, but
            // the per-entry SetAsync calls in ExecutePhaseAsync route
            // through the public ILattice surface which fetches a
            // fresh routing snapshot per call. If the shard map
            // changed between prepare and execute (an online reshard
            // / resize / shard-split landing mid-saga, which is
            // exactly what the chaos suite exercises), some entries'
            // prepared writes may have landed on a *different*
            // physical shard than the one captured in TouchedShards.
            // The terminal broadcast must reach EVERY shard that
            // could hold a pending-tx bucket for this saga, otherwise
            // those buckets are orphaned forever (or until the replay
            // coordinator ages them out) and a reader routed to that
            // shard surfaces the destination's pre-saga value
            // indefinitely. Fix: re-resolve every entry against a
            // fresh routing snapshot and union the result into
            // TouchedShards. This is purely additive - old captures
            // are preserved (for sagas whose prepare landed on the
            // OLD owner before a migration moved the slot, the OLD
            // owner is in TouchedShards and the
            // TerminalFanOutResolver pass below BFS-expands the
            // OLD owner's MovedAwaySlots / SplitInProgress into the
            // broadcast's destination set).
            routing = await lattice.GetRoutingAsync(forceRefresh: true);
            physicalTreeId = routing.PhysicalTreeId;
            HashSet<int>? union = null;
            foreach (var entry in state.State.Entries)
            {
                var owner = routing.Map.Resolve(entry.Key);
                if (state.State.TouchedShards.Contains(owner)) continue;
                (union ??= new HashSet<int>(state.State.TouchedShards)).Add(owner);
            }
            if (union is not null)
            {
                var sorted = new List<int>(union);
                sorted.Sort();
                // Class B snapshot/restore: a failure here leaves the union'd
                // TouchedShards dirty in memory. The remaining broadcast pass
                // would iterate the dirty set and double-fan-out terminal
                // appends to the union'd shards.
                var prevTouchedShards = state.State.TouchedShards;
                state.State.TouchedShards = sorted;
                try
                {
                    await WriteSagaStateAsync("broadcast-touched-drift");
                }
                catch
                {
                    state.State.TouchedShards = prevTouchedShards;
                    throw;
                }
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG broadcast-drift-corrected] op={OperationKey} tx={transactionId} touched=[{string.Join(",", state.State.TouchedShards)}]");
#endif
            }
        }

        // Authoritative participant union from the per-tree TxRegistry.
        // Every ShardRootGrain that routed a prepare-phase write under
        // this saga registered itself as a participant inside
        // RecordAffectedLeafIfPreparedAsync (gated by a per-activation
        // dedup so the RPC fires once per saga per shard). Unioning the
        // registry's participant set into TouchedShards before the
        // broadcast closes the orphaning window left by the snapshot-
        // based drift correction above: if a key's prepare landed on a
        // shard that is no longer the routing target at broadcast time
        // (e.g. a shard-split swap completed mid-saga), the snapshot-
        // based correction cannot rediscover the old owner, but the
        // registry remembers it. The registry RPC is a single
        // cross-grain call regardless of saga fan-out width, so the
        // amortised cost is negligible compared to the per-shard
        // terminal fan-out below. The registry returns a sorted set
        // so the union iteration is order-deterministic.
        //
        // The registry handle is hoisted to function scope so the
        // post-fan-out late-participant fetch-loop below can reuse it
        // without re-resolving the grain reference. It is non-null
        // here only when transactionId != Guid.Empty (the early-return
        // at the top of this method guarantees that precondition).
        ITxRegistryGrain? registry = null;
        if (transactionId != Guid.Empty)
        {
            registry = grainFactory.GetGrain<ITxRegistryGrain>(state.State.TreeId);
            var participants = await registry.GetParticipantsAsync(transactionId);
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG broadcast-registry-fetch] op={OperationKey} tx={transactionId} participants=[{string.Join(",", participants)}]");
#endif
            if (participants.Count > 0)
            {
                HashSet<int>? regUnion = null;
                foreach (var shardIndex in participants)
                {
                    if (state.State.TouchedShards.Contains(shardIndex)) continue;
                    (regUnion ??= new HashSet<int>(state.State.TouchedShards)).Add(shardIndex);
                }
                if (regUnion is not null)
                {
                    var sorted = new List<int>(regUnion);
                    sorted.Sort();
                    // Class B snapshot/restore: same shape as the drift-correction
                    // branch above. Reverts TouchedShards on persist failure so the
                    // remaining broadcast pass does not iterate a dirty union.
                    var prevTouchedShards = state.State.TouchedShards;
                    state.State.TouchedShards = sorted;
                    try
                    {
                        await WriteSagaStateAsync("broadcast-touched-registry");
                    }
                    catch
                    {
                        state.State.TouchedShards = prevTouchedShards;
                        throw;
                    }
#if LATTICE_DIAG
                    DiagSink.Write($"[DIAG broadcast-registry-unioned] op={OperationKey} tx={transactionId} touched=[{string.Join(",", state.State.TouchedShards)}]");
#endif
                }
            }
        }

        if (state.State.TouchedShards.Count == 0)
        {
            // Empty saga (zero entries): no shard was touched, so no
            // terminal mark to broadcast.
            return;
        }

        // Seed the broadcast pass with the logical tree id as the
        // physical tree id. Under steady state (no online resize in
        // flight) logical and physical coincide, so the first attempt
        // succeeds and we save an RPC. If a resize alias-swap has
        // landed between prepare and broadcast, the first per-shard
        // call throws StaleTreeRoutingException and MarkOneShardAsync's
        // retry loop refreshes routing against the new physical tree -
        // amortised across the per-shard fan-out. When we just had to
        // reconstruct the touched-shard set above we already have a
        // fresh routing snapshot in hand and reuse it.
        physicalTreeId ??= state.State.TreeId;

        // Pre-resolve the transitive split-forward closure of
        // TouchedShards. Each shard root's GetSplitForwardTargetsAsync
        // reports its in-flight split destination (when one is
        // recorded) plus every distinct value in its MovedAwaySlots
        // map, and the resolver BFS-expands those wavefronts until
        // no new destinations are discovered. The expanded set
        // replaces the recursive ForwardSplitTerminalAsync hop that
        // ShardRootGrain.AppendTxTerminalAsync used to perform on
        // every receive: under cascading mid-saga splits that
        // recursion compounded into an unbounded RPC chain depth on
        // a single shard's turn, which tripped Orleans' default
        // response timeout (~30s) on deep multi-hop reshard chains
        // (e.g. 4 -> 8 reshard with cascading adaptive splits ending
        // at 11 physical shards). Pre-resolving here moves the fan-
        // out into the saga's own broadcast loop, where every
        // destination is reached in a single parallel hop. Persist
        // the expanded set so a crash-resume picks up the same
        // closure without re-running the BFS.
        if (state.State.TouchedShards.Count > 0)
        {
            var expanded = await TerminalFanOutResolver.ResolveTransitiveAsync(
                grainFactory,
                physicalTreeId,
                state.State.TouchedShards,
                CancellationToken.None);
            if (expanded.Count != state.State.TouchedShards.Count)
            {
                state.State.TouchedShards = expanded;
                await WriteSagaStateAsync("broadcast-touched-expand");
            }
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG broadcast-expanded] op={OperationKey} tx={transactionId} touched=[{string.Join(",", state.State.TouchedShards)}]");
#endif
        }

        // Compute per-shard subsets of the saga's committed values for
        // the cross-migration LWW backstop. The backstop fires on the
        // commit path only; on abort, the leaf-side handler drops the
        // pending bucket and the values are not surfaced. Each shard
        // receives ONLY the (key, value) pairs that route to it under
        // the routing snapshot in hand - the shard root performs the
        // further per-leaf grouping inside AppendTxTerminalAsync.
        //
        // The keys whose routing has DRIFTED since prepare (already
        // unioned into TouchedShards above) are routed to their NEW
        // owner here, so the destination shard's leaf receives the
        // backstop even when its prepare-phase shadow-forward was
        // dropped. Note: a shard in TouchedShards that no longer owns
        // any of the saga's keys (e.g. the original owner of a
        // migrated slot) receives a null backstop dict; that's
        // correct - its pending-flip path remains the authoritative
        // delivery for it.
        Dictionary<int, Dictionary<string, byte[]>>? perShardCommitted = null;
        if (committed && state.State.Entries.Count > 0)
        {
            var routingForBackstop = routing ?? await lattice.GetRoutingAsync(forceRefresh: true);
            perShardCommitted = new Dictionary<int, Dictionary<string, byte[]>>();
            foreach (var entry in state.State.Entries)
            {
                var owner = routingForBackstop.Map.Resolve(entry.Key);
                if (!perShardCommitted.TryGetValue(owner, out var bucket))
                {
                    bucket = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                    perShardCommitted[owner] = bucket;
                }
                bucket[entry.Key] = entry.Value;
            }
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG broadcast-perShardCommitted] op={OperationKey} tx={transactionId} shards=[{string.Join(",", perShardCommitted.Keys.Select(k => $"{k}({perShardCommitted[k].Count})"))}]");
#endif
        }

        // Defensive backstop for transitive-expansion shards. The
        // routing-based perShardCommitted loop above only populates
        // entries for shards whose ownership is reflected in the
        // current routing map. Transitively-discovered shards from
        // TerminalFanOutResolver (cascading mid-saga split
        // destinations whose alias swap hasn't landed yet) would
        // otherwise receive a NULL backstop dict, leaving any saga
        // key whose pending bucket on the destination was dropped
        // (sweep failure, drain race) orphaned at the destination's
        // pre-saga value. The pre-flat-fan-out recursive forwarding
        // path covered this defensively by passing committedValues
        // unchanged through every hop; the destination's
        // BroadcastTerminalToLeavesAsync then localized each key via
        // per-key TraverseToLeafAsync. Restore the same defensive
        // surface by handing every TouchedShard without a routing-
        // resolved subset the FULL backstop dict - the shard root's
        // per-key traversal routes only the keys it actually owns to
        // its own leaves. CRDT-LWW-safe under accidental delivery:
        // leaf ApplyTxTerminalAsync performs no range-ownership
        // check (see ShardRootGrain.TxTerminal.cs:339-340).
        //
        // The full-backstop dict is hoisted to function scope so the
        // post-fan-out late-participant fetch-loop below can reuse it
        // for any late arrival whose ownership is not reflected in the
        // routing snapshot used to compute perShardCommitted above.
        Dictionary<string, byte[]>? fullBackstop = null;
        if (committed && state.State.Entries.Count > 0 && perShardCommitted is not null)
        {
            foreach (var shardIndex in state.State.TouchedShards)
            {
                if (perShardCommitted.ContainsKey(shardIndex)) continue;
                if (fullBackstop is null)
                {
                    fullBackstop = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                    foreach (var entry in state.State.Entries)
                        fullBackstop[entry.Key] = entry.Value;
                }
                perShardCommitted[shardIndex] = fullBackstop;
            }
        }

#if LATTICE_DIAG
        DiagSink.Write($"[DIAG broadcast-initial-fanout] op={OperationKey} tx={transactionId} shards=[{string.Join(",", state.State.TouchedShards)}]");
#endif

        var pending = new List<Task<WalRecord?>>(state.State.TouchedShards.Count);
        foreach (var shardIndex in state.State.TouchedShards)
        {
            IReadOnlyDictionary<string, byte[]>? subset = null;
            if (perShardCommitted is not null && perShardCommitted.TryGetValue(shardIndex, out var bucket))
                subset = bucket;
            pending.Add(MarkOneShardAsync(physicalTreeId, shardIndex, transactionId, committed, subset));
        }

        var initialRecords = await Task.WhenAll(pending);
        // c2-xxiii: batched-WAL durability. The shard fan-out built one
        // WalRecord per touched shard but did not write any of them;
        // collapse the N serialised single-entry partition transactions
        // into one ICommitLogWriter.AppendManyAsync dispatch per
        // partition. The writer adapter already groups by partition
        // and fans out in parallel, so each WAL partition observes one
        // batched AppendBatchAsync rather than N stop-and-wait single
        // appends. Saga still awaits durability before returning.
        await FlushPendingTerminalsAsync(initialRecords);

        // Orphan-window closure: re-fetch participants and drain any
        // late arrivals. The initial GetParticipantsAsync fetch above
        // is a single snapshot, but a concurrent
        // TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync
        // can register a destination shard as a participant via
        // RecordAffectedLeafIfPreparedAsync AFTER the snapshot and
        // BEFORE the saga calls ForgetAsync on the registry. Without
        // this loop, that late-arrived destination keeps an orphaned
        // _pendingTx bucket whose Decisions[txid] = Committed is still
        // recorded, so a reader routed to that destination resolves
        // the pending status to Committed and surfaces the saga's
        // pre-overlay value indefinitely (until the next saga's
        // shadow-overwrite, which the chaos suite caught as the
        // surviving visibility-race after the flat saga-terminal
        // fan-out eliminated the recursive-forward timeout failure
        // mode but left this orphan window open).
        //
        // Each iteration: (1) re-fetch the registry participants;
        // (2) diff against the already-terminalled set; (3)
        // transitively expand new arrivals via
        // TerminalFanOutResolver so any cascading-split children of
        // the new arrival are also reached; (4) fan terminals out
        // to the late shards using the same per-shard subset / full-
        // backstop logic as the initial pass; (5) persist the
        // updated TouchedShards so crash-resume picks up the same
        // closure. Bounded by MaxLateRefetchRounds to guarantee
        // liveness under continuous cascading splits - the leaf-side
        // _recentlyTerminal dedup makes re-targeting an already-
        // terminalled shard a safe no-op, so the cap is a wall-
        // clock guard, not a correctness guard.
        if (registry is not null)
        {
            const int MaxLateRefetchRounds = 5;
            var terminalled = new HashSet<int>(state.State.TouchedShards);
            for (var latePass = 0; latePass < MaxLateRefetchRounds; latePass++)
            {
                var freshParticipants = await registry.GetParticipantsAsync(transactionId);
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG broadcast-late-pass-fetch] op={OperationKey} tx={transactionId} pass={latePass} participants=[{string.Join(",", freshParticipants)}] alreadyTerminalled=[{string.Join(",", terminalled)}]");
#endif
                if (freshParticipants.Count == 0) break;
                var newlyArrived = new List<int>();
                foreach (var s in freshParticipants)
                {
                    if (!terminalled.Contains(s)) newlyArrived.Add(s);
                }
                if (newlyArrived.Count == 0) break;

                var expanded = await TerminalFanOutResolver.ResolveTransitiveAsync(
                    grainFactory,
                    physicalTreeId,
                    newlyArrived,
                    CancellationToken.None);
                var lateToSend = new List<int>();
                foreach (var s in expanded)
                {
                    if (terminalled.Add(s)) lateToSend.Add(s);
                }
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG broadcast-late-pass-send] op={OperationKey} tx={transactionId} pass={latePass} newlyArrived=[{string.Join(",", newlyArrived)}] expanded=[{string.Join(",", expanded)}] lateToSend=[{string.Join(",", lateToSend)}]");
#endif
                if (lateToSend.Count == 0) break;

                var latePending = new List<Task<WalRecord?>>(lateToSend.Count);
                foreach (var shardIndex in lateToSend)
                {
                    IReadOnlyDictionary<string, byte[]>? subset = null;
                    if (perShardCommitted is not null)
                    {
                        if (perShardCommitted.TryGetValue(shardIndex, out var existing))
                        {
                            subset = existing;
                        }
                        else if (committed && state.State.Entries.Count > 0)
                        {
                            if (fullBackstop is null)
                            {
                                fullBackstop = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                                foreach (var entry in state.State.Entries)
                                    fullBackstop[entry.Key] = entry.Value;
                            }
                            perShardCommitted[shardIndex] = fullBackstop;
                            subset = fullBackstop;
                        }
                    }
                    latePending.Add(MarkOneShardAsync(physicalTreeId, shardIndex, transactionId, committed, subset));
                }
                var lateRecords = await Task.WhenAll(latePending);
                await FlushPendingTerminalsAsync(lateRecords);

                var sortedSent = new List<int>(terminalled);
                sortedSent.Sort();
                state.State.TouchedShards = sortedSent;
                await WriteSagaStateAsync("broadcast-touched-late");
            }
        }

#if LATTICE_DIAG
        DiagSink.Write($"[DIAG broadcast-done] op={OperationKey} tx={transactionId} finalTouched=[{string.Join(",", state.State.TouchedShards)}]");
#endif
    }

    /// <summary>
    /// Resolves the optional <see cref="ICommitLogWriter"/> registered
    /// by <c>AddLattice</c>. Cached after first lookup. Returns
    /// <c>null</c> on single-node / unit-test deployments that bypass
    /// the WAL adapter entirely - in which case the per-shard
    /// <see cref="ShardRootGrain.AppendTxTerminalAsync"/> calls also
    /// return null and there is nothing to flush.
    /// </summary>
    private ICommitLogWriter? ResolveCommitLogWriter()
    {
        if (_commitLogWriterResolved) return _commitLogWriter;
        _commitLogWriterResolved = true;
        _commitLogWriter = GrainContext.ActivationServices?.GetService<ICommitLogWriter>();
        return _commitLogWriter;
    }
    private bool _commitLogWriterResolved;
    private ICommitLogWriter? _commitLogWriter;

    /// <summary>
    /// c2-xxiii batched-WAL durability barrier. Filters out the null
    /// records produced by the no-WAL-adapter path and by
    /// already-marked / Guid.Empty / stale-routing-no-op shards, then
    /// dispatches the remainder through
    /// <see cref="ICommitLogWriter.AppendManyAsync"/> which groups by
    /// WAL partition and fans out one batched <see cref="IWalShardGrain.AppendBatchAsync"/>
    /// call per partition in parallel. Awaits all partition writes so
    /// the saga still observes WAL durability before returning - only
    /// the dispatcher shape changes. A null or empty record list is a
    /// no-op so single-node / unit-test deployments behave
    /// historically.
    /// </summary>
    private async Task FlushPendingTerminalsAsync(WalRecord?[] records)
    {
        if (records is null || records.Length == 0) return;
        var writer = ResolveCommitLogWriter();
        if (writer is null) return;
        List<WalRecord>? buffer = null;
        for (var i = 0; i < records.Length; i++)
        {
            if (records[i] is { } r)
            {
                buffer ??= new List<WalRecord>(records.Length);
                buffer.Add(r);
            }
        }
        if (buffer is null) return;
        await writer.AppendManyAsync(buffer, CancellationToken.None);
    }

    /// <summary>
    /// Per-shard terminal append with deadline-bounded routing-refresh
    /// retry. Encapsulates the stale-routing recovery so the
    /// <see cref="BroadcastTerminalsAsync(bool)"/> fan-out body stays
    /// linear. Catches both <see cref="StaleShardRoutingException"/>
    /// (a slot moved between prepare and broadcast) and
    /// <see cref="StaleTreeRoutingException"/> (the tree's physical
    /// alias was swapped, e.g. online resize) and retries the call
    /// against a freshly-resolved owner until success or
    /// <see cref="StaleRoutingRetryBudget"/> elapses. A bounded-attempt
    /// retry was insufficient under reshard storms where many sequential
    /// ShardMap swaps can land between the prepare and broadcast
    /// windows; the wall-clock budget absorbs any reasonable storm and
    /// still terminates with the original stale-routing throw if the
    /// topology never quiesces.
    /// <para>
    /// c2-xxiii: the shard returns the constructed terminal
    /// <see cref="WalRecord"/> rather than appending it to its own WAL
    /// partition - the saga coordinator collects every touched-shard
    /// record from the parallel fan-out and dispatches them as one
    /// batched <see cref="ICommitLogWriter.AppendManyAsync"/> call so
    /// the N serialised single-entry partition transactions collapse
    /// into one per-partition batched transaction. Durability is
    /// preserved: the saga still awaits the batched write before
    /// returning. The returned record is null when no WAL adapter is
    /// registered (single-node / unit-test path) or when the shard
    /// rejected the call before constructing one.
    /// </para>
    /// </summary>
    private async Task<WalRecord?> MarkOneShardAsync(
        string physicalTreeId,
        int shardIndex,
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues)
    {
#if LATTICE_DIAG
        DiagSink.Write($"[DIAG broadcast-mark-shard] op={OperationKey} tx={transactionId} shardIndex={shardIndex} committed={committed} subsetKeys=[{(committedValues is null ? "<null>" : string.Join(",", committedValues.Keys))}]");
#endif
        // Stamp the saga's authoritative touched-shard count on the
        // ambient request context so ShardRootGrain.AppendTxTerminalAsync
        // reads it while assembling the terminal LatticeMutation and
        // stamps LatticeMutation.AtomicShardCount. This is the
        // receiver-side cross-cluster atomic-visibility gate: the
        // remote cluster's ApplyTxTerminalAsync holds back the
        // per-tree TxRegistry mark until it has tallied this many
        // distinct source-shard terminal arrivals. The value is read
        // fresh from state.State.TouchedShards.Count each call so
        // late-pass shards observe the post-union count.
        using var shardCountScope = LatticeAtomicShardCountContext.With(state.State.TouchedShards.Count);

        // The catch blocks unconditionally fire so the original
        // stale-routing throw surfaces to the caller once the wall-clock
        // budget elapses; a when-filter on the catch could race against
        // the loop condition and exit silently.
        var deadline = DateTime.UtcNow + StaleRoutingRetryBudget;
        while (true)
        {
            try
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
                return await shard.AppendTxTerminalAsync(
                    transactionId, committed, committedValues,
                    cancellationToken: CancellationToken.None,
                    inlineWalAppend: false);
            }
            catch (StaleShardRoutingException)
            {
                // Slot ownership moved (adaptive shard split). Refresh
                // routing under the same logical tree id and retry against
                // the new owner; AppendTxTerminalAsync is shard-keyed so
                // the refreshed call may resolve to a different physical
                // tree id under online resize.
                if (DateTime.UtcNow >= deadline) throw;
            }
            catch (StaleTreeRoutingException)
            {
                // Tree alias swapped mid-saga (online resize). Refresh
                // routing under the same logical tree id and retry against
                // the new physical tree.
                if (DateTime.UtcNow >= deadline) throw;
            }

            var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
            // forceRefresh:true breaks out of any cached-routing spin -
            // see the CaptureShardAsync stale-routing catch for the
            // full rationale (StatelessWorker per-activation cache,
            // private invalidation hooks).
            var refreshed = await lattice.GetRoutingAsync(forceRefresh: true);
            physicalTreeId = refreshed.PhysicalTreeId;
        }
    }

    /// <summary>
    /// Dispatches on <see cref="AtomicWriteState.Phase"/> and drives the saga
    /// to a terminal state. Throws the original failure exception (or a
    /// surrogate) after compensation completes.
    /// </summary>
    private async Task RunSagaAsync()
    {
        StampOperationIdContext();
        StampTransactionIdContext();
        StampDeltaContext();
        StampVectorClockContext();
        StampAtomicBatchContext();

        // Phase-attribution instrumentation (c2-xv routing memo).
        // The saga's three internal phases - parallel prepare via
        // ExecutePhaseAsync, terminal-decision write via
        // RecordTerminalDecisionAsync, per-shard terminal broadcast via
        // BroadcastTerminalsAsync - are timed independently so the
        // dashboards can decompose the saga's end-to-end p50 across
        // the three contributors. The c2-xi memo's residual-cost
        // attribution was inconclusive at the single-cluster bench
        // (c2-xv memo, Phase D2-A null result); these histograms
        // close the attribution gap so the next optimisation step
        // targets the actual binding constraint.
        var (sagaTreeTag, sagaWalPartitionsTag) = GetSagaMetricTags();

        if (state.State.Phase == AtomicWritePhase.Prepare)
        {
            // Crash before execute was persisted - replay Prepare.
            var entries = state.State.Entries;
            await PrepareAsync(state.State.TreeId, entries);
        }

        if (state.State.Phase == AtomicWritePhase.Execute)
        {
            var prepareStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                await ExecutePhaseAsync();
            }
            finally
            {
                LatticeMetrics.SagaPrepareDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(prepareStartTicks).TotalMilliseconds,
                    sagaTreeTag,
                    sagaWalPartitionsTag);
            }
        }

        if (state.State.Phase == AtomicWritePhase.Compensate)
        {
            // Saga aborted mid-Execute. The prepare-phase writes were
            // stamped IsPrepared=true and bucketed into each leaf's
            // pending-tx map - they are NOT visible to readers, so
            // per-key compensation rolls would be both unnecessary
            // and harmful (a non-prepared rollback write would land
            // as a fresh visible entry that subsequently overlays
            // the pre-saga state once the abort terminal drops the
            // pending bucket on the same leaf).
            //
            // Strict per-tree atomic-visibility: record the abort
            // decision in the per-tree TxRegistry BEFORE fanning out
            // the per-leaf abort terminals. This is the single
            // tree-wide linearization point on the rollback side -
            // any leaf serving a read against this saga's pending
            // bucket between this call and the terminal fan-out
            // resolves to the pre-saga value via the registry, so
            // readers never observe a partial rollback.
            var decisionStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                await RecordTerminalDecisionAsync(committed: false);
            }
            finally
            {
                LatticeMetrics.SagaTerminalDecisionDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(decisionStartTicks).TotalMilliseconds,
                    sagaTreeTag,
                    sagaWalPartitionsTag);
            }
            var broadcastStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                await BroadcastTerminalsAsync(committed: false);
            }
            finally
            {
                LatticeMetrics.SagaBroadcastDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(broadcastStartTicks).TotalMilliseconds,
                    sagaTreeTag,
                    sagaWalPartitionsTag);
            }
            await CompleteSagaAsync(success: false);
            throw new InvalidOperationException(
                $"Atomic write saga for tree '{state.State.TreeId}' failed and was rolled back: " +
                (state.State.FailureMessage ?? "unknown failure"));
        }

        if (state.State.Phase == AtomicWritePhase.Execute && state.State.NextIndex >= state.State.Entries.Count)
        {
            // Every prepare-phase write succeeded.
            //
            // Strict per-tree atomic-visibility: record the commit
            // decision in the per-tree TxRegistry BEFORE fanning out
            // the per-leaf commit terminals. This is the single
            // tree-wide linearization point on the commit side - any
            // leaf serving a read against this saga's pending bucket
            // between this call and the terminal fan-out resolves to
            // the prepared (post-saga) value via the registry, so
            // readers never observe a partial commit.
            //
            // A crash between the registry write and the Completed
            // flip leaves the saga in Execute (NextIndex == Entries.Count); reminder-driven re-entry observes the
            // post-loop condition, re-runs the registry write
            // (idempotent), re-runs the broadcast (idempotent via
            // the leaf-side recently-terminal dedup), and proceeds
            // to CompleteSagaAsync.
            var decisionStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                await RecordTerminalDecisionAsync(committed: true);
            }
            finally
            {
                LatticeMetrics.SagaTerminalDecisionDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(decisionStartTicks).TotalMilliseconds,
                    sagaTreeTag,
                    sagaWalPartitionsTag);
            }
            var broadcastStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                await BroadcastTerminalsAsync(committed: true);
            }
            finally
            {
                LatticeMetrics.SagaBroadcastDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(broadcastStartTicks).TotalMilliseconds,
                    sagaTreeTag,
                    sagaWalPartitionsTag);
            }
            await CompleteSagaAsync(success: true);
        }
    }

    /// <summary>
    /// Records the saga's terminal commit/abort decision on the per-tree
    /// <see cref="ITxRegistryGrain"/> before the per-leaf terminal
    /// fan-out begins. The registry write is the single tree-wide
    /// linearization point that delivers strict atomic-visibility:
    /// every leaf reader that sees the saga in its pending bucket
    /// dials back through the registry to resolve the read against the
    /// already-recorded outcome, so the post-fan-out window in which
    /// some leaves have flipped and others have not is invisible to
    /// readers. Idempotent - reminder-driven re-entry after a crash
    /// between the registry write and the saga's Completed flip is
    /// safe because both <c>MarkCommittedAsync</c> and
    /// <c>MarkAbortedAsync</c> treat repeated same-outcome calls as
    /// no-ops.
    /// </summary>
    private Task RecordTerminalDecisionAsync(bool committed)
    {
        var txid = state.State.TransactionId;
        if (txid == Guid.Empty) return Task.CompletedTask;
        var registry = grainFactory.GetGrain<ITxRegistryGrain>(state.State.TreeId);
        return committed
            ? registry.MarkCommittedAsync(txid)
            : registry.MarkAbortedAsync(txid);
    }

    /// <summary>
    /// Applies every entry in <see cref="State.AtomicWriteState.Entries"/>
    /// from <see cref="State.AtomicWriteState.NextIndex"/> onward. Under
    /// Phase D step D1 (c2-viii memo): the historically-sequential
    /// per-key loop becomes a bounded-parallelism fan-out under
    /// <c>Task.WhenAll</c>, capped at the tree's
    /// <see cref="LatticeOptions.WalPartitions"/> (so the fan-out
    /// matches the WAL partition count by construction and never
    /// over-saturates the per-partition pipeline). Same-key chaining
    /// is unnecessary because <see cref="ValidateInputs"/> guarantees
    /// the batch contains no duplicate keys; the prepare-time
    /// <see cref="LatticePreparedContext"/> scope still applies to
    /// every parallel write so the leaf pending-tx map keys correctly
    /// and atomic-visibility is preserved by the per-tree
    /// <see cref="ITxRegistryGrain"/> linearization point invoked
    /// after the fan-out completes (see <see cref="RunSagaAsync"/>).
    /// <para>
    /// Crash-recovery: the per-key checkpoint that previously advanced
    /// <see cref="State.AtomicWriteState.NextIndex"/> after every
    /// committed key is replaced with a single post-fan-out persist
    /// (set to <see cref="State.AtomicWriteState.Entries"/>.Count when
    /// the whole batch succeeds, or rolled back via Class-B revert
    /// when the persist throws). A crash mid-fan-out leaves
    /// <c>NextIndex</c> at its pre-batch value; reactivation re-runs
    /// every entry. Re-running prepared writes is idempotent at the
    /// leaf - <c>AddPreparedMutation</c> merges via
    /// <c>LwwValue.Merge</c> on duplicate <c>(transactionId, key)</c>
    /// pairs and the saga's terminal <c>MarkCommittedAsync</c> is the
    /// single visibility gate, so a re-run produces a bit-identical
    /// post-saga state.
    /// </para>
    /// <para>
    /// Retry semantics: <see cref="MaxRetriesPerStep"/> is reinterpreted
    /// as a per-batch retry budget rather than per-key. On any task's
    /// failure the whole unwritten remainder is re-attempted; on
    /// budget exhaustion the saga pivots to Compensate exactly as the
    /// pre-c2-viii sequential loop did.
    /// </para>
    /// </summary>
    private async Task ExecutePhaseAsync()
    {
        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
#if LATTICE_DIAG
        var swPhase = System.Diagnostics.Stopwatch.StartNew();
        DiagSink.Write($"[DIAG saga-execute-phase-enter] op={OperationKey} tree={state.State.TreeId} nextIndex={state.State.NextIndex} totalEntries={state.State.Entries.Count} retriesOnCurrentStep={state.State.RetriesOnCurrentStep}");
#endif

        // Phase A horizontal-scaling diagnostic: publish the saga's
        // fan-out size once per execute-phase entry (regardless of
        // whether the activation later compensates or completes),
        // tagged by tree only.
        //
        // Tags are sourced from the per-activation cache populated by
        // RunSagaAsync so we do not re-allocate the KeyValuePair pair
        // (and re-box the WalPartitions int) on every saga.
        var (sagaTreeTag, sagaWalPartitionsTag) = GetSagaMetricTags();
        LatticeMetrics.SagaFanoutSize.Record(state.State.Entries.Count, sagaTreeTag, sagaWalPartitionsTag);

        // Phase D1b (c2-ix memo): collapse the D1 per-key
        // Task.WhenAll-of-N-SetAsync fan-out into a single
        // ILattice.SetManyAsync call per batch. The leaf's
        // CommitSetManyAsync now handles isPrepared=true via
        // AddPreparedMutation after a single
        // ICommitLogWriter.AppendManyAsync dispatch per partition;
        // D1's phase-A data showed each per-key SetAsync dispatched
        // as a size-1 WAL batch with wal.append.in_flight=0 throughout,
        // so the saga was paying N WAL round-trips when one would do.
        //
        // Per-entry global index preservation across shard bucketing:
        // LatticeGrain.SetManyAsync re-groups the saga's flat entry
        // list into per-shard buckets and dispatches one
        // IShardRootGrain.SetManyAsync per shard. Bucket-local
        // position no longer equals saga-global position, so the leaf
        // cannot stamp AtomicBatchIndex as BaseIndex + bucketLocal
        // alone - that would produce 4 records all stamped "index 0
        // of size 4" when the 4 saga keys hash to 4 different shards
        // and break receiver-side cross-cluster atomic visibility.
        // The saga therefore stamps an additional `key -> globalIndex`
        // map alongside the (Size, BaseIndex) pair via the
        // LatticeAtomicBatchContext.With(batch, indexMap) overload;
        // the leaf's CommitSetManyAsync looks each entry's key up in
        // the map to recover its saga-global index regardless of how
        // SetManyAsync routed the entries to leaves.
        //
        // Crash-recovery: the per-key checkpoint that previously
        // advanced NextIndex after every committed key was replaced
        // in D1 with a single post-batch persist; D1b keeps the same
        // semantics (set NextIndex to Entries.Count when the whole
        // batch succeeds, Class-B revert on persist throw). A crash
        // mid-batch leaves NextIndex at its pre-batch value;
        // reactivation re-runs every entry. Re-running prepared
        // writes is idempotent at the leaf - AddPreparedMutation
        // merges via LwwValue.Merge on duplicate (transactionId, key)
        // pairs and the saga's terminal MarkCommittedAsync is the
        // single visibility gate.
        //
        // Retry semantics: MaxRetriesPerStep is the per-batch retry
        // budget (carried forward from D1). On batch failure the
        // whole unwritten remainder is re-attempted; on budget
        // exhaustion the saga pivots to Compensate exactly as the
        // pre-c2-viii sequential loop did.
        using (LatticePreparedContext.BeginScope())
        {
            while (state.State.NextIndex < state.State.Entries.Count)
            {
                var startIndex = state.State.NextIndex;
                var totalEntries = state.State.Entries.Count;
                var remaining = totalEntries - startIndex;

#if LATTICE_DIAG
                var swBatch = System.Diagnostics.Stopwatch.StartNew();
                DiagSink.Write($"[DIAG saga-execute-batch-enter] op={OperationKey} tree={state.State.TreeId} startIndex={startIndex} totalEntries={totalEntries} retries={state.State.RetriesOnCurrentStep}");
#endif

                // Build the per-batch entry slice plus a parallel
                // key -> globalIndex map. The slice covers every
                // unwritten entry; the indexMap lets the leaf-side
                // batched commit path stamp each per-entry WAL record
                // with its true saga-global AtomicBatchIndex regardless
                // of which shard's bucket the entry ends up in after
                // LatticeGrain.SetManyAsync's shard-bucketing fan-out.
                var slice = new List<KeyValuePair<string, byte[]>>(remaining);
                var indexMap = new Dictionary<string, int>(remaining);
                for (var i = startIndex; i < totalEntries; i++)
                {
                    var entry = state.State.Entries[i];
                    slice.Add(entry);
                    indexMap[entry.Key] = i;
                }

                Exception? batchFailure = null;
                var batchStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
                try
                {
                    // Phase D1c (post-c2-xi): restored the single-call
                    // shape of D1 - one ILattice.SetManyAsync covering
                    // the whole unwritten slice. The shard-bucketing
                    // fan-out inside LatticeGrain.SetManyAsync runs
                    // cross-leaf calls in parallel via Task.WhenAll,
                    // giving the saga back its concurrent per-shard
                    // dispatch. D1b's per-shard SERIAL dispatch -
                    // installed as the load-bearing fix for the
                    // changefeed's HLC-cursor inversion-drop bug - is
                    // no longer required because the changefeed cursor
                    // is now per-partition WAL offset, which IS
                    // monotonic per partition by construction (offsets
                    // are assigned under the WAL grain activation's
                    // lock at append time). The HLC ordering of WAL
                    // records may still interleave across leaves, but
                    // the consume-side filter no longer drops entries
                    // by HLC, so cross-leaf HLC interleaving is benign.
                    //
                    // The (Size, BaseIndex) + key->globalIndex ambient
                    // remains required: LatticeGrain.SetManyAsync still
                    // buckets entries by shard, and the leaf-side
                    // CommitSetManyAsync needs the index map to stamp
                    // each per-entry WAL record's AtomicBatchIndex
                    // from the saga's global slot (bucket-local
                    // position does not equal saga-global position).
                    using (LatticeAtomicBatchContext.With(
                        (state.State.AtomicBatchSize, startIndex),
                        indexMap))
                    {
                        await lattice.SetManyAsync(slice).ConfigureAwait(true);
                    }
                }
                catch (Exception ex)
                {
                    batchFailure = ex;
#if LATTICE_DIAG
                    DiagSink.Write($"[DIAG saga-execute-batch-fault] op={OperationKey} tree={state.State.TreeId} startIndex={startIndex} totalEntries={totalEntries} ex={batchFailure.GetType().Name} msg={batchFailure.Message.Replace(System.Environment.NewLine, " | ")}");
#endif
                }

                // Record per-key duration as elapsed / count - the
                // batched dispatch makes individual per-key timing
                // unrecoverable from the saga's vantage point, but the
                // average is the right shape for the per-key duration
                // histogram (matches how dashboards consume it).
                var batchElapsedMs = System.Diagnostics.Stopwatch.GetElapsedTime(batchStartTicks).TotalMilliseconds;
                if (remaining > 0)
                {
                    var perKeyAvgMs = batchElapsedMs / remaining;
                    for (var i = 0; i < remaining; i++)
                    {
                        LatticeMetrics.SagaPerKeyDuration.Record(perKeyAvgMs, sagaTreeTag, sagaWalPartitionsTag);
                    }
                }

                if (batchFailure is null)
                {
                    // Whole batch committed - single post-batch
                    // checkpoint advancing NextIndex to Entries.Count.
                    // Class B snapshot/restore: identical contract to
                    // the D1 post-fan-out persist.
                    var prevNextIndex = state.State.NextIndex;
                    var prevRetriesOnCurrentStep = state.State.RetriesOnCurrentStep;
                    state.State.NextIndex = totalEntries;
                    state.State.RetriesOnCurrentStep = 0;
                    try
                    {
                        await WriteSagaStateAsync("execute-batch-commit");
                    }
                    catch
                    {
                        state.State.NextIndex = prevNextIndex;
                        state.State.RetriesOnCurrentStep = prevRetriesOnCurrentStep;
                        throw;
                    }
#if LATTICE_DIAG
                    DiagSink.Write($"[DIAG saga-execute-batch-exit] op={OperationKey} tree={state.State.TreeId} startIndex={startIndex} totalEntries={totalEntries} elapsedMs={swBatch.Elapsed.TotalMilliseconds:F0}");
#endif
                    break;
                }

                // Batch failed. Identical retry / compensate-pivot
                // contract to D1's per-batch loop.
                if (state.State.RetriesOnCurrentStep < MaxRetriesPerStep)
                {
#if LATTICE_DIAG
                    DiagSink.Write($"[DIAG saga-execute-batch-retry] op={OperationKey} tree={state.State.TreeId} startIndex={startIndex} retries={state.State.RetriesOnCurrentStep} ex={batchFailure.GetType().Name} msg={batchFailure.Message.Replace(System.Environment.NewLine, " | ")}");
#endif
                    var prevRetriesOnCurrentStep = state.State.RetriesOnCurrentStep;
                    state.State.RetriesOnCurrentStep++;
                    try
                    {
                        await WriteSagaStateAsync("execute-batch-retry");
                    }
                    catch
                    {
                        state.State.RetriesOnCurrentStep = prevRetriesOnCurrentStep;
                        throw;
                    }
                    Logger.LogWarning(batchFailure,
                        "Atomic-write saga {OperationKey}: retrying batch from index {Index} (attempt {Attempt}).",
                        OperationKey, state.State.NextIndex, state.State.RetriesOnCurrentStep);
                    continue;
                }

                // Exhausted retries - pivot to compensation. Class B
                // snapshot/restore: identical contract to D1's
                // compensate-pivot site.
                var prevPhase = state.State.Phase;
                var prevFailureMessage = state.State.FailureMessage;
                var prevRetriesOnCurrentStepPivot = state.State.RetriesOnCurrentStep;
                state.State.Phase = AtomicWritePhase.Compensate;
                state.State.FailureMessage = batchFailure.Message;
                state.State.RetriesOnCurrentStep = 0;
                try
                {
                    await WriteSagaStateAsync("execute-to-compensate");
                }
                catch
                {
                    state.State.Phase = prevPhase;
                    state.State.FailureMessage = prevFailureMessage;
                    state.State.RetriesOnCurrentStep = prevRetriesOnCurrentStepPivot;
                    throw;
                }
                return;
            }
        }

#if LATTICE_DIAG
        DiagSink.Write($"[DIAG saga-execute-phase-exit] op={OperationKey} tree={state.State.TreeId} nextIndex={state.State.NextIndex} totalEntries={state.State.Entries.Count} elapsedMs={swPhase.Elapsed.TotalMilliseconds:F0}");
#endif
    }

    /// <summary>
    /// Marks the saga Completed, unregisters the keepalive reminder, arms the
    /// retention reminder (via the shared TtlGrain base) for delayed state
    /// cleanup, and requests deactivation. Safe to call in both success and
    /// post-compensation paths. <paramref name="success"/> gates emission of
    /// the terminal <see cref="LatticeTreeEventKind.AtomicWriteCompleted"/>
    /// event - rolled-back sagas do not publish a completion event because
    /// no write actually committed.
    /// </summary>
    private async Task CompleteSagaAsync(bool success)
    {
        // Class B snapshot/restore at Site 8 (CompleteSagaAsync terminal
        // persist). A failure here would leave Phase=Completed in memory
        // while disk still says Execute. The ExecuteAsync L159
        // Phase==Completed short-circuit then reports false success on
        // every retry from the same activation, but a reactivation finds
        // disk at Execute and re-runs the entire saga.
        var prevPhase = state.State.Phase;
        var prevRetriesOnCurrentStep = state.State.RetriesOnCurrentStep;
        state.State.Phase = AtomicWritePhase.Completed;
        state.State.RetriesOnCurrentStep = 0;
        try
        {
            await WriteSagaStateAsync("complete");
        }
        catch
        {
            state.State.Phase = prevPhase;
            state.State.RetriesOnCurrentStep = prevRetriesOnCurrentStep;
            throw;
        }
        await UnregisterKeepaliveAsync();
        await SlideTtlAsync();

        // Emit a terminal outcome counter for operators. "committed" = all
        // writes applied; "failed" = compensation ran after a Prepare/Execute
        // failure surrogate was recorded; "compensated" = rolled back for a
        // reason that was not captured as a surrogate failure (e.g. explicit
        // caller cancellation path).
        var outcome = success
            ? "committed"
            : (state.State.FailureMessage is not null ? "failed" : "compensated");
        LatticeMetrics.AtomicWriteCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcome));

        // End-to-end saga duration and batch size, paired histograms tagged
        // by the same {tree, outcome} dimensions as AtomicWriteCompleted so
        // operators can pivot from "how many sagas committed?" to "how long
        // did they take?" and "how big were they?" on the same dashboard.
        // SagaStartedAtTicks is captured once on the first PrepareAsync and
        // persisted across reminder-driven recovery, so the recorded ms
        // reflects true wall-clock cost (including any time the saga was
        // suspended across silo restarts). A legacy persisted state with a
        // missing Id-17 slot decodes SagaStartedAtTicks to 0; the duration
        // record is suppressed in that case rather than emit a misleadingly
        // huge "ticks since 0001-01-01" value.
        if (state.State.SagaStartedAtTicks > 0)
        {
            var elapsedMs = (DateTimeOffset.UtcNow.UtcTicks - state.State.SagaStartedAtTicks)
                / (double)TimeSpan.TicksPerMillisecond;
            LatticeMetrics.AtomicWriteDuration.Record(elapsedMs,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcome));
        }

        LatticeMetrics.AtomicWriteBatchSize.Record(state.State.Entries.Count,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcome));

        // Publish AtomicWriteCompleted only when the saga committed all writes.
        // Rolled-back sagas emitted per-key Set events during ExecutePhase but
        // compensated them back via LWW tombstones/restores; there is no net
        // write to notify subscribers about.
        if (success)
        {
            await PublishCompletedEventAsync();
        }

        // Strict per-tree atomic-visibility cleanup: every touched leaf
        // has now applied its terminal (and therefore drained its
        // pending-tx bucket for this saga), so no leaf will ever
        // consult the registry for this txid again. Forget the
        // decision so the registry's persisted footprint stays
        // bounded by in-flight + recently-completed sagas. Best-effort:
        // a transient failure here leaves a tombstone behind that the
        // next saga on the same tree will eventually amortise; it is
        // not worth a retry loop on the saga critical path.
        try
        {
            var txid = state.State.TransactionId;
            if (txid != Guid.Empty)
            {
                var registry = grainFactory.GetGrain<ITxRegistryGrain>(state.State.TreeId);
                await registry.ForgetAsync(txid);
            }
        }
        catch
        {
            // Swallow - registry GC is non-critical.
        }

        this.DeactivateOnIdle();
    }

    /// <summary>
    /// Extracts <c>operationId</c> from the composite grain key
    /// (<c>{treeId}/{operationId}</c>) and stamps it into Orleans
    /// <see cref="RequestContext"/> so downstream <c>SetAsync</c> /
    /// <c>DeleteAsync</c> calls made by this saga carry the correlation id
    /// onto their emitted <see cref="LatticeTreeEvent"/>s.
    /// </summary>
    private void StampOperationIdContext()
    {
        var idx = OperationKey.IndexOf('/');
        if (idx < 0 || idx == OperationKey.Length - 1) return;
        var opId = OperationKey[(idx + 1)..];
        RequestContext.Set(LatticeEventConstants.OperationIdRequestContextKey, opId);
    }

    /// <summary>
    /// Stamps the saga's persisted transaction id onto Orleans
    /// <see cref="RequestContext"/> so every per-key <c>SetAsync</c> /
    /// <c>DeleteAsync</c> call the saga makes - including compensation
    /// rewrites - surfaces with the same
    /// <see cref="LatticeMutation.TransactionId"/>. Lazily mints an id
    /// when persisted state is empty (e.g. legacy persisted state or a
    /// reactivation after a crash that pre-dated this field) so resumed
    /// sagas always share a non-empty id across their remaining emits.
    /// </summary>
    private void StampTransactionIdContext()
    {
        if (state.State.TransactionId == Guid.Empty)
        {
            state.State.TransactionId = Guid.NewGuid();
        }
        LatticeTransactionContext.Set(state.State.TransactionId);
    }

    /// <summary>
    /// Re-establishes the saga's persisted author-delta carry on Orleans
    /// <see cref="RequestContext"/> so every per-key <c>SetAsync</c> /
    /// <c>DeleteAsync</c> the saga issues - including compensation
    /// rewrites - surfaces with the same
    /// <see cref="LatticeMutation.Delta"/> as the original batch.
    /// No-op when the caller did not supply a delta context on the first
    /// <see cref="ExecuteAsync"/> call.
    /// </summary>
    private void StampDeltaContext()
    {
        if (state.State.Delta is null) return;
        LatticeDeltaContext.Current = state.State.Delta;
    }

    /// <summary>
    /// Re-establishes the saga's persisted vector-clock frontier on
    /// Orleans <see cref="RequestContext"/> so every per-key
    /// <c>SetAsync</c> the saga issues during the
    /// <see cref="AtomicWritePhase.Execute"/> phase emits a
    /// <see cref="LatticeMutation"/> carrying the identical
    /// <see cref="LatticeMutation.VectorClock"/> across the batch.
    /// Compensation rolls override this per-key with each
    /// <see cref="AtomicPreValue.VectorClock"/> via
    /// <see cref="LatticeVectorClockContext.With"/>; the saga-wide
    /// stamp is restored when each rollback's scope disposes.
    /// Setting <see langword="null"/> explicitly clears any stale
    /// ambient context inherited from the reminder-driven activation
    /// so a saga that captured a null frontier emits null verbatim.
    /// </summary>
    private void StampVectorClockContext()
    {
        LatticeVectorClockContext.Current = state.State.VectorClock;
    }

    /// <summary>
    /// Re-establishes the saga's persisted batch size on Orleans
    /// <see cref="RequestContext"/> via
    /// <see cref="LatticeAtomicBatchContext"/> as a saga-wide
    /// <c>(Size, Index=0)</c> default at the head of every
    /// <see cref="RunSagaAsync"/> entry. The execute and compensate
    /// per-key loops override the index inside their own
    /// <see cref="LatticeAtomicBatchContext.With"/> scopes; the
    /// saga-wide stamp is what reminder-driven re-entry observes
    /// before the per-key loop runs and what an uncaught throw out
    /// of a per-key scope leaves visible to any post-saga publish
    /// helper. Persisted-zero (legacy state from before this field
    /// existed, or a saga that never entered Prepare) clears the
    /// ambient explicitly so a single-key non-saga write running on
    /// the same activation observes the absent default.
    /// </summary>
    private void StampAtomicBatchContext()
    {
        LatticeAtomicBatchContext.Current = state.State.AtomicBatchSize > 0
            ? (state.State.AtomicBatchSize, 0)
            : null;
    }

    private async Task PublishCompletedEventAsync()
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId)) return;
        var options = optionsMonitor.Get(treeId);
        if (!await _eventsGate.IsEnabledAsync(grainFactory, treeId, options)) return;
        var idx = OperationKey.IndexOf('/');
        var opId = idx < 0 || idx == OperationKey.Length - 1 ? null : OperationKey[(idx + 1)..];
        var evt = new LatticeTreeEvent
        {
            Kind = LatticeTreeEventKind.AtomicWriteCompleted,
            TreeId = treeId,
            Key = null,
            ShardIndex = null,
            OperationId = opId,
            AtUtc = DateTimeOffset.UtcNow,
        };
        await LatticeEventPublisher.PublishAsync(GrainContext.ActivationServices, options, evt, Logger);
    }

    private readonly PublishEventsGate _eventsGate = new();

    /// <summary>
    /// Cached per-activation metric tags. The two histograms
    /// (<see cref="LatticeMetrics.SagaFanoutSize"/>,
    /// <see cref="LatticeMetrics.SagaPerKeyDuration"/>,
    /// <see cref="LatticeMetrics.SagaPrepareDuration"/>,
    /// <see cref="LatticeMetrics.SagaTerminalDecisionDuration"/>,
    /// <see cref="LatticeMetrics.SagaBroadcastDuration"/>) all share
    /// the same <c>(tree, walPartitions)</c> tag pair. The pair is
    /// constructed once per activation (per-saga lifetime, since
    /// AtomicWriteGrain uses per-operation grain keys) and reused
    /// across every <c>Record</c> call. This avoids allocating two
    /// fresh <see cref="KeyValuePair{TKey, TValue}"/> boxes (the
    /// int <c>walPartitions</c> is boxed into <c>object?</c>) per
    /// histogram observation. The cache also closes the
    /// pre-instrumentation duplication where both
    /// <see cref="RunSagaAsync"/> and <see cref="ExecutePhaseAsync"/>
    /// independently rebuilt the same pair per saga.
    /// </summary>
    private (KeyValuePair<string, object?> Tree, KeyValuePair<string, object?> WalPartitions)? _sagaMetricTags;

    /// <summary>
    /// Returns the cached per-saga metric tag pair, building it on
    /// first access. See <see cref="_sagaMetricTags"/> for the
    /// allocation-reduction rationale.
    /// </summary>
    private (KeyValuePair<string, object?> Tree, KeyValuePair<string, object?> WalPartitions) GetSagaMetricTags()
    {
        if (_sagaMetricTags is { } cached)
        {
            return cached;
        }
        // Source the tree id from the grain key rather than
        // state.State.TreeId so the cache is correctly populated even
        // when the first observation fires before PrepareAsync has
        // initialised the persisted state (e.g. RegisterKeepaliveAsync
        // runs at saga entry, before PrepareAsync sets
        // state.State.TreeId; reading from state at that point would
        // cache a null tree tag for the activation's lifetime).
        // The grain key is "{treeId}/{operationId}", so the prefix
        // before the first slash is the tree id.
        var grainKey = GrainContext.GrainId.Key.ToString()!;
        var slashIndex = grainKey.IndexOf('/');
        var treeIdFromKey = slashIndex >= 0 ? grainKey.Substring(0, slashIndex) : grainKey;
        var built = (
            Tree: new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeIdFromKey),
            WalPartitions: new KeyValuePair<string, object?>(
                LatticeMetrics.TagWalPartitions,
                optionsMonitor.Get(treeIdFromKey).WalPartitions));
        _sagaMetricTags = built;
        return built;
    }

    /// <summary>
    /// Wraps a single <c>state.WriteStateAsync()</c> call with timing
    /// instrumentation: records the wall-clock duration on
    /// <see cref="LatticeMetrics.SagaCheckpointDuration"/> tagged with
    /// the saga's tree, wal-partitions, and the supplied
    /// <paramref name="phase"/> tag identifying the call site.
    /// <para>
    /// Used in place of <c>await state.WriteStateAsync()</c> at every
    /// persist site on this grain so the per-saga checkpoint cost can
    /// be decomposed across the ~10 distinct sites without joining
    /// instruments at dashboard time. The <c>try/finally</c> ensures
    /// the histogram captures even on the failure path; the caller's
    /// surrounding rollback <c>try/catch</c> still observes the
    /// original exception unchanged.
    /// </para>
    /// <para>
    /// Per-call allocation: zero heap allocations beyond the
    /// <see cref="KeyValuePair{TKey, TValue}"/> struct passed to
    /// <c>Record</c> (stack-allocated; the <see cref="string"/>
    /// <paramref name="phase"/> value avoids the int-boxing that the
    /// wal-partitions tag pays once-per-activation in
    /// <see cref="GetSagaMetricTags"/>).
    /// </para>
    /// </summary>
    /// <param name="phase">
    /// Short string tag identifying the call site (e.g. <c>"prepare"</c>,
    /// <c>"execute-batch-commit"</c>, <c>"complete"</c>). Should be a
    /// string literal so it is interned and observation-time
    /// allocation is bounded to the struct itself.
    /// </param>
    private async Task WriteSagaStateAsync(string phase)
    {
        var (treeTag, walTag) = GetSagaMetricTags();
        var startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await state.WriteStateAsync();
        }
        finally
        {
            LatticeMetrics.SagaCheckpointDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds,
                treeTag,
                walTag,
                new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, phase));
        }
    }


    /// <summary>
    /// Re-throws a remembered failure when the caller re-invokes a terminal
    /// but failed saga. The grain normally deactivates on completion so this
    /// is mainly a defensive path for short-lived re-entry.
    /// </summary>
    private Task TryThrowFailureAsync()
    {
        if (state.State.FailureMessage is not null)
        {
            throw new InvalidOperationException(
                $"Atomic write saga for tree '{state.State.TreeId}' previously failed and was rolled back: " +
                state.State.FailureMessage);
        }
        return Task.CompletedTask;
    }

    private async Task RegisterKeepaliveAsync()
    {
        var (treeTag, walTag) = GetSagaMetricTags();
        var startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await ReminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: GrainContext.GrainId,
                reminderName: KeepaliveReminderName,
                dueTime: TimeSpan.FromMinutes(1),
                period: TimeSpan.FromMinutes(1));
        }
        finally
        {
            LatticeMetrics.SagaReminderDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds,
                treeTag,
                walTag,
                new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, "register"));
        }
    }

    private async Task UnregisterKeepaliveAsync()
    {
        var (treeTag, walTag) = GetSagaMetricTags();
        try
        {
            var getStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            IGrainReminder? reminder;
            try
            {
                reminder = await ReminderRegistry.GetReminder(GrainContext.GrainId, KeepaliveReminderName);
            }
            finally
            {
                LatticeMetrics.SagaReminderDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(getStartTicks).TotalMilliseconds,
                    treeTag,
                    walTag,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, "unregister-get"));
            }

            if (reminder is not null)
            {
                var dropStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
                try
                {
                    await ReminderRegistry.UnregisterReminder(GrainContext.GrainId, reminder);
                }
                finally
                {
                    LatticeMetrics.SagaReminderDuration.Record(
                        System.Diagnostics.Stopwatch.GetElapsedTime(dropStartTicks).TotalMilliseconds,
                        treeTag,
                        walTag,
                        new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, "unregister-drop"));
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Atomic-write saga {OperationKey}: failed to unregister keepalive reminder.",
                OperationKey);
        }
    }
}
