using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
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

        // Defensive grain-key collision guard: the apply-mode saga
        // (ExecuteApplyAsync) and the local saga (ExecuteAsync) share a
        // grain interface but use disjoint composite-key conventions
        // ({tree}/{tx:N} vs {tree}/{opId}). Reject the mismatch loudly
        // so a colliding caller-supplied opId surfaces as a caller error
        // rather than silently bleeding apply-mode state into the local
        // saga's resume path.
        if (state.State.Phase != AtomicWritePhase.NotStarted && state.State.IsApplyMode)
        {
            throw new InvalidOperationException(
                $"Atomic-write operation '{OperationKey}' grain id collides with a previously-started apply-mode saga; " +
                "use a different operationId.");
        }

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

        // Fresh saga — validate inputs, register the keepalive reminder first
        // (so a crash mid-Prepare still has a reminder-driven recovery path),
        // and then capture pre-saga state.
        if (state.State.Phase == AtomicWritePhase.NotStarted)
        {
            ValidateInputs(entries);
            await RegisterKeepaliveAsync();
            await PrepareAsync(treeId, entries);
        }

        await RunSagaAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() =>
        Task.FromResult(
            state.State.Phase == AtomicWritePhase.NotStarted ||
            state.State.Phase == AtomicWritePhase.Completed);

    /// <inheritdoc />
    public async Task<AtomicApplyResult> ExecuteApplyAsync(
        string treeId,
        List<AtomicApplyEntry> applyEntries,
        string originClusterId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(applyEntries);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        // Empty batch: fast success, no saga work, no reminder needed.
        if (applyEntries.Count == 0)
        {
            return new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Committed,
                AppliedCount = 0,
                FailureReason = null,
            };
        }

        // Validate every incoming batch — including idempotent re-entry and
        // reminder-driven resume — so a caller passing a malformed payload
        // gets a clean ArgumentException regardless of saga state, rather
        // than a downstream NullReferenceException from the projection /
        // fingerprint helpers.
        ValidateApplyInputs(applyEntries);

        // Defensive grain-key collision guard: the local saga
        // (ExecuteAsync) and the apply-mode saga (ExecuteApplyAsync) share
        // a grain interface but use disjoint composite-key conventions
        // ({tree}/{opId} vs {tree}/{tx:N}). If a caller's opId happens to
        // collide with a transactionId.N format, persistent state from one
        // path would otherwise silently bleed into the other. Reject the
        // mismatch loudly so the collision surfaces as a caller error
        // rather than a wrong AppliedCount.
        if (state.State.Phase != AtomicWritePhase.NotStarted && !state.State.IsApplyMode)
        {
            throw new InvalidOperationException(
                $"Atomic-apply saga '{OperationKey}' grain id collides with a previously-started local saga; " +
                "use a different transactionId.");
        }

        // Caller-supplied transactionId: if the same saga grain is
        // re-targeted with a different key set, reject it rather than
        // silently replaying the original persisted entries. Mirrors
        // the local saga's KeyFingerprint check. Computed directly
        // against the IReadOnlyList shape so the resume path skips the
        // per-entry projection allocation. Runs BEFORE the Completed
        // idempotent-replay early-return so a malformed retry against a
        // committed saga surfaces as a clean InvalidOperationException
        // rather than a silent no-op.
        if (state.State.Phase != AtomicWritePhase.NotStarted
            && state.State.KeyFingerprint is { } persistedFingerprint)
        {
            var incomingFingerprint = ComputeKeyFingerprint(applyEntries);
            if (!CryptographicOperations.FixedTimeEquals(persistedFingerprint, incomingFingerprint))
            {
                throw new InvalidOperationException(
                    $"Atomic-apply saga '{OperationKey}' was previously submitted with a different key set; " +
                    "reuse of a transactionId requires the exact same set of keys.");
            }
        }

        // Idempotent re-entry: a prior call has already completed this
        // saga, so the second call observes the original outcome
        // verbatim. Reconstruct the result from persisted state rather
        // than persisting an extra slot.
        if (state.State.Phase == AtomicWritePhase.Completed)
        {
            return BuildApplyResult();
        }

        // Fresh saga - validate inputs, register the keepalive reminder
        // first (so a crash mid-Prepare still has a reminder-driven
        // recovery path), and then capture pre-saga state for
        // compensation. Projection from AtomicApplyEntry to the
        // local-saga KeyValuePair shape happens only here — the
        // resume / re-entry paths above never need it.
        if (state.State.Phase == AtomicWritePhase.NotStarted)
        {
            state.State.IsApplyMode = true;
            state.State.ApplyEntries = applyEntries;
            state.State.OriginClusterId = originClusterId;
            await RegisterKeepaliveAsync();
            var derivedEntries = ProjectApplyEntriesToLocalEntries(applyEntries);
            await PrepareAsync(treeId, derivedEntries);
        }

        try
        {
            await RunSagaAsync();
        }
        catch (InvalidOperationException ex)
        {
            // RunSagaAsync throws after compensation completes for the
            // local-saga code path; for apply mode we surface the same
            // information through the structured result type. Persisted
            // state is already terminal at this point.
            return new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Compensated,
                AppliedCount = 0,
                FailureReason = state.State.FailureMessage ?? ex.Message,
            };
        }

        return BuildApplyResult();
    }

    /// <summary>
    /// Validates the apply batch: non-null keys and no duplicate keys.
    /// Tombstone entries deliberately allow <see langword="null"/>
    /// values (they carry no payload); non-tombstone entries with a
    /// <see langword="null"/> value are rejected.
    /// </summary>
    private static void ValidateApplyInputs(List<AtomicApplyEntry> applyEntries)
    {
        var seen = new HashSet<string>(applyEntries.Count, StringComparer.Ordinal);
        foreach (var entry in applyEntries)
        {
            if (entry.Key is null)
                throw new ArgumentException(
                    "Atomic apply batch contains a null key.", nameof(applyEntries));
            if (!entry.IsTombstone && entry.Value is null)
                throw new ArgumentException(
                    $"Atomic apply batch contains a null value for non-tombstone key '{entry.Key}'.",
                    nameof(applyEntries));
            if (entry.IsTombstone && entry.ExpiresAtTicks != 0)
                throw new ArgumentException(
                    $"Atomic apply batch contains a tombstone with non-zero ExpiresAtTicks for key '{entry.Key}'.",
                    nameof(applyEntries));
            if (!seen.Add(entry.Key))
                throw new ArgumentException(
                    $"Atomic apply batch contains duplicate key '{entry.Key}'.", nameof(applyEntries));
        }
    }

    /// <summary>
    /// Projects the apply-mode batch into the
    /// <see cref="AtomicWriteState.Entries"/> shape used by the existing
    /// <see cref="PrepareAsync"/> capture and <see cref="ComputeKeyFingerprint"/>
    /// helper. Tombstone entries surface as a zero-length placeholder
    /// value because the local saga's value field is non-null; the
    /// real authoritative shape lives in <see cref="AtomicWriteState.ApplyEntries"/>.
    /// </summary>
    private static List<KeyValuePair<string, byte[]>> ProjectApplyEntriesToLocalEntries(
        List<AtomicApplyEntry> applyEntries)
    {
        var derived = new List<KeyValuePair<string, byte[]>>(applyEntries.Count);
        foreach (var entry in applyEntries)
        {
            derived.Add(new KeyValuePair<string, byte[]>(
                entry.Key,
                entry.IsTombstone ? Array.Empty<byte>() : entry.Value!));
        }
        return derived;
    }

    /// <summary>
    /// Reconstructs the terminal <see cref="AtomicApplyResult"/> from
    /// persisted state. Called on idempotent re-entry and on the
    /// post-saga return path.
    /// </summary>
    private AtomicApplyResult BuildApplyResult()
    {
        if (state.State.FailureMessage is not null)
        {
            return new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Compensated,
                AppliedCount = 0,
                FailureReason = state.State.FailureMessage,
            };
        }

        return new AtomicApplyResult
        {
            Outcome = AtomicApplyOutcome.Committed,
            AppliedCount = state.State.ApplyEntries.Count,
            FailureReason = null,
        };
    }

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
    /// are not hashed — a caller retrying the same logical operation with
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

    /// <summary>
    /// Apply-mode overload of <see cref="ComputeKeyFingerprint(List{KeyValuePair{string, byte[]}})"/>
    /// that reads keys directly from the
    /// <see cref="IReadOnlyList{AtomicApplyEntry}"/> shape, avoiding the
    /// per-entry projection allocation on the resume path. The two
    /// overloads must produce identical fingerprints for the same set
    /// of keys so the apply saga's fingerprint persisted on first
    /// Prepare can be cross-checked against the apply-mode resubmit
    /// path.
    /// </summary>
    internal static byte[] ComputeKeyFingerprint(IReadOnlyList<AtomicApplyEntry> applyEntries)
    {
        var sortedKeys = new string[applyEntries.Count];
        for (int i = 0; i < applyEntries.Count; i++) sortedKeys[i] = applyEntries[i].Key;
        Array.Sort(sortedKeys, StringComparer.Ordinal);

        return ComputeKeyFingerprintCore(sortedKeys);
    }

    private static byte[] ComputeKeyFingerprintCore(string[] sortedKeys)
    {
        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lenBuf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lenBuf, sortedKeys.Length);
        sha.AppendData(lenBuf);
        foreach (var key in sortedKeys)
        {
            var bytes = Encoding.UTF8.GetBytes(key);
            BinaryPrimitives.WriteInt32LittleEndian(lenBuf, bytes.Length);
            sha.AppendData(lenBuf);
            sha.AppendData(bytes);
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
    /// interfaces — it never crosses the public <see cref="ILattice"/> surface.
    /// A <see cref="StaleShardRoutingException"/> from an adaptive shard split
    /// triggers a one-shot routing refresh and retry for the affected key.
    /// </para>
    /// </summary>
    private async Task PrepareAsync(string treeId, List<KeyValuePair<string, byte[]>> entries)
    {
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
        // persisted fields are reused verbatim, mirroring the
        // KeyFingerprint / TransactionId capture-once pattern.
        if (state.State.DeltaKind is null && state.State.DeltaPayload is null)
        {
            var deltaCarry = LatticeDeltaContext.Current;
            if (deltaCarry is { } carry)
            {
                state.State.DeltaKind = carry.Kind;
                state.State.DeltaPayload = carry.Payload;
            }
        }

        // Capture caller's ambient vector-clock frontier once, on the
        // first Prepare. On a reminder-driven replay (no caller
        // context) the persisted value is reused verbatim, mirroring
        // the KeyFingerprint / TransactionId / DeltaKind capture-once
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

        var lattice = grainFactory.GetGrain<ILattice>(treeId);
        var routing = await lattice.GetRoutingAsync();
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

        foreach (var entry in entries)
        {
            LwwEntry? raw;
            try
            {
                raw = await GetShardForKey(routing, entry.Key).GetRawEntryAsync(entry.Key);
            }
            catch (StaleShardRoutingException)
            {
                // Adaptive shard split has remapped virtual slots; refresh
                // routing and retry this key once.
                routing = await lattice.GetRoutingAsync();
                raw = await GetShardForKey(routing, entry.Key).GetRawEntryAsync(entry.Key);
            }

            // LwwEntry fields are flat (Value/Timestamp/IsTombstone/ExpiresAtTicks).
            // ToLwwValue() rehydrates the underlying LwwValue for IsExpired.
            var existed = raw is not null
                && !raw.Value.IsTombstone
                && !raw.Value.ToLwwValue().IsExpired(nowTicks);
            state.State.PreValues.Add(new AtomicPreValue
            {
                Key = entry.Key,
                Value = existed ? raw!.Value.Value : null,
                Existed = existed,
                ExpiresAtTicks = existed ? raw!.Value.ExpiresAtTicks : 0,
                OriginClusterId = existed ? raw!.Value.OriginClusterId : null,
                VectorClock = existed ? raw!.Value.VectorClock : null,
            });
        }

        state.State.Phase = AtomicWritePhase.Execute;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Resolves the <see cref="IShardRootGrain"/> that owns <paramref name="key"/>
    /// for the supplied <paramref name="routing"/> snapshot. Mirrors
    /// <c>LatticeGrain.GetShardGrainAsync</c> but inlined because the saga
    /// holds the <see cref="RoutingInfo"/> externally rather than caching it
    /// on a per-activation basis.
    /// </summary>
    private IShardRootGrain GetShardForKey(RoutingInfo routing, string key)
    {
        var shardIndex = routing.Map.Resolve(key);
        return grainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}");
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
            // skip the broadcast. The saga still completes — the
            // worst case is that prepared writes (if any) remain
            // bucketed in the leaves' pending-tx maps until they
            // age out of replay or the operator manually drops them.
            Logger.LogWarning(
                "Atomic-write saga {OperationKey}: skipping terminal broadcast — no transaction id is set on persisted state.",
                OperationKey);
            return;
        }

        // Reconstruct the touched-shard set on legacy state where the
        // slot was never populated. Persist the reconstruction so a
        // subsequent crash-resume can skip the rebuild.
        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
        if (state.State.TouchedShards.Count == 0 && state.State.Entries.Count > 0)
        {
            var routing = await lattice.GetRoutingAsync();
            var touched = new HashSet<int>();
            foreach (var entry in state.State.Entries)
            {
                touched.Add(routing.Map.Resolve(entry.Key));
            }
            var sorted = new List<int>(touched);
            sorted.Sort();
            state.State.TouchedShards = sorted;
            await state.WriteStateAsync();
        }

        if (state.State.TouchedShards.Count == 0)
        {
            // Empty saga (zero entries): no shard was touched, so no
            // terminal mark to broadcast.
            return;
        }

        // Resolve the physical-tree id once for the broadcast pass.
        // The retry path inside MarkOneShardAsync re-resolves on a
        // stale-routing throw.
        var physicalTreeId = (await lattice.GetRoutingAsync()).PhysicalTreeId;

        var pending = new List<Task>(state.State.TouchedShards.Count);
        foreach (var shardIndex in state.State.TouchedShards)
        {
            pending.Add(MarkOneShardAsync(physicalTreeId, shardIndex, transactionId, committed));
        }

        await Task.WhenAll(pending);
    }

    /// <summary>
    /// Per-shard terminal append with one-shot routing-refresh retry.
    /// Encapsulates the stale-routing recovery so the
    /// <see cref="BroadcastTerminalsAsync(bool)"/> fan-out body stays
    /// linear.
    /// </summary>
    private async Task MarkOneShardAsync(string physicalTreeId, int shardIndex, Guid transactionId, bool committed)
    {
        var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
        try
        {
            await shard.AppendTxTerminalAsync(transactionId, committed);
            return;
        }
        catch (StaleShardRoutingException)
        {
            // Slot ownership moved between prepare and broadcast
            // (adaptive shard split). Refresh routing under the same
            // logical tree id and retry once against the new owner.
        }
        catch (StaleTreeRoutingException)
        {
            // Tree alias swapped mid-saga (online resize). Refresh
            // routing under the same logical tree id and retry once
            // against the new physical tree.
        }

        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);
        var refreshed = await lattice.GetRoutingAsync();
        var refreshedShard = grainFactory.GetGrain<IShardRootGrain>($"{refreshed.PhysicalTreeId}/{shardIndex}");
        await refreshedShard.AppendTxTerminalAsync(transactionId, committed);
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

        if (state.State.Phase == AtomicWritePhase.Prepare)
        {
            // Crash before execute was persisted — replay Prepare.
            var entries = state.State.Entries;
            await PrepareAsync(state.State.TreeId, entries);
        }

        if (state.State.Phase == AtomicWritePhase.Execute)
        {
            await ExecutePhaseAsync();
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
            // pending bucket on the same leaf). The abort terminal
            // mark is the per-shard linearization point on the
            // rollback side: every leaf on every touched shard
            // observes either the pre-saga state for every key or -
            // briefly, between Apply'ing the prepares and Apply'ing
            // the terminal - the post-prepare bucketed state, which
            // is invisible to readers via the prepared-mutation
            // read-path filter.
            await BroadcastTerminalsAsync(committed: false);
            await CompleteSagaAsync(success: false);
            throw new InvalidOperationException(
                $"Atomic write saga for tree '{state.State.TreeId}' failed and was rolled back: " +
                (state.State.FailureMessage ?? "unknown failure"));
        }

        if (state.State.Phase == AtomicWritePhase.Execute && state.State.NextIndex >= state.State.Entries.Count)
        {
            // Every prepare-phase write succeeded — broadcast the
            // commit terminal to every touched shard before flipping
            // the saga to Completed. A crash between the broadcast
            // start and the Completed flip leaves the saga in
            // Execute (NextIndex == Entries.Count); reminder-driven
            // re-entry observes the post-loop condition, re-runs the
            // broadcast (idempotent via the leaf-side recently-
            // terminal dedup), and proceeds to CompleteSagaAsync.
            await BroadcastTerminalsAsync(committed: true);
            await CompleteSagaAsync(success: true);
        }
    }

    /// <summary>
    /// Applies each entry in order. A failure transitions the saga into
    /// <see cref="AtomicWritePhase.Compensate"/> without re-throwing — the
    /// caller is driven by <see cref="RunSagaAsync"/> which continues into
    /// compensation on the same call.
    /// </summary>
    private async Task ExecutePhaseAsync()
    {
        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);

        while (state.State.NextIndex < state.State.Entries.Count)
        {
            try
            {
                // Stamp the per-key (Size, Index) ambient so the
                // mutation publish helpers stamp identical
                // LatticeMutation.AtomicBatchSize across the batch
                // and a strictly-increasing AtomicBatchIndex per
                // emit. The saga-wide stamp from
                // StampAtomicBatchContext seeded Index=0; this
                // per-key scope overrides that with the actual
                // per-step index and is restored on disposal.
                using (LatticeAtomicBatchContext.With((state.State.AtomicBatchSize, state.State.NextIndex)))
                {
                    // Stamp the prepare-phase ambient so the leaf
                    // grain's commit pipeline routes the resulting
                    // mutation into the per-leaf in-memory pending-tx
                    // map (IsPrepared=true on the LatticeMutation
                    // wire) rather than into the visible projection.
                    // The terminal-mark broadcast that runs after
                    // every prepare succeeds (or after any prepare
                    // fails) is the per-shard linearization point
                    // that flips pending entries into Entries (commit)
                    // or drops them (abort).
                    using (LatticePreparedContext.BeginScope())
                    {
                        if (state.State.IsApplyMode)
                        {
                            await ExecuteApplyStepAsync(lattice, state.State.NextIndex);
                        }
                        else
                        {
                            var entry = state.State.Entries[state.State.NextIndex];
                            await lattice.SetAsync(entry.Key, entry.Value);
                        }
                    }
                }

                state.State.NextIndex++;
                state.State.RetriesOnCurrentStep = 0;
                await state.WriteStateAsync();
            }
            catch (Exception ex)
            {
                if (state.State.RetriesOnCurrentStep < MaxRetriesPerStep)
                {
                    state.State.RetriesOnCurrentStep++;
                    await state.WriteStateAsync();
                    Logger.LogWarning(ex,
                        "Atomic-write saga {OperationKey}: retrying step {Index} (attempt {Attempt}).",
                        OperationKey, state.State.NextIndex, state.State.RetriesOnCurrentStep);
                    continue;
                }

                // Exhausted retries — pivot to compensation.
                state.State.Phase = AtomicWritePhase.Compensate;
                state.State.FailureMessage = ex.Message;
                // NextIndex currently points at the failed-to-commit entry; it
                // was NOT written, so compensation rolls back entries [0..NextIndex-1].
                state.State.RetriesOnCurrentStep = 0;
                await state.WriteStateAsync();
                return;
            }
        }

        // Every entry committed — switch to Completed marker on saga exit.
    }

    /// <summary>
    /// Apply-mode per-key dispatch. Wraps the per-key call in nested
    /// <see cref="LatticeOriginContext"/> + <see cref="LatticeVectorClockContext"/>
    /// + <see cref="LatticeHlcOverrideContext"/> scopes drawn from
    /// <see cref="AtomicWriteState.OriginClusterId"/> and the entry's
    /// <see cref="AtomicApplyEntry.VectorClock"/> /
    /// <see cref="AtomicApplyEntry.Timestamp"/> so the leaf grain
    /// re-stamps the source-side metadata bit-identically. TTL'd
    /// entries route through the
    /// <see cref="ILattice.SetAsync(string, byte[], TimeSpan, CancellationToken)"/>
    /// overload with the remaining time-to-live computed against the
    /// current wall clock; if the absolute expiry has already elapsed
    /// the entry is treated as absent and tombstoned instead, matching
    /// the public-read semantics for expired entries.
    /// </summary>
    private async Task ExecuteApplyStepAsync(ILattice lattice, int index)
    {
        var apply = state.State.ApplyEntries[index];

        using (LatticeOriginContext.With(state.State.OriginClusterId))
        using (LatticeVectorClockContext.With(apply.VectorClock))
        using (LatticeHlcOverrideContext.With(apply.Timestamp))
        {
            if (apply.IsTombstone)
            {
                await lattice.DeleteAsync(apply.Key);
            }
            else if (apply.ExpiresAtTicks > 0)
            {
                var remainingTicks = apply.ExpiresAtTicks - DateTimeOffset.UtcNow.UtcTicks;
                if (remainingTicks <= 0)
                {
                    // Absolute expiry already elapsed — treat as absent
                    // (public reads would already hide the entry).
                    await lattice.DeleteAsync(apply.Key);
                }
                else
                {
                    await lattice.SetAsync(apply.Key, apply.Value!, TimeSpan.FromTicks(remainingTicks));
                }
            }
            else
            {
                await lattice.SetAsync(apply.Key, apply.Value!);
            }
        }
    }

    /// <summary>
    /// Marks the saga Completed, unregisters the keepalive reminder, arms the
    /// retention reminder (via the shared TtlGrain base) for delayed state
    /// cleanup, and requests deactivation. Safe to call in both success and
    /// post-compensation paths. <paramref name="success"/> gates emission of
    /// the terminal <see cref="LatticeTreeEventKind.AtomicWriteCompleted"/>
    /// event — rolled-back sagas do not publish a completion event because
    /// no write actually committed.
    /// </summary>
    private async Task CompleteSagaAsync(bool success)
    {
        state.State.Phase = AtomicWritePhase.Completed;
        state.State.RetriesOnCurrentStep = 0;
        await state.WriteStateAsync();
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

        // Publish AtomicWriteCompleted only when the saga committed all writes.
        // Rolled-back sagas emitted per-key Set events during ExecutePhase but
        // compensated them back via LWW tombstones/restores; there is no net
        // write to notify subscribers about.
        if (success)
        {
            await PublishCompletedEventAsync();
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
    /// <c>DeleteAsync</c> call the saga makes — including compensation
    /// rewrites — surfaces with the same
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
    /// <c>DeleteAsync</c> the saga issues — including compensation
    /// rewrites — surfaces with the same
    /// <see cref="LatticeMutation.DeltaKind"/> /
    /// <see cref="LatticeMutation.DeltaPayload"/> as the original batch.
    /// No-op when the caller did not supply a delta context on the first
    /// <see cref="ExecuteAsync"/> call.
    /// </summary>
    private void StampDeltaContext()
    {
        if (state.State.DeltaKind is null || state.State.DeltaPayload is null) return;
        LatticeDeltaContext.Current = (state.State.DeltaKind, state.State.DeltaPayload);
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

    private Task RegisterKeepaliveAsync() =>
        ReminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: GrainContext.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await ReminderRegistry.GetReminder(GrainContext.GrainId, KeepaliveReminderName);
            if (reminder is not null)
                await ReminderRegistry.UnregisterReminder(GrainContext.GrainId, reminder);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Atomic-write saga {OperationKey}: failed to unregister keepalive reminder.",
                OperationKey);
        }
    }
}
