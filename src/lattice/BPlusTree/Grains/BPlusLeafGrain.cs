using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf node grain implementation. Stores key → <see cref="LwwValue{T}"/> entries
/// in a sorted dictionary. Splits when the entry count exceeds the leaf-sizing
/// pin in the tree registry.
/// </summary>
// CS9113: 'originClusterIdResolver' is referenced only inside #if LATTICE_DIAG
// blocks (used by DiagSiloTag to disambiguate Site A vs Site B emissions in the
// shared file-based DiagSink log). Suppressed at the parameter list because in
// non-diag builds the parameter is genuinely unread, but removing it would break
// the activation-DI signature and the diag build's site-tagging behaviour.
#pragma warning disable CS9113
internal sealed partial class BPlusLeafGrain(
    IGrainContext context,
    [PersistentState("leaf", LatticeOptions.StorageProviderName)] IPersistentState<LeafNodeState> state,
    IGrainFactory grainFactory,
    LatticeOptionsResolver optionsResolver,
    MutationObserverDispatcher mutationObservers,
    ILatticeOriginClusterIdResolver originClusterIdResolver) : IBPlusLeafGrain, ILeafProjection, IGrainBase
#pragma warning restore CS9113
{
    IGrainContext IGrainBase.GrainContext => context;

#if LATTICE_DIAG
    /// <summary>
    /// Cached cluster id of the silo hosting this leaf activation, used to
    /// disambiguate Site A vs Site B emissions in the shared file-based
    /// <see cref="DiagSink"/> log. Resolved lazily because <c>state.State.TreeId</c>
    /// is null until activation completes; the resolver is keyed by tree id
    /// only because the replication package's per-tree options map may carry
    /// distinct cluster ids per tree (the common case is a single host-wide id).
    /// </summary>
    private string? _diagSiloTag;

    private string DiagSiloTag => _diagSiloTag
        ??= (originClusterIdResolver.Resolve(state.State.TreeId ?? string.Empty) is { Length: > 0 } id ? id : "(local)");
#endif

    /// <summary>
    /// Synchronously flushes any pending projection-checkpoint advance
    /// to durable storage on graceful deactivation so a clean shutdown
    /// does not lose an unflushed checkpoint that the materialiser has
    /// already issued. Crash deactivations bypass this hook by design -
    /// the persisted offset bounds replay cost in that case.
    /// </summary>
    async Task IGrainBase.OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        try
        {
            await ((ILeafProjection)this).FlushCheckpointAsync(cancellationToken);
        }
        catch
        {
            // A storage failure on shutdown must not block deactivation;
            // the persisted offset still bounds replay cost on the next
            // activation.
        }
        finally
        {
            DisposeProjectionHasher();

            // Remove this activation's same-silo revision cookie so a
            // future re-activation starts fresh and any same-silo
            // LeafCacheGrain that may still hold _lastSeenPrimaryRevision
            // from this activation falls through to the cross-grain
            // refresh path on its next read. Cookies are best-effort,
            // not correctness-critical, but pruning keeps the registry
            // bounded by the live-leaf set rather than the lifetime-leaf
            // set.
            LeafRevisionRegistry.TryRemove(context.GrainId, out _);
        }
    }

    private static readonly Dictionary<string, LwwValue<byte[]>> EmptyEntries = new();

    /// <summary>
    /// Process-wide singleton returned by <see cref="GetDeltaSinceAsync"/>
    /// on the cache-up-to-date fast path (caller's version dominates the
    /// leaf's, no pending split). Sharing the singleton elides three
    /// per-read allocations on the steady-state read path through
    /// <see cref="LeafCacheGrain.RefreshAsync"/>:
    /// <list type="bullet">
    ///   <item>The <see cref="StateDelta"/> record itself (~24 B).</item>
    ///   <item>The <see cref="VersionVector"/> wrapper (~24 B).</item>
    ///   <item>The wrapper's <c>Dictionary&lt;string, HybridLogicalClock&gt;</c>
    ///   (~80 B once the leaf has any writes).</item>
    /// </list>
    /// Safe to share because the only production callers of
    /// <see cref="GetDeltaSinceAsync"/> consume <c>delta.Version</c>
    /// exclusively through the pure static
    /// <see cref="VersionVector.Merge(VersionVector, VersionVector)"/>
    /// (see <see cref="LeafCacheGrain.RefreshAsync"/>) - no caller mutates
    /// the returned vector. The singleton's empty version is also
    /// correctness-equivalent on this branch: the dominate-or-equals
    /// precondition guarantees the caller already saw everything the leaf
    /// has, so merging an empty vector into the caller's vector is a no-op
    /// in observable state. The pre-allocated <see cref="EmptyDeltaTask"/>
    /// elides the <c>Task.FromResult</c> wrapper as well, leaving the
    /// fast-path return as a single static-field load.
    /// </summary>
    private static readonly StateDelta EmptyDelta = new()
    {
        Entries = EmptyEntries,
        Version = new VersionVector(),
        SplitKey = null,
        MovedAwaySlots = null,
        MovedAwayVsc = null,
    };

    private static readonly Task<StateDelta> EmptyDeltaTask = Task.FromResult(EmptyDelta);

    /// <summary>
    /// Cached <see cref="IGrainContext.GrainId"/> rendered as a <see cref="string"/>.
    /// The grain id is immutable for the lifetime of an activation, so this field
    /// is populated lazily on first <see cref="ReplicaId"/> access and reused for
    /// every subsequent read. Eliminates the per-call <see cref="object.ToString"/>
    /// allocation that the previous getter shape paid on every CRUD operation
    /// (8 hot-path call sites: 6 <see cref="VersionVector.Tick(string)"/> calls
    /// across <c>CommitSetAsync</c> / <c>CommitDeleteAsync</c> / <c>MergeAsync</c>
    /// / saga commit + 2 caller-clock reads inside <c>GetDeltaSinceAsync</c>).
    /// </summary>
    private string? _replicaId;

    private string ReplicaId => _replicaId ??= context.GrainId.ToString();
    private ResolvedLatticeOptions? _options;
    private ValueTask<ResolvedLatticeOptions> GetOptionsAsync() =>
        _options is not null
            ? new ValueTask<ResolvedLatticeOptions>(_options)
            : ResolveOptionsSlowAsync();

    private async ValueTask<ResolvedLatticeOptions> ResolveOptionsSlowAsync() =>
        _options = await optionsResolver.ResolveAsync(state.State.TreeId ?? string.Empty);

    /// <summary>
    /// Advances the leaf's local <see cref="HybridLogicalClock"/> for a
    /// commit and returns the value to persist on the freshly-constructed
    /// <see cref="LwwValue{T}"/>. When
    /// <see cref="LatticeHlcOverrideContext.Current"/> is <see langword="null"/>
    /// (the foreground-caller default), the local clock advances via
    /// <see cref="HybridLogicalClock.Tick"/> and the same value is
    /// returned. When an override is present (the cross-cluster atomic
    /// apply path), the local clock advances via
    /// <see cref="HybridLogicalClock.Merge"/> so subsequent foreground
    /// ticks remain strictly greater than the override (preserving local
    /// monotonicity), but the <em>override</em> is returned verbatim so
    /// the persisted <see cref="LwwValue{T}.Timestamp"/> matches the
    /// authoring cluster's HLC bit-identically - preserving the
    /// receiver-side LWW resolution invariant.
    /// </summary>
    private HybridLogicalClock AdvanceClockOrOverride()
    {
        var ovr = LatticeHlcOverrideContext.Current;
        if (ovr is { } sourceHlc)
        {
            state.State.Clock = HybridLogicalClock.Merge(state.State.Clock, sourceHlc);
            return sourceHlc;
        }

        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        return state.State.Clock;
    }

    /// <inheritdoc />
    public Task<HybridLogicalClock> GetClockAsync() => Task.FromResult(state.State.Clock);

    public Task<byte[]?> GetAsync(string key)
    {
        // Moved-away seal: a slot recorded on this leaf as having
        // migrated to a sibling shard is invisible to every read
        // path, including the LeafCacheGrain pending-key delegation
        // that bypasses the shard front door. See IsKeyMovedAway for the rationale.
        if (IsKeyMovedAway(key))
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG read1-moved-away] gid={context.GrainId} key={key}");
#endif
            return Task.FromResult<byte[]?>(null);
        }

        // Strict atomic-visibility: a key with a pending-tx entry
        // dials back through the per-tree TxRegistry - the
        // registry-recorded saga outcome is the single tree-wide
        // linearization point, so readers never observe a partial
        // commit / abort across leaves. The fast path (no pending
        // entry on this leaf) avoids the RPC entirely.
        if (TryFindPendingForKey(key, out var txid, out var pendingValue))
        {
            return GetWithPendingAsync(key, txid, pendingValue);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
        {
            // Migration-window shadow guard. A migrated entry on this
            // destination leaf carries the source's pre-saga snapshot
            // (IsMigrated=true). If the split coordinator installed a
            // shadow marker naming a saga that committed at the
            // registry but whose backstop terminal has not yet reached
            // this leaf, serving the migrated value here would split
            // observation against any sibling leaf whose backstop has
            // already landed. The slow path consults the registry and
            // raises StaleShardRoutingException for the Committed-no-
            // backstop case so the LatticeGrain deadline-bounded retry
            // loop re-fans under a fresh snapshot.
            if (lww.IsMigrated && TryGetShadowedSagas(key, out var sagas))
            {
                return GetWithShadowedMigratedAsync(key, lww.Value, sagas);
            }
#if LATTICE_DIAG
            // DIAG: single-key read-return path.
            DiagSink.Write($"[DIAG read1] gid={context.GrainId} key={key} valRound={DiagDecodeRound(lww.Value)} " +
                $"hlc={lww.Timestamp} isMig={lww.IsMigrated} origin={lww.OriginClusterId ?? "(local)"}");
#endif
            return Task.FromResult<byte[]?>(lww.Value);
        }

#if LATTICE_DIAG
        // DIAG: single-key returning null.
        DiagSink.Write($"[DIAG read1-null] gid={context.GrainId} key={key}");
#endif
        return Task.FromResult<byte[]?>(null);
    }

    /// <summary>
    /// Slow-path completion of <see cref="GetAsync"/> when the key
    /// would surface a migrated entry but carries a destination-side
    /// shadow marker. Resolves every shadowing saga through the
    /// registry and either passes the migrated value through
    /// (InFlight / Aborted / Committed-with-backstop) or raises
    /// <see cref="StaleShardRoutingException"/> with a sentinel
    /// <c>(-1, -1, -1)</c> tuple so the caller's deadline-bounded
    /// retry loop re-fans under a fresh snapshot.
    /// </summary>
    private async Task<byte[]?> GetWithShadowedMigratedAsync(string key, byte[]? migratedValue, HashSet<Guid> sagas)
    {
        if (await IsShadowedReadSafeAsync(sagas))
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG read1-shadow-pass] gid={context.GrainId} key={key} valRound={DiagDecodeRound(migratedValue)}");
#endif
            return migratedValue;
        }
#if LATTICE_DIAG
        DiagSink.Write($"[DIAG read1-shadow-stale] gid={context.GrainId} key={key} sagas=[{string.Join(',', sagas)}]");
#endif
        throw new StaleShardRoutingException(-1, -1, -1);
    }

    private async Task<byte[]?> GetWithPendingAsync(string key, Guid txid, LwwValue<byte[]> pendingValue)
    {
        var status = await ResolvePendingStatusAsync(txid);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        switch (status)
        {
            case TxStatus.Committed:
                // Orphan-pending guard. A pending bucket that survives
                // AFTER this leaf has already processed the saga's
                // terminal is an orphan from a late-arriving shadow-
                // forward: under an online reshard, the saga's terminal
                // broadcast can reach this destination leaf via the
                // cross-migration LWW backstop (no bucket existed, so
                // ApplyTxTerminalAsync wrote the saga's value directly
                // into Entries and set _recentlyTerminal) BEFORE the
                // source shard's shadow-forward of the prepare lands
                // here. The shadow-forward then bucketed the prepare,
                // and TryFindPendingForKey's HLC tie-break can pick
                // that orphan over a sibling bucket from a later saga
                // whose prepare HLC was stamped against the
                // destination's own clock (typically lower than the
                // orphan's source-stamped HLC). Returning the orphan's
                // value would then shadow Entries[key], which may hold
                // a later saga's already-drained value, producing the
                // "split (pre=1, post=15)" / "unknown-round (other=1)"
                // chaos shapes the reshard-topology suite caught.
                //
                // _recentlyTerminal is the per-leaf "this saga's
                // terminal has already been applied here" flag set by
                // every ApplyTxTerminalAsync exit path (drain commit,
                // drain abort, fast-path-no-bucket commit, backstop-
                // only commit). When it is set for the bucket's txid,
                // the bucket cannot be the saga's primary delivery, so
                // we surface the durably-projected value from Entries
                // (which the saga's own backstop or drain wrote, or
                // which a strictly-later saga has since overwritten).
                if (_recentlyTerminal is not null && _recentlyTerminal.Contains(txid))
                {
                    if (state.State.Entries.TryGetValue(key, out var entriesLww)
                        && !entriesLww.IsTombstone
                        && !entriesLww.IsExpired(nowTicks))
                        return entriesLww.Value;
                    return null;
                }
                if (pendingValue.IsTombstone || pendingValue.IsExpired(nowTicks))
                    return null;
                return pendingValue.Value;
            default:
                // InFlight or Aborted - surface the pre-saga value
                // from Entries. Strict atomic visibility: until the
                // registry records a Committed decision, the saga's
                // prepared writes are invisible and readers must see
                // exactly the state that existed before the saga
                // started. Hiding the key on InFlight would create a
                // split observation across leaves whose prepares
                // arrive at different wall-clock moments.
                if (state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
                    return lww.Value;
                return null;
        }
    }

    public Task<VersionedValue> GetWithVersionAsync(string key)
    {
        // Moved-away seal. See GetAsync for the rationale.
        if (IsKeyMovedAway(key))
        {
            return Task.FromResult(new VersionedValue());
        }

        if (TryFindPendingForKey(key, out var txid, out var pendingValue))
        {
            return GetWithVersionWithPendingAsync(key, txid, pendingValue);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
        {
            return Task.FromResult(new VersionedValue { Value = lww.Value, Version = lww.Timestamp });
        }

        return Task.FromResult(new VersionedValue());
    }

    private async Task<VersionedValue> GetWithVersionWithPendingAsync(string key, Guid txid, LwwValue<byte[]> pendingValue)
    {
        var status = await ResolvePendingStatusAsync(txid);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        switch (status)
        {
            case TxStatus.Committed:
                // Orphan-pending guard. See GetWithPendingAsync for the
                // full rationale: a Committed bucket whose txid is in
                // _recentlyTerminal is a late-arriving shadow-forward
                // orphan and must not shadow Entries[key]'s authoritative
                // value.
                if (_recentlyTerminal is not null && _recentlyTerminal.Contains(txid))
                {
                    if (state.State.Entries.TryGetValue(key, out var entriesLww)
                        && !entriesLww.IsTombstone
                        && !entriesLww.IsExpired(nowTicks))
                        return new VersionedValue { Value = entriesLww.Value, Version = entriesLww.Timestamp };
                    return new VersionedValue();
                }
                if (pendingValue.IsTombstone || pendingValue.IsExpired(nowTicks))
                    return new VersionedValue();
                return new VersionedValue { Value = pendingValue.Value, Version = pendingValue.Timestamp };
            default:
                // InFlight or Aborted - surface the pre-saga value.
                // See GetWithPendingAsync for the rationale.
                if (state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
                    return new VersionedValue { Value = lww.Value, Version = lww.Timestamp };
                return new VersionedValue();
        }
    }

    public Task<bool> ExistsAsync(string key)
    {
        // Moved-away seal. See GetAsync for the rationale.
        if (IsKeyMovedAway(key))
        {
            return Task.FromResult(false);
        }

        if (TryFindPendingForKey(key, out var txid, out var pendingValue))
        {
            return ExistsWithPendingAsync(key, txid, pendingValue);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        return Task.FromResult(
            state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks));
    }

    private async Task<bool> ExistsWithPendingAsync(string key, Guid txid, LwwValue<byte[]> pendingValue)
    {
        var status = await ResolvePendingStatusAsync(txid);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        switch (status)
        {
            case TxStatus.Committed:
                // Orphan-pending guard. See GetWithPendingAsync for the
                // full rationale.
                if (_recentlyTerminal is not null && _recentlyTerminal.Contains(txid))
                {
                    return state.State.Entries.TryGetValue(key, out var entriesLww)
                        && !entriesLww.IsTombstone
                        && !entriesLww.IsExpired(nowTicks);
                }
                return !pendingValue.IsTombstone && !pendingValue.IsExpired(nowTicks);
            default:
                // InFlight or Aborted - fall through to Entries.
                // See GetWithPendingAsync for the rationale.
                return state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks);
        }
    }

    public Task<GetOrSetResult> GetOrSetAsync(string key, byte[] value)
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Short-circuit: if the key already exists and is live (and not expired)
        // AND has no pending-tx mutation, return its value without writing.
        // A pending mutation makes the key invisible, so we must fall through
        // to the write path to record the caller's intent.
        if (!IsKeyPending(key)
            && state.State.Entries.TryGetValue(key, out var existing)
            && !existing.IsTombstone
            && !existing.IsExpired(nowTicks))
        {
            return Task.FromResult(new GetOrSetResult { ExistingValue = existing.Value });
        }

        // Key is absent, tombstoned, expired, or pending - delegate to the write path and wrap the result.
        return GetOrSetWriteAsync(key, value);
    }

    private async Task<GetOrSetResult> GetOrSetWriteAsync(string key, byte[] value)
    {
        var splitResult = await SetAsync(key, value);
        return new GetOrSetResult { Split = splitResult };
    }

    public Task<CasResult> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Pending-tx isolation: a key with an in-flight saga prepare is
        // invisible to CAS - treat it as absent so expectedVersion must
        // be Zero. The CAS write itself races with the saga's terminal
        // mark; LWW resolves the conflict deterministically via HLC.
        var pending = IsKeyPending(key);

        // Check current entry version. Treat expired live entries as absent
        // for CAS purposes (same as tombstones) so a fresh write with
        // expectedVersion == Zero succeeds after expiry.
        if (!pending
            && state.State.Entries.TryGetValue(key, out var existing)
            && !existing.IsTombstone
            && !existing.IsExpired(nowTicks))
        {
            if (existing.Timestamp != expectedVersion)
            {
                return Task.FromResult(new CasResult
                {
                    Success = false,
                    CurrentVersion = existing.Timestamp
                });
            }
        }
        else
        {
            // Key is absent, tombstoned, or pending - expectedVersion must be Zero.
            if (expectedVersion != HybridLogicalClock.Zero)
            {
                return Task.FromResult(new CasResult
                {
                    Success = false,
                    CurrentVersion = HybridLogicalClock.Zero
                });
            }
        }

        // Version matches - delegate to the async write path.
        return SetIfVersionWriteAsync(key, value);
    }

    private async Task<CasResult> SetIfVersionWriteAsync(string key, byte[] value)
    {
        var splitResult = await SetAsync(key, value);
        // After SetAsync, the entry has a new timestamp.
        var newVersion = state.State.Entries[key].Timestamp;
        return new CasResult
        {
            Success = true,
            CurrentVersion = newVersion,
            Split = splitResult
        };
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        var result = new Dictionary<string, byte[]>(keys.Count);
        foreach (var key in keys)
        {
            // Moved-away seal. See GetAsync for the rationale.
            // Hot path: leaves with no moved slots short-circuit on a
            // single nullable read inside IsKeyMovedAway.
            if (IsKeyMovedAway(key))
            {
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG read-moved-away] gid={context.GrainId} key={key}");
#endif
                continue;
            }

            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                if (status == TxStatus.Committed
                    && !(_recentlyTerminal is not null && _recentlyTerminal.Contains(pending.txid)))
                {
                    if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks))
                    {
                        result[key] = pending.value.Value!;
#if LATTICE_DIAG
                        // DIAG: pending-bucket-committed read path.
                        DiagSink.Write($"[DIAG read-pending-committed] silo={DiagSiloTag} gid={context.GrainId} key={key} tx={pending.txid} valRound={DiagDecodeRound(pending.value.Value)} hlc={pending.value.Timestamp}");
#endif
                    }
                    else
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG read-pending-committed-tomb] silo={DiagSiloTag} gid={context.GrainId} key={key} tx={pending.txid}");
#endif
                    }
                    continue;
                }
#if LATTICE_DIAG
                // DIAG: pending-bucket-fallthrough (InFlight, Aborted, or already-terminal'd).
                DiagSink.Write($"[DIAG read-pending-fallthrough] silo={DiagSiloTag} gid={context.GrainId} key={key} tx={pending.txid} status={status} alreadyTerminal={(_recentlyTerminal is not null && _recentlyTerminal.Contains(pending.txid))}");
#endif
                // InFlight, Aborted, or orphan-pending (committed bucket whose
                // saga terminal has already landed on this leaf) - fall through
                // to Entries. See GetWithPendingAsync for the orphan-pending
                // rationale: a late-arriving shadow-forward of a prepare can
                // bucket a saga whose terminal has already drained into Entries,
                // and surfacing the orphan would shadow the authoritative
                // Entries value (or a strictly-later saga's value).
            }

            if (state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
            {
                // Migration-window shadow guard. See GetAsync for the
                // full rationale: when the surfacing entry is a
                // destination-side migration (IsMigrated=true) and
                // the split coordinator installed a shadow marker
                // naming a committed-no-backstop saga as the owner
                // of this key, raise StaleShardRoutingException so
                // the LatticeGrain retry loop re-fans under a fresh
                // snapshot. Cheap on the steady-state path: a single
                // null check plus a dictionary miss when no marker
                // is installed.
                if (lww.IsMigrated && TryGetShadowedSagas(key, out var shadowSagas))
                {
                    if (!await IsShadowedReadSafeAsync(shadowSagas))
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG read-shadow-stale] silo={DiagSiloTag} gid={context.GrainId} key={key} sagas=[{string.Join(',', shadowSagas)}]");
#endif
                        throw new StaleShardRoutingException(-1, -1, -1);
                    }
                }
                result[key] = lww.Value!;
#if LATTICE_DIAG
                // DIAG: read-return path - capture what each leaf returns per key.
                DiagSink.Write($"[DIAG read] silo={DiagSiloTag} gid={context.GrainId} key={key} valRound={DiagDecodeRound(lww.Value)} " +
                    $"hlc={lww.Timestamp} isMig={lww.IsMigrated} origin={lww.OriginClusterId ?? "(local)"}");
#endif
            }
        }
        return result;
    }

    public Task<SplitResult?> SetAsync(string key, byte[] value) =>
        SetCoreAsync(key, value, 0L);

    /// <inheritdoc />
    public Task<SplitResult?> SetAsync(string key, byte[] value, long expiresAtTicks) =>
        SetCoreAsync(key, value, expiresAtTicks);

    public Task<LwwEntry?> GetRawEntryAsync(string key)
    {
        if (state.State.Entries.TryGetValue(key, out var lww))
            return Task.FromResult<LwwEntry?>(new LwwEntry(key, lww));
        return Task.FromResult<LwwEntry?>(null);
    }

    /// <inheritdoc />
    public Task<List<LwwEntry?>> GetRawEntriesAsync(List<string> keys)
    {
        // Pure in-memory dictionary lookup loop; no I/O, no allocation
        // beyond the result list itself. The Orleans grain-call boundary
        // wraps this in a single async state machine even though the
        // method body is synchronous, so the cost per batch is one
        // Task allocation regardless of key count - which is exactly
        // the win the saga's PrepareAsync capture loop targets.
        var entries = state.State.Entries;
        var result = new List<LwwEntry?>(keys.Count);
        foreach (var key in keys)
        {
            if (entries.TryGetValue(key, out var lww))
                result.Add(new LwwEntry(key, lww));
            else
                result.Add(null);
        }
        return Task.FromResult(result);
    }

    private async Task<SplitResult?> SetCoreAsync(string key, byte[] value, long expiresAtTicks)
    {
        // Recovery: if a previous split was interrupted, complete it first.
        if (state.State.SplitState == Primitives.SplitState.SplitInProgress)
        {
            var recovered = await CompleteSplitAsync();
            await PersistAsync();

            // Apply the caller's write to the correct leaf so it isn't silently dropped.
            if (string.Compare(key, state.State.SplitKey!, StringComparison.Ordinal) >= 0)
            {
                // The key belongs to the new sibling - forward it there.
                // The sibling publishes its own mutation notification after persist,
                // so we do not publish one here to avoid a duplicate for the same key.
                var sibling = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.SplitSiblingId!.Value);
                await sibling.SetAsync(key, value, expiresAtTicks);
            }
            else
            {
                // The key belongs to this leaf - write it via the
                // dual-durability commit path so the WAL append, the
                // in-memory projection update, and the shadow persist
                // remain consistent with the main path below.
                await CommitSetAsync(key, value, expiresAtTicks);
            }

            return recovered;
        }

        return await CommitSetAsync(key, value, expiresAtTicks);
    }

    /// <summary>
    /// Commit path for <see cref="MutationKind.Set"/>.
    /// Steps in order:
    /// <list type="number">
    ///   <item><b>build</b> - tick HLC + version vector and construct
    ///   the LWW value plus its observer-bound mutation envelope;</item>
    ///   <item><b>wal</b> - append the mutation to the per-shard WAL via
    ///   the resolved <see cref="ICommitLogWriter"/> (no-op when the
    ///   adapter is absent);</item>
    ///   <item><b>apply</b> - merge the LWW value into the in-memory
    ///   projection and check the leaf-split predicate;</item>
    ///   <item><b>observer</b> - publish the post-commit mutation to
    ///   any registered <see cref="IMutationObserver"/> inside a
    ///   <see cref="LatticeCommitLogContext"/> scope so a downstream
    ///   replication-aware observer can detect the commit-log source
    ///   and short-circuit its loop-prevention.</item>
    /// </list>
    /// </summary>
    private async Task<SplitResult?> CommitSetAsync(string key, byte[] value, long expiresAtTicks)
    {
        // step 0 (build) - HLC tick (or override), build LwwValue. Version
        // vector is foreground-only; ILeafProjection.Apply does not advance it.
        // Prepared writes (saga prepare phase) skip the Version publication
        // because they route into the pending-tx map, not visible Entries;
        // publishing on prepare would advance the cache's saved callerClock
        // past prepare time and the cache's per-entry HLC delta filter would
        // then exclude the drained value when the saga's terminal mark
        // re-stamps and surfaces it. The terminal handler publishes Version
        // itself so the cache observes a single linearization-point
        // advance covering the whole saga's drained set.
        //
        // PublishVersionAdvance: lift Version[ReplicaId] to the entry's
        // own stamp. The stamp equals Entries[key].Timestamp by
        // construction, so the cache filter `lww.Timestamp > callerClock`
        // delivers this entry on any refresh where the caller's saved
        // callerClock is strictly less than the stamp (i.e. every fresh
        // LeafCacheGrain activation, and every refresh that has not yet
        // observed this write). VersionVector.Tick(ReplicaId) would call
        // HLC.Tick against DateTimeOffset.UtcNow.Ticks and could land
        // strictly above stamp, causing the filter to silently drop this
        // entry on its next refresh. Passing stamp directly avoids that.
        // See PublishVersionAdvance's XML doc for the full invariant.
        var stamp = AdvanceClockOrOverride();
        var isPrepared = LatticePreparedContext.Current;
        if (!isPrepared)
            PublishVersionAdvance(stamp);
        BumpLocalRevision();
        var newEntry = LwwValue<byte[]>.CreateWithExpiry(value, stamp, expiresAtTicks)
            with
            {
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
            };
        var delta = LatticeDeltaContext.Current;
        var batch = LatticeAtomicBatchContext.Current;
        var mutation = new LatticeMutation
        {
            TreeId = state.State.TreeId ?? string.Empty,
            Kind = MutationKind.Set,
            Key = key,
            Value = newEntry.IsTombstone ? null : newEntry.Value,
            Timestamp = newEntry.Timestamp,
            IsTombstone = newEntry.IsTombstone,
            ExpiresAtTicks = newEntry.ExpiresAtTicks,
            OriginClusterId = newEntry.OriginClusterId,
            VectorClock = newEntry.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            DeltaKind = delta?.Kind,
            DeltaPayload = delta?.Payload,
            AtomicBatchSize = batch?.Size ?? 0,
            AtomicBatchIndex = batch?.Index ?? 0,
            IsPrepared = isPrepared,
            // Stamp the leaf's owning chain-shard index so a sibling
            // shard's leaf reading the same WAL partition can filter
            // this entry out at activation-time replay. Falls back to
            // 0 for the V1 single-shard test path where SetShardIndexAsync
            // has not yet been called (every chain shard is shard 0).
            ShardIndex = state.State.ShardIndex ?? 0,
        };

        var options = await GetOptionsAsync();

        // step 1 (wal) - propagate exceptions: pre-Apply failure leaves
        // state untouched and the foreground caller observes the WAL error.
        var walStartTicks = Stopwatch.GetTimestamp();
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            await writer.AppendAsync(mutation);
        }
        RecordCommitStep("wal", walStartTicks);

        // step 2 (apply) - LWW-merge into the in-memory projection, or
        // into the per-leaf pending-tx map when the mutation is a saga
        // prepare-phase write. Prepared writes never trigger a leaf
        // split because they are not yet visible in Entries.
        var applyStartTicks = Stopwatch.GetTimestamp();
        SplitResult? splitResult = null;
        if (isPrepared)
        {
            AddPreparedMutation(mutation.TransactionId, key, newEntry);
        }
        else
        {
            StoreEntry(key, newEntry);
            // Foreground commit constructs a fresh LwwValue with the
            // default IsMigrated=false, so StoreEntry's merge clears
            // any stale migration provenance from a prior migrated
            // entry on the same key automatically - the flag rides
            // with the value, not in a side-channel map.
            if (state.State.Entries.Count > options.MaxLeafKeys)
            {
                splitResult = await SplitAsync();
            }
        }
        RecordCommitStep("apply", applyStartTicks);

        // step 3 (observer) - publish under a commit-log scope so a
        // replication-aware observer can detect the source and avoid
        // re-appending its own input back into the WAL.
        var observerStartTicks = Stopwatch.GetTimestamp();
        if (mutationObservers.HasObservers)
        {
            // For non-prepared writes, the key may have migrated to the
            // new sibling on a split - fall back to newEntry, which is
            // guaranteed by strict-HLC-tick monotonicity to be the
            // committed LWW winner. For prepared writes the entry is in
            // the pending-tx map (not Entries), so always use newEntry
            // verbatim; the observer payload's IsPrepared flag tells
            // downstream consumers the entry is not yet visible.
            LwwValue<byte[]> published;
            if (isPrepared)
            {
                published = newEntry;
            }
            else
            {
                published = state.State.Entries.TryGetValue(key, out var committed) ? committed : newEntry;
            }
            using (LatticeCommitLogContext.BeginScope())
            {
                await PublishSetAsync(key, published);
            }
        }
        RecordCommitStep("observer", observerStartTicks);

        return splitResult;
    }

    public async Task<SplitResult?> SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        SplitResult? lastSplit = null;
        foreach (var entry in entries)
        {
            var split = await SetAsync(entry.Key, entry.Value);
            if (split is not null)
                lastSplit = split;
        }
        return lastSplit;
    }

    public async Task<bool> DeleteAsync(string key)
    {
        var isPrepared = LatticePreparedContext.Current;

        // For non-prepared deletes, the absent / tombstoned short-circuit
        // saves an HLC tick and a WAL append. For prepared deletes the
        // saga still expects a pending-tx entry, so we always emit a
        // tombstone into the pending bucket - committing the saga must
        // make the absence durable (the caller's pre-saga value is
        // captured separately by the saga coordinator).
        if (!isPrepared && (!state.State.Entries.TryGetValue(key, out var existing) || existing.IsTombstone))
        {
            return false;
        }

        // step 0 (build) - HLC tick (or override), build tombstone, build mutation envelope.
        // PublishVersionAdvance lifts Version[ReplicaId] to the tombstone's
        // own Timestamp; the cache filter `lww.Timestamp > callerClock`
        // then delivers the tombstone on its next refresh. See
        // CommitSetAsync for the full invariant.
        var stamp = AdvanceClockOrOverride();
        // Prepared deletes route to the pending-tx map and skip the
        // Version publication for the same reason as CommitSetAsync (see
        // the build-step comment there for the cache-callerClock argument).
        if (!isPrepared)
            PublishVersionAdvance(stamp);
        BumpLocalRevision();
        var tombstone = LwwValue<byte[]>.Tombstone(stamp)
            with
            {
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
            };
        var delta = LatticeDeltaContext.Current;
        var batch = LatticeAtomicBatchContext.Current;
        var mutation = new LatticeMutation
        {
            TreeId = state.State.TreeId ?? string.Empty,
            Kind = MutationKind.Delete,
            Key = key,
            Timestamp = tombstone.Timestamp,
            IsTombstone = true,
            OriginClusterId = tombstone.OriginClusterId,
            VectorClock = tombstone.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            DeltaKind = delta?.Kind,
            DeltaPayload = delta?.Payload,
            AtomicBatchSize = batch?.Size ?? 0,
            AtomicBatchIndex = batch?.Index ?? 0,
            IsPrepared = isPrepared,
        };

        // step 1 (wal)
        var walStartTicks = Stopwatch.GetTimestamp();
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            await writer.AppendAsync(mutation);
        }
        RecordCommitStep("wal", walStartTicks);

        // step 2 (apply) - LWW-merge into the in-memory projection, or
        // into the per-leaf pending-tx map when the mutation is a saga
        // prepare-phase write.
        var applyStartTicks = Stopwatch.GetTimestamp();
        if (isPrepared)
        {
            AddPreparedMutation(mutation.TransactionId, key, tombstone);
        }
        else
        {
            StoreEntry(key, tombstone);
            // Tombstone has IsMigrated=false (default), so the merge
            // result inside StoreEntry clears any stale migration
            // marker for the same key naturally - no explicit cleanup
            // call required.
        }
        RecordCommitStep("apply", applyStartTicks);

        LatticeMetrics.LeafTombstonesCreated.Add(1, LeafTreeTag());

        // step 3 (observer) - inside a commit-log scope.
        var observerStartTicks = Stopwatch.GetTimestamp();
        if (mutationObservers.HasObservers)
        {
            using (LatticeCommitLogContext.BeginScope())
            {
                await PublishDeleteAsync(key, tombstone);
            }
        }
        RecordCommitStep("observer", observerStartTicks);

        return true;
    }

    public async Task<RangeDeleteResult> DeleteRangeAsync(string startInclusive, string endExclusive)
    {
        // Collect matching keys. Entries is a SortedDictionary so we can
        // break early once we pass endExclusive - but we must still report
        // whether we observed a key >= endExclusive so the shard
        // coordinator can terminate the chain walk deterministically.
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        List<string>? keysToDelete = null;
        var pastRange = false;
        foreach (var (key, lww) in state.State.Entries)
        {
            if (string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
            {
                pastRange = true;
                break;
            }

            if (string.Compare(key, startInclusive, StringComparison.Ordinal) >= 0
                && !lww.IsTombstone && !lww.IsExpired(nowTicks))
                (keysToDelete ??= []).Add(key);
        }

        if (keysToDelete is null)
        {
            // Nothing to delete on this leaf - skip the WAL append, the
            // HLC tick, and every other step. The shard-level publish
            // helper still emits a per-shard DeleteRange mutation with
            // HybridLogicalClock.Zero so replication consumers propagate
            // the range unconditionally.
            return new RangeDeleteResult { Deleted = 0, PastRange = pastRange };
        }

        // step 0 (build) - HLC tick (or override), build tombstone, build
        // mutation envelope covering the whole range. The leaf does not
        // publish the per-range mutation - that's a shard-level concern -
        // but it still appends the range tombstone to the WAL so a future
        // replay applies the same set-of-keys closure rather than each
        // individual key. PublishVersionAdvance lifts Version[ReplicaId]
        // to the range tombstone's own stamp so the cache delta filter
        // delivers every fresh tombstone (see CommitSetAsync for the
        // full invariant).
        var stamp = AdvanceClockOrOverride();
        PublishVersionAdvance(stamp);
        BumpLocalRevision();
        var tombstone = LwwValue<byte[]>.Tombstone(stamp)
            with
            {
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
            };
        var delta = LatticeDeltaContext.Current;
        var mutation = new LatticeMutation
        {
            TreeId = state.State.TreeId ?? string.Empty,
            Kind = MutationKind.DeleteRange,
            Key = startInclusive,
            EndExclusiveKey = endExclusive,
            Timestamp = tombstone.Timestamp,
            IsTombstone = true,
            OriginClusterId = tombstone.OriginClusterId,
            VectorClock = tombstone.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            DeltaKind = delta?.Kind,
            DeltaPayload = delta?.Payload,
            // Stamp the leaf's owning chain-shard index; see SetAsync
            // for the rationale. DeleteRange replay on the receiving
            // leaf iterates that leaf's own Entries only, so the
            // filter is not strictly required for correctness on
            // DeleteRange - but stamping consistently keeps every
            // mutation kind on the same wire shape so receivers and
            // operator tooling can rely on the slot being populated.
            ShardIndex = state.State.ShardIndex ?? 0,
        };

        // step 1 (wal)
        var walStartTicks = Stopwatch.GetTimestamp();
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            await writer.AppendAsync(mutation);
        }
        RecordCommitStep("wal", walStartTicks);

        // step 2 (apply) - tombstone every matched key with the same HLC.
        var applyStartTicks = Stopwatch.GetTimestamp();
        foreach (var key in keysToDelete)
        {
            StoreEntry(key, tombstone);
            // Range tombstone has IsMigrated=false (default); merge
            // inside StoreEntry naturally clears any stale migration
            // marker - the flag rides with the value, not in a
            // side-channel map.
        }
        RecordCommitStep("apply", applyStartTicks);

        LatticeMetrics.LeafTombstonesCreated.Add(keysToDelete.Count, LeafTreeTag());

        // No leaf-level observer publish for DeleteRange - the shard
        // coordinator publishes one per-shard mutation after the
        // chain walk completes. RecordCommitStep("observer", ...) is
        // skipped to avoid recording a zero-duration measurement that
        // would skew the histogram for the legitimate per-key emit
        // step on Set / Delete.

        return new RangeDeleteResult { Deleted = keysToDelete.Count, PastRange = pastRange };
    }

    public async Task<int> CountAsync()
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        var count = 0;
        foreach (var (key, lww) in state.State.Entries)
        {
            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                if (status == TxStatus.Committed)
                {
                    if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks)) count++;
                    continue;
                }
                // InFlight or Aborted - fall through to Entries
                // (pre-saga visibility). See GetWithPendingAsync.
            }
            if (lww.IsTombstone || lww.IsExpired(nowTicks)) continue;
            count++;
        }

        // Fresh committed pending keys that are NOT in Entries
        // (saga inserted a brand-new key) must also be counted.
        foreach (var (key, pending) in pendingKeys)
        {
            if (state.State.Entries.ContainsKey(key)) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            count++;
        }

        return count;
    }

    public async Task<LeafStats> GetStatsAsync()
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        var live = 0;
        var tombstones = 0;
        foreach (var (key, lww) in state.State.Entries)
        {
            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                if (status == TxStatus.Committed)
                {
                    if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) tombstones++;
                    else live++;
                    continue;
                }
                // InFlight or Aborted - fall through to Entries
                // (pre-saga visibility). See GetWithPendingAsync.
            }
            if (lww.IsTombstone || lww.IsExpired(nowTicks)) tombstones++;
            else live++;
        }

        // Fresh committed pending keys not yet in Entries.
        foreach (var (key, pending) in pendingKeys)
        {
            if (state.State.Entries.ContainsKey(key)) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) tombstones++;
            else live++;
        }

        return new LeafStats { LiveKeys = live, Tombstones = tombstones };
    }

    public Task<GrainId?> GetNextSiblingAsync() =>
        Task.FromResult(state.State.NextSibling);

    public async Task SetNextSiblingAsync(GrainId? siblingId)
    {
        state.State.NextSibling = siblingId;
        await PersistAsync();
    }

    public Task<GrainId?> GetPrevSiblingAsync() =>
        Task.FromResult(state.State.PrevSibling);

    public async Task SetPrevSiblingAsync(GrainId? siblingId)
    {
        state.State.PrevSibling = siblingId;
        await PersistAsync();
    }

    public async Task SetTreeIdAsync(string treeId)
    {
        if (state.State.TreeId is not null) return;
        var prevTreeId = state.State.TreeId;
        state.State.TreeId = treeId;
        try
        {
            await PersistAsync();
        }
        catch
        {
            // Class B revert: a thrown WriteStateAsync leaves the
            // in-memory TreeId set while storage stays null. The
            // idempotency guard above would then short-circuit every
            // retry from this activation, permanently divorcing the
            // leaf's in-memory tree id from storage. Roll back the
            // in-memory assignment so the next call retries the persist.
            state.State.TreeId = prevTreeId;
            throw;
        }
    }

    public Task<string?> GetTreeIdAsync() =>
        Task.FromResult(state.State.TreeId);

    public async Task SetShardIndexAsync(int shardIndex)
    {
        // Idempotent: skip the persist if the slot is already seeded.
        // The shard-root coordinator calls this once per leaf-create
        // alongside SetTreeIdAsync; a re-call (e.g. from a defensive
        // re-seed in a future code path) must not silently overwrite
        // the persisted value, both because the value is immutable
        // for a leaf's lifetime and because the writer would
        // otherwise pay an extra WriteStateAsync round-trip on every
        // shard-root activation that walks its leaves.
        if (state.State.ShardIndex is not null) return;
        var prevShardIndex = state.State.ShardIndex;
        state.State.ShardIndex = shardIndex;
        try
        {
            await PersistAsync();
        }
        catch
        {
            // Class B revert: see SetTreeIdAsync above. Without this,
            // the activation stamps ShardIndex on every foreground
            // commit while every peer (or a future reactivation) still
            // sees a null slot, and the replay-time ownership filter on
            // the cross-shard fanout regression gate silently drops the
            // legitimate records.
            state.State.ShardIndex = prevShardIndex;
            throw;
        }
    }

    public async Task SetKeyRangeAsync(string? lowKeyInclusive, string? highKeyExclusive)
    {
        // Idempotent on the low bound: every legitimate caller
        // (CompleteSplitAsync stamping a freshly-created sibling)
        // passes a non-null splitKey as the low bound, so a non-null
        // persisted LowKeyInclusive is the unambiguous "already
        // seeded" sentinel. Donors never call this - they update
        // their own HighKeyExclusive directly inside CompleteSplitAsync
        // when narrowing their own range to the split key.
        if (state.State.LowKeyInclusive is not null) return;
        var prevLowKey = state.State.LowKeyInclusive;
        var prevHighKey = state.State.HighKeyExclusive;
        state.State.LowKeyInclusive = lowKeyInclusive;
        state.State.HighKeyExclusive = highKeyExclusive;
        try
        {
            await PersistAsync();
        }
        catch
        {
            // Class B revert: see SetTreeIdAsync above. Both fields
            // are restored together because the guard short-circuits
            // on LowKeyInclusive alone - leaving the in-memory range
            // even partially seeded would re-route range scans against
            // a topology storage never accepted.
            state.State.LowKeyInclusive = prevLowKey;
            state.State.HighKeyExclusive = prevHighKey;
            throw;
        }
    }

    public Task SetCheckpointOffsetHintAsync(long offset)
    {
        // Routes through the existing ILeafProjection seam so the
        // unresolved-prepare clamp is honoured. For a freshly-created
        // sibling at birth there are no unresolved prepares so the
        // clamp is a no-op; the seam's monotonic-non-decrease guard
        // makes a re-call with a smaller offset a silent no-op.
        return ((ILeafProjection)this).SetCheckpointOffsetAsync(offset, CancellationToken.None);
    }

    public async Task<int> CompactTombstonesAsync(TimeSpan gracePeriod)
    {
        // Skip scan if nothing has changed since last compaction.
        if (state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version))
            return 0;

        var startTicks = Stopwatch.GetTimestamp();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var cutoff = nowTicks - gracePeriod.Ticks;
        var toRemove = new List<string>();
        var anyInGraceRemaining = false;
        var tombstonesRemoved = 0;
        var expiredRemoved = 0;

        foreach (var (key, lww) in state.State.Entries)
        {
            if (lww.IsTombstone)
            {
                if (lww.Timestamp.WallClockTicks <= cutoff)
                {
                    toRemove.Add(key);
                    tombstonesRemoved++;
                }
                else
                {
                    // Tombstone is still within the grace window - a future pass
                    // must re-scan it once the grace has elapsed.
                    anyInGraceRemaining = true;
                }
                continue;
            }

            // Reap expired live entries past the same grace period.
            // Reads already hide them; a short retention after expiry protects
            // against a stale merge resurrecting the entry (another replica
            // whose clock is behind could re-send the pre-expiry LwwValue).
            if (lww.ExpiresAtTicks != 0 && lww.ExpiresAtTicks <= nowTicks)
            {
                if (lww.ExpiresAtTicks <= cutoff)
                {
                    toRemove.Add(key);
                    expiredRemoved++;
                }
                else
                {
                    anyInGraceRemaining = true;
                }
            }
        }

        if (toRemove.Count > 0)
        {
            foreach (var key in toRemove)
            {
                RemoveEntry(key);
            }
        }

        // Only mark this version as "fully compacted" when no tombstones were
        // left in the grace window. Stamping while tombstones remain would
        // dead-end every subsequent pass until a new write ticks the version
        // vector (audit bug #2).
        if (!anyInGraceRemaining)
            state.State.LastCompactionVersion = state.State.Version.Clone();

        await PersistAsync();
        var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        var treeTag = LeafTreeTag();
        LatticeMetrics.LeafCompactionDuration.Record(elapsedMs, treeTag);
        if (tombstonesRemoved > 0)
            LatticeMetrics.LeafTombstonesReaped.Add(tombstonesRemoved, treeTag);
        if (expiredRemoved > 0)
            LatticeMetrics.LeafTombstonesExpired.Add(expiredRemoved, treeTag);
        return toRemove.Count;
    }

    public Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion)
    {
        // NOTE: Replication paths intentionally propagate expired entries.
        // Readers filter them via LwwValue.IsExpired; shipping them to peers
        // preserves CRDT convergence so LWW can resolve by timestamp on
        // replicas whose wall clocks are drifted. CompactTombstonesAsync
        // reaps them after the configured grace period on each replica.
        // If the caller's version dominates ours, they already have everything.
        if (sinceVersion.DominatesOrEquals(state.State.Version))
        {
            // Steady-state fast path: no pending split, no moved-away
            // slots to advertise. Return the process-wide empty-delta
            // singleton so the receiver's VersionVector.Merge folds in
            // nothing (the caller already dominates) and we elide three
            // heap allocations per read. See EmptyDelta XML doc above
            // for the safety argument.
            if (state.State.SplitKey is null
                && (state.State.MovedAwaySlots is null || state.State.MovedAwaySlots.Length == 0))
            {
                return EmptyDeltaTask;
            }

            // SplitKey or MovedAwaySlots is set: the caller needs the
            // prune signal even though Entries is empty. Allocate a
            // per-call envelope so the signal is observed; this branch
            // is rare (only fires between a split / moved-away commit
            // and the next compaction sweep).
            return Task.FromResult(new StateDelta
            {
                Entries = EmptyEntries,
                Version = state.State.Version.Clone(),
                SplitKey = state.State.SplitKey,
                MovedAwaySlots = state.State.MovedAwaySlots is { Length: > 0 } ms ? ms : null,
                MovedAwayVsc = state.State.MovedAwayVirtualShardCount,
            });
        }

        // Return all entries whose timestamp is newer than what the caller has seen.
        // We compare each entry's timestamp against the caller's clock for our replica.
        var callerClock = sinceVersion.GetClock(ReplicaId);
        var changed = new Dictionary<string, LwwValue<byte[]>>();

        foreach (var (key, lww) in state.State.Entries)
        {
            if (lww.Timestamp > callerClock)
            {
                changed[key] = lww;
            }
        }

        return Task.FromResult(new StateDelta
        {
            Entries = changed,
            Version = state.State.Version.Clone(),
            SplitKey = state.State.SplitKey,
            MovedAwaySlots = state.State.MovedAwaySlots is { Length: > 0 } ms2 ? ms2 : null,
            MovedAwayVsc = state.State.MovedAwayVirtualShardCount,
        });
    }

    public Task<StateDelta> GetDeltaSinceForSlotsAsync(VersionVector sinceVersion, int[] sortedMovedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sinceVersion);
        ArgumentNullException.ThrowIfNull(sortedMovedSlots);

        if (sortedMovedSlots.Length == 0 || sinceVersion.DominatesOrEquals(state.State.Version))
        {
            return EmptyDeltaTask;
        }

        var callerClock = sinceVersion.GetClock(ReplicaId);
        var changed = new Dictionary<string, LwwValue<byte[]>>();

        foreach (var (key, lww) in state.State.Entries)
        {
            if (lww.Timestamp <= callerClock) continue;
            var slot = ShardMap.GetVirtualSlot(key, virtualShardCount);
            if (Array.BinarySearch(sortedMovedSlots, slot) < 0) continue;
            changed[key] = lww;
        }

        return Task.FromResult(new StateDelta
        {
            Entries = changed,
            Version = state.State.Version.Clone(),
            SplitKey = state.State.SplitKey,
            MovedAwaySlots = state.State.MovedAwaySlots is { Length: > 0 } ms3 ? ms3 : null,
            MovedAwayVsc = state.State.MovedAwayVirtualShardCount,
        });
    }

    public async Task MergeEntriesAsync(Dictionary<string, LwwValue<byte[]>> entries)
    {
#if LATTICE_DIAG
        // DIAG leaf-cross-leaf-merge: fires when a sibling leaf or
        // a split-source leaf hands a batch of LWW values into this
        // leaf. This path stamps IsMigrated=true on every incoming
        // entry (see StoreEntry call below) but does NOT update
        // MovedAwaySlots on the SOURCE leaf - that mask is driven only
        // by ShardRootGrain.MarkLeavesMovedAwayAsync during a shard-
        // wide split. The V_{N-2} regression in Section 14 hinges on
        // whether a slot migration arrives via this path (no source-
        // side mask) or via the shard-split path (source-side mask
        // present). The DIAG event records the merge size and a key
        // sample so the trace can attribute each post-merge
        // commit-key event to the correct upstream channel.
        var diagKeySample = entries.Count == 0
            ? string.Empty
            : string.Join(",", entries.Keys.Take(8));
        DiagSink.Write($"[DIAG leaf-cross-leaf-merge] gid={context.GrainId} entriesCount={entries.Count} keySample=[{diagKeySample}] currentMovedSlots=[{(state.State.MovedAwaySlots is null ? "" : string.Join(',', state.State.MovedAwaySlots))}] currentClock={state.State.Clock}");
#endif
        // NOTE: Expired entries are merged as-is and not filtered here.
        // Replication must preserve them so CRDT LWW convergence is resolved
        // by timestamp, not by the wall clock of whichever replica happens to
        // see a write first. Readers filter expired entries; compaction reaps
        // them after the grace period.
        //
        // Track the high-water timestamp of the incoming batch so we can
        // (a) advance state.State.Clock past it (audit bug #3), and (b)
        // publish it as Version[ReplicaId] so LeafCacheGrain delta checks
        // detect the new entries. Using VersionVector.Tick(ReplicaId) here
        // would key the advance off DateTimeOffset.UtcNow.Ticks and could
        // land strictly above the merged entries' Timestamps - causing the
        // cache's `lww.Timestamp > callerClock` delta filter to silently
        // drop the freshly-merged values on its next refresh. Publishing
        // maxIncoming keeps Version[ReplicaId] equal to the latest stamp
        // any merged entry actually carries.
        var maxIncoming = HybridLogicalClock.Zero;
        foreach (var (key, incoming) in entries)
        {
            if (incoming.Timestamp > maxIncoming)
                maxIncoming = incoming.Timestamp;

            // Cross-leaf migration provenance: stamp IsMigrated=true on
            // the incoming value before merging. If the imported HLC
            // wins the LWW merge, the resulting Entries[K] carries the
            // flag (StoreEntry returns the merged winner). If a
            // pre-existing dominator wins, its own IsMigrated is
            // preserved by Merge - either way the value's provenance
            // travels with the value, and no out-of-band map is
            // required. The foreground orphan-drain guard in
            // BPlusLeafGrain.ApplyTxCommit reads existing.IsMigrated
            // to distinguish a migrated dominator (drain proceeds)
            // from a sibling-saga drain (drain skips).
            StoreEntry(key, incoming with { IsMigrated = true });
        }

        // Publish a Version advance so LeafCacheGrain delta checks detect
        // the new entries. Without this, a freshly-split sibling has an
        // empty version vector and the cache short-circuits (empty dominates
        // empty), never populating its local cache.
        if (entries.Count > 0)
        {
            // Advance the local HLC past the highest incoming timestamp so a
            // subsequent local write produces a stamp that dominates the just-
            // merged values (audit bug #3). Without this, a merged future-dated
            // entry silently wins LWW against every local write until wall clock
            // catches up.
            if (maxIncoming > state.State.Clock)
                state.State.Clock = maxIncoming;
            PublishVersionAdvance(maxIncoming);
            BumpLocalRevision();
        }

        await PersistAsync();
    }

    public async Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null)
    {
        var startTicks = Stopwatch.GetTimestamp();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var splitInProgress = state.State.SplitState == Primitives.SplitState.SplitInProgress;
        var splitKey = state.State.SplitKey;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();

        // Pre-size the result list to bound the small-end resize chain
        // (0 -> 4 -> 8 -> 16 -> ... -> 256 = 7 resizes for the common
        // ~250-entry-per-leaf shape). Capped at 256: for small leaves the
        // cap collapses to Entries.Count (no waste); for large leaves the
        // cap prevents the cycle-27 trap where pre-sizing to Entries.Count
        // over-allocates by ~10x when the range filter or split-key bound
        // truncates iteration well below the leaf's total entry count.
        // 256 is just above the typical page-size shape (KeysPageSize
        // default 512 / typical fanout 2-4 cursors = ~128-256 keys
        // per leaf per page) so it sized the initial array to the
        // expected emission, not the worst-case.
        var keys = new List<string>(capacity: Math.Min(state.State.Entries.Count, 256));
        foreach (var (key, lww) in state.State.Entries)
        {
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (splitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                break;

            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                continue;

            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0)
                continue;

            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                if (status == TxStatus.Committed)
                {
                    if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks))
                        keys.Add(key);
                    continue;
                }
                // InFlight, Aborted, or orphan-pending (committed bucket whose
                // saga terminal has already landed on this leaf) - fall through
                // to Entries. See GetWithPendingAsync for the orphan-pending
                // rationale: a late-arriving shadow-forward of a prepare can
                // bucket a saga whose terminal has already drained into Entries,
                // and surfacing the orphan would shadow the authoritative
                // Entries value (or a strictly-later saga's value).
            }

            if (lww.IsTombstone || lww.IsExpired(nowTicks))
                continue;

            keys.Add(key);
        }

        // Fresh committed pending keys not yet in Entries, respecting range filters.
        foreach (var (key, pending) in pendingKeys)
        {
            if (state.State.Entries.ContainsKey(key)) continue;
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0) continue;
            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0) continue;
            if (splitInProgress && splitKey is not null && string.Compare(key, splitKey, StringComparison.Ordinal) >= 0) continue;
            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0) continue;
            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            keys.Add(key);
        }
        keys.Sort(StringComparer.Ordinal);

        var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        LatticeMetrics.LeafScanDuration.Record(elapsedMs,
            LeafTreeTag(),
            new KeyValuePair<string, object?>(LatticeMetrics.TagOperation, "keys"));
        return keys;
    }

    public async Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null)
    {
        var startTicks = Stopwatch.GetTimestamp();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var splitInProgress = state.State.SplitState == Primitives.SplitState.SplitInProgress;
        var splitKey = state.State.SplitKey;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();

        var entries = new List<KeyValuePair<string, byte[]>>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (splitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                break;

            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                continue;

            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0)
                continue;

            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                if (status == TxStatus.Committed)
                {
                    if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks))
                        entries.Add(new KeyValuePair<string, byte[]>(key, pending.value.Value!));
                    continue;
                }
                // InFlight or Aborted - fall through to Entries
                // (pre-saga visibility). See GetWithPendingAsync.
            }

            if (lww.IsTombstone || lww.IsExpired(nowTicks))
                continue;

            entries.Add(new KeyValuePair<string, byte[]>(key, lww.Value!));
        }

        // Fresh committed pending keys not yet in Entries, respecting range filters.
        foreach (var (key, pending) in pendingKeys)
        {
            if (state.State.Entries.ContainsKey(key)) continue;
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0) continue;
            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0) continue;
            if (splitInProgress && splitKey is not null && string.Compare(key, splitKey, StringComparison.Ordinal) >= 0) continue;
            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0) continue;
            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            entries.Add(new KeyValuePair<string, byte[]>(key, pending.value.Value!));
        }
        entries.Sort(static (a, b) => StringComparer.Ordinal.Compare(a.Key, b.Key));

        var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        LatticeMetrics.LeafScanDuration.Record(elapsedMs,
            LeafTreeTag(),
            new KeyValuePair<string, object?>(LatticeMetrics.TagOperation, "entries"));
        return entries;
    }

    public async Task<Dictionary<string, byte[]>> GetLiveEntriesAsync()
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        var result = new Dictionary<string, byte[]>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                if (status == TxStatus.Committed)
                {
                    if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks))
                        result[key] = pending.value.Value!;
                    continue;
                }
                // InFlight or Aborted - fall through to Entries
                // (pre-saga visibility). See GetWithPendingAsync.
            }
            if (lww.IsTombstone || lww.IsExpired(nowTicks)) continue;
            result[key] = lww.Value!;
        }
        foreach (var (key, pending) in pendingKeys)
        {
            if (state.State.Entries.ContainsKey(key)) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            result[key] = pending.value.Value!;
        }
        return result;
    }

    /// <inheritdoc />
    public async Task<List<LwwEntry>> GetLiveRawEntriesAsync()
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        var result = new List<LwwEntry>(state.State.Entries.Count);
        foreach (var (key, lww) in state.State.Entries)
        {
            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                if (status == TxStatus.Committed)
                {
                    if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks))
                        result.Add(new LwwEntry(key, pending.value));
                    continue;
                }
                // InFlight or Aborted - fall through to Entries
                // (pre-saga visibility). See GetWithPendingAsync.
            }
            if (lww.IsTombstone || lww.IsExpired(nowTicks)) continue;
            result.Add(new LwwEntry(key, lww));
        }
        foreach (var (key, pending) in pendingKeys)
        {
            if (state.State.Entries.ContainsKey(key)) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            result.Add(new LwwEntry(key, pending.value));
        }
        return result;
    }

    /// <summary>
    /// Returns all key-value entries in this leaf including tombstones,
    /// preserving the original <see cref="LwwValue{T}"/> timestamps.
    /// Internal method for unit testing - not exposed on the grain interface
    /// to avoid Orleans generic type serialization issues.
    /// </summary>
    internal Task<Dictionary<string, LwwValue<byte[]>>> GetAllRawEntriesAsync()
    {
        return Task.FromResult(
            new Dictionary<string, LwwValue<byte[]>>(state.State.Entries));
    }

    public async Task<SplitResult?> MergeManyAsync(Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration = false)
    {
        // Recovery: if a previous split was interrupted, complete it first.
        if (state.State.SplitState == Primitives.SplitState.SplitInProgress)
        {
            var recovered = await CompleteSplitAsync();
            await PersistAsync();

            // Re-merge entries that belong to the new sibling.
            var siblingEntries = new Dictionary<string, LwwValue<byte[]>>();
            var localEntries = new Dictionary<string, LwwValue<byte[]>>();
            foreach (var (key, lww) in entries)
            {
                if (string.Compare(key, state.State.SplitKey!, StringComparison.Ordinal) >= 0)
                    siblingEntries[key] = lww;
                else
                    localEntries[key] = lww;
            }

            if (siblingEntries.Count > 0)
            {
                var sibling = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.SplitSiblingId!.Value);
                // Forward the caller's migration intent verbatim - a cross-shard migration
                // import that arrives during split recovery is still a migration on the sibling.
                await sibling.MergeManyAsync(siblingEntries, isCrossShardMigration);
            }

            // Merge remaining local entries.
            if (localEntries.Count > 0)
            {
                MergeIntoState(localEntries, isCrossShardMigration);
                await PersistAsync();
            }

            return recovered;
        }

        if (entries.Count == 0)
        {
            return null;
        }

        MergeIntoState(entries, isCrossShardMigration);

        SplitResult? splitResult = null;
        if (state.State.Entries.Count > (await GetOptionsAsync()).MaxLeafKeys)
        {
            splitResult = await SplitAsync();
        }

        await PersistAsync();
        return splitResult;
    }

    private void MergeIntoState(Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration)
    {
        // Track the high-water timestamp of the incoming batch so we can
        // (a) advance state.State.Clock past it (audit bug #3), and (b)
        // publish it as Version[ReplicaId] so LeafCacheGrain delta checks
        // detect the new entries. See MergeEntriesAsync for the full
        // invariant.
        var maxIncoming = HybridLogicalClock.Zero;
        foreach (var (key, incoming) in entries)
        {
            if (incoming.Timestamp > maxIncoming)
                maxIncoming = incoming.Timestamp;

            // Asymmetric migration-vs-foreground rule. Only fires on the
            // cross-shard migration callsites (TreeShardSplitGrain
            // -> ForwardMovedSlotEntriesAsync, and ShardRootGrain.Split.cs
            // shadow-forward). When the destination already has a
            // non-migration entry for the key, that entry is an
            // authoritative post-split foreground commit (a direct Set,
            // a saga's prepared-bucket drain, or a saga's cross-migration
            // LWW backstop) and MUST NOT be overwritten by a migration
            // import regardless of LWW HLC comparison: the migrated
            // record's HLC reflects the SOURCE leaf's accumulated clock
            // at migration time, which can dominate the freshly-created
            // destination's Clock at the time the foreground write landed
            // - even though the foreground write is logically newer
            // (post-split-routing-update) and the destination is the new
            // owner of the key.
            //
            // Non-migration callers (cross-cluster replication, tree-merge,
            // snapshot restore, intra-shard sibling-merge) use the
            // symmetric LWW-by-HLC contract: the incoming entry wins iff
            // its HLC dominates, regardless of the existing entry's
            // IsMigrated flag. For those callers the asymmetric guard
            // would silently drop legitimate higher-HLC imports and
            // tombstones (see the cross-cluster LWW contract tests).
            //
            // The Fix-M backstop pre-advance (BPlusLeafGrain.PendingTx.cs)
            // already handles the migration-FIRST, terminal-SECOND
            // ordering by Ticking the stamp past existing migrated
            // Entries' HLCs. This guard handles the reverse ordering
            // (terminal-FIRST on a fresh leaf, migration-SECOND with
            // an inverted HLC).
            if (isCrossShardMigration
                && state.State.Entries.TryGetValue(key, out var existing)
                && !existing.IsMigrated)
            {
                continue;
            }

            // Stamp IsMigrated=true ONLY on the cross-shard migration
            // callsite. Non-migration callers preserve the incoming entry's
            // own IsMigrated flag verbatim - that flag is normally `false`
            // for foreground writes on the source and `true` only when the
            // source-side entry was itself a migration import being
            // re-replicated / re-merged forward.
            StoreEntry(key, isCrossShardMigration ? (incoming with { IsMigrated = true }) : incoming);
        }

        if (entries.Count > 0)
        {
            // Advance the local HLC past the highest incoming timestamp so
            // subsequent local writes dominate the merged values (audit bug #3).
            if (maxIncoming > state.State.Clock)
                state.State.Clock = maxIncoming;
            PublishVersionAdvance(maxIncoming);
            BumpLocalRevision();
        }
    }

    public async Task ClearGrainStateAsync()
    {
        await state.ClearStateAsync();
        context.Deactivate(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "Tree purged"));
    }
}
