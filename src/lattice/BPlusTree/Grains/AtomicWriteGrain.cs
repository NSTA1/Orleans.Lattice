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

        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lenBuf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lenBuf, entries.Count);
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

        var lattice = grainFactory.GetGrain<ILattice>(treeId);
        var routing = await lattice.GetRoutingAsync();
        var nowTicks = DateTimeOffset.UtcNow.UtcTicks;

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
    /// Dispatches on <see cref="AtomicWriteState.Phase"/> and drives the saga
    /// to a terminal state. Throws the original failure exception (or a
    /// surrogate) after compensation completes.
    /// </summary>
    private async Task RunSagaAsync()
    {
        StampOperationIdContext();

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
            await CompensatePhaseAsync();
            await CompleteSagaAsync(success: false);
            throw new InvalidOperationException(
                $"Atomic write saga for tree '{state.State.TreeId}' failed and was rolled back: " +
                (state.State.FailureMessage ?? "unknown failure"));
        }

        if (state.State.Phase == AtomicWritePhase.Execute && state.State.NextIndex >= state.State.Entries.Count)
        {
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
            var entry = state.State.Entries[state.State.NextIndex];

            try
            {
                await lattice.SetAsync(entry.Key, entry.Value);
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
    /// Rolls back committed entries in reverse order by rewriting pre-saga
    /// values (or tombstoning previously-absent keys). Each revert uses a
    /// fresh HLC via the normal write path; LWW merge guarantees the revert
    /// wins over the prior commit. Entries captured with a non-zero
    /// <see cref="AtomicPreValue.ExpiresAtTicks"/> are restored through the
    /// TTL overload <see cref="ILattice.SetAsync(string, byte[], TimeSpan, CancellationToken)"/>
    /// with the remaining time-to-live computed against the current wall clock,
    /// so expiry is preserved across compensation. If the entry's
    /// absolute expiry has already elapsed by the time compensation runs it
    /// is treated as absent and tombstoned instead.
    /// </summary>
    private async Task CompensatePhaseAsync()
    {
        var lattice = grainFactory.GetGrain<ILattice>(state.State.TreeId);

        // On reminder-driven re-entry, reset the per-step retry counter so a
        // transient fault that outlived a previous activation can be retried
        // fresh rather than stalling at MaxRetriesPerStep immediately.
        if (state.State.RetriesOnCurrentStep > 0)
        {
            state.State.RetriesOnCurrentStep = 0;
            await state.WriteStateAsync();
        }

        // Compensation walks committed writes in reverse. The saga persists
        // NextIndex during Execute to point at the next uncommitted entry, so
        // committed entries are [0 .. NextIndex-1]. During Compensate we treat
        // NextIndex as the next index still needing revert (walks downward).
        // On fresh Compensate entry, NextIndex equals the count of committed
        // writes. Crash-resume restarts from whatever value was last persisted.
        while (state.State.NextIndex > 0)
        {
            var index = state.State.NextIndex - 1;
            var pre = state.State.PreValues[index];

            try
            {
                if (pre.Existed && pre.Value is not null)
                {
                    if (pre.ExpiresAtTicks > 0)
                    {
                        // Preserve the original absolute expiry by computing
                        // the remaining time-to-live against the current wall
                        // clock. If the entry's expiry has already elapsed,
                        // treat it as absent and tombstone instead — public
                        // reads would already hide an expired entry.
                        var remainingTicks = pre.ExpiresAtTicks - DateTimeOffset.UtcNow.UtcTicks;
                        if (remainingTicks <= 0)
                            await lattice.DeleteAsync(pre.Key);
                        else
                            await lattice.SetAsync(pre.Key, pre.Value, TimeSpan.FromTicks(remainingTicks));
                    }
                    else
                    {
                        await lattice.SetAsync(pre.Key, pre.Value);
                    }
                }
                else
                {
                    await lattice.DeleteAsync(pre.Key);
                }

                state.State.NextIndex--;
                state.State.RetriesOnCurrentStep = 0;
                await state.WriteStateAsync();
            }
            catch (Exception ex) when (state.State.RetriesOnCurrentStep < MaxRetriesPerStep)
            {
                state.State.RetriesOnCurrentStep++;
                await state.WriteStateAsync();
                Logger.LogWarning(ex,
                    "Atomic-write saga {OperationKey}: retrying compensation of step {Index} (attempt {Attempt}).",
                    OperationKey, index, state.State.RetriesOnCurrentStep);
            }
            catch (Exception ex)
            {
                // Persistent compensation failure — log and stop. The saga is
                // poisoned; state remains Compensate so a future reminder tick
                // can retry once the underlying fault clears.
                Logger.LogError(ex,
                    "Atomic-write saga {OperationKey}: compensation of step {Index} failed after retries; saga is poisoned.",
                    OperationKey, index);
                throw;
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
