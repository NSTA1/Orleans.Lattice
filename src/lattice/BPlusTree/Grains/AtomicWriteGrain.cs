using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Saga coordinator for atomic multi-key writes (F-031). One grain activation
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
/// </summary>
internal sealed class AtomicWriteGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    ILogger<AtomicWriteGrain> logger,
    [PersistentState("atomic-write", LatticeOptions.StorageProviderName)]
    IPersistentState<AtomicWriteState> state) : IAtomicWriteGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "atomic-write-keepalive";
    private const int MaxRetriesPerStep = 1;

    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// Composite grain key (<c>{treeId}/{operationId}</c>); used for logging.
    /// </summary>
    private string OperationKey => context.GrainId.Key.ToString()!;

    /// <inheritdoc />
    public async Task ExecuteAsync(string treeId, List<KeyValuePair<string, byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entries);

        // Empty batch: fast success, no saga work, no reminder needed.
        if (entries.Count == 0) return;

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
    public async Task ReceiveReminder(string reminderName, TickStatus status)
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
                    logger.LogWarning(ex,
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
    /// Captures pre-saga values for every key via <see cref="ILattice.GetAsync"/>
    /// and persists the full saga state so that a crash mid-Execute can still
    /// compensate.
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

        var lattice = grainFactory.GetGrain<ILattice>(treeId);
        foreach (var entry in entries)
        {
            var current = await lattice.GetAsync(entry.Key);
            state.State.PreValues.Add(new AtomicPreValue
            {
                Key = entry.Key,
                Value = current,
                Existed = current is not null,
            });
        }

        state.State.Phase = AtomicWritePhase.Execute;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Dispatches on <see cref="AtomicWriteState.Phase"/> and drives the saga
    /// to a terminal state. Throws the original failure exception (or a
    /// surrogate) after compensation completes.
    /// </summary>
    private async Task RunSagaAsync()
    {
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
            await CompleteSagaAsync();
            throw new InvalidOperationException(
                $"Atomic write saga for tree '{state.State.TreeId}' failed and was rolled back: " +
                (state.State.FailureMessage ?? "unknown failure"));
        }

        if (state.State.Phase == AtomicWritePhase.Execute && state.State.NextIndex >= state.State.Entries.Count)
        {
            await CompleteSagaAsync();
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
                    logger.LogWarning(ex,
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
    /// wins over the prior commit.
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
                    await lattice.SetAsync(pre.Key, pre.Value);
                else
                    await lattice.DeleteAsync(pre.Key);

                state.State.NextIndex--;
                state.State.RetriesOnCurrentStep = 0;
                await state.WriteStateAsync();
            }
            catch (Exception ex) when (state.State.RetriesOnCurrentStep < MaxRetriesPerStep)
            {
                state.State.RetriesOnCurrentStep++;
                await state.WriteStateAsync();
                logger.LogWarning(ex,
                    "Atomic-write saga {OperationKey}: retrying compensation of step {Index} (attempt {Attempt}).",
                    OperationKey, index, state.State.RetriesOnCurrentStep);
            }
            catch (Exception ex)
            {
                // Persistent compensation failure — log and stop. The saga is
                // poisoned; state remains Compensate so a future reminder tick
                // can retry once the underlying fault clears.
                logger.LogError(ex,
                    "Atomic-write saga {OperationKey}: compensation of step {Index} failed after retries; saga is poisoned.",
                    OperationKey, index);
                throw;
            }
        }
    }

    /// <summary>
    /// Marks the saga Completed, unregisters the keepalive reminder, and
    /// requests deactivation. Safe to call in both success and post-compensation
    /// paths.
    /// </summary>
    private async Task CompleteSagaAsync()
    {
        state.State.Phase = AtomicWritePhase.Completed;
        state.State.RetriesOnCurrentStep = 0;
        await state.WriteStateAsync();
        await UnregisterKeepaliveAsync();
        this.DeactivateOnIdle();
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

    private Task RegisterKeepaliveAsync() =>
        reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
            if (reminder is not null)
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Atomic-write saga {OperationKey}: failed to unregister keepalive reminder.",
                OperationKey);
        }
    }
}
