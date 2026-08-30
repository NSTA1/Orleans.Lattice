using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The generic atomic-action (saga / TCC) coordinator behind
/// <see cref="IAtomicActionGrain"/>. One activation per operation id runs an ordered
/// plan of forward/compensating steps all-or-nothing, and the step-sequencing and
/// crash-resume safety decisions are delegated to the pure
/// <see cref="AtomicActionPlanCore"/> so the exact rule the grain runs is the one
/// the Coyote model checks.
/// <para>
/// <b>Durability.</b> The plan, the per-step status vector, the phase, and the
/// captured tree pre-images are persisted after every step transition, so a
/// reactivation resumes from the persisted state and reaches a terminal outcome
/// exactly once. Crash recovery is reminder-driven: a keepalive reminder registered
/// at saga start reactivates a collected grain and drives the resume through the
/// same pure core.
/// </para>
/// <para>
/// <b>Delegation.</b> A built-in tree-write step delegates its forward write to the
/// tree's verified atomic-write coordinator (<see cref="IAtomicWriteGrain"/>), so it
/// inherits that machinery's atomicity guarantee, and captures each key's pre-image
/// so the coordinator can synthesize the step's compensation (restore the
/// pre-images). A custom step invokes a caller-registered handler resolved by id
/// against the fail-closed handler catalog.
/// </para>
/// <para>
/// <b>Retention.</b> After the saga reaches a terminal state the grain arms a
/// one-shot retention reminder (<see cref="LatticeOptions.AtomicActionRetention"/>,
/// default 48h). When it fires the grain clears its persisted state and
/// deactivates, so a re-issue of the same operation id within the window observes
/// the memoized outcome while saga state does not leak forever.
/// </para>
/// </summary>
internal sealed class AtomicActionGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    IAtomicActionCatalog catalog,
    ILogger<AtomicActionGrain> logger,
    [PersistentState("atomic-action", LatticeOptions.StorageProviderName)]
    IPersistentState<AtomicActionState> state)
    : TtlGrain<AtomicActionGrain>(context, reminderRegistry, logger), IAtomicActionGrain
{
    private const string KeepaliveReminderName = "atomic-action-keepalive";
    private const string RetentionReminderName = "atomic-action-retention";

    /// <summary>
    /// The per-step compensation retry budget: a compensating effect that faults is
    /// retried up to this many additional times (across reminder-driven resumes)
    /// before the saga parks in
    /// <see cref="AtomicActionPhase.CompensationFailed"/>.
    /// </summary>
    private const int MaxCompensationRetries = 1;

    private string OperationId => GrainContext.GrainId.Key.ToString()!;

    /// <summary>
    /// Rejects an operation id that cannot survive the composite saga grain key.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A per-tree write step is dispatched to an <see cref="IAtomicWriteGrain"/>
    /// keyed <c>{treeId}/{operationId}::aa::{index}</c>, and that saga splits the
    /// key at its <b>last</b> separator to recover the tree id and the operation
    /// id. A tree id may itself be segmented (a tenant-composed
    /// <c>t/{tenantId}/{name}</c>), so the last separator is the only choice that
    /// is correct for every tree - which in turn requires that an operation id
    /// contribute no separator of its own. The two sibling entry points that
    /// compose the same key space (<c>LatticeGrain.SetManyAtomicAsync</c> and the
    /// cross-tree coordinator) already enforce exactly this; the atomic-action
    /// operation id arrives as this grain's own key, so it is enforced here at
    /// the single point every plan funnels through.
    /// </para>
    /// <para>
    /// Fail-closed and allocation-free: the check runs before any state is read
    /// or written, and turns what would otherwise be a silently mis-keyed saga
    /// (a plausible-but-wrong tree id and correlation id) into an actionable
    /// caller error.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentException">
    /// The grain key is empty, whitespace, or contains <c>'/'</c>.
    /// </exception>
    private void ValidateOperationId()
    {
        var operationId = OperationId;
        if (string.IsNullOrWhiteSpace(operationId) || operationId.Contains('/'))
        {
            throw new ArgumentException(
                "An atomic-action operationId (this grain's key) must be non-empty and must not "
                + "contain '/' (reserved as the grain-key separator). Resolve the grain with an "
                + "operationId that carries no '/'.",
                nameof(operationId));
        }
    }

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => optionsMonitor.CurrentValue.AtomicActionRetention;

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "Atomic-action saga {OperationId}: retention window expired; clearing state.",
            OperationId);
        await state.ClearStateAsync();
    }

    /// <inheritdoc />
    protected override async Task OnOtherReminderAsync(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;

        switch (state.State.Phase)
        {
            case AtomicActionPhase.Forward:
            case AtomicActionPhase.Compensate:
                if (!state.State.Started) goto default;
                try
                {
                    await RunSagaAsync();
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex,
                        "Atomic-action saga {OperationId} failed on reminder-driven resume.",
                        OperationId);
                }
                break;
            default:
                await UnregisterKeepaliveAsync();
                this.DeactivateOnIdle();
                break;
        }
    }

    /// <inheritdoc />
    public async Task<AtomicActionOutcome> ExecuteAsync(AtomicActionPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ValidateOperationId();

        // Idempotent re-entry: a plan already accepted under this operation id must
        // match the original, and a terminal saga returns its memoized outcome.
        if (state.State.Started)
        {
            var incomingFingerprint = ComputePlanFingerprint(plan);
            if (state.State.PlanFingerprint is { } persisted
                && !CryptographicOperations.FixedTimeEquals(persisted, incomingFingerprint))
            {
                throw new ArgumentException(
                    "This operationId was already submitted with a different atomic-action plan. "
                    + "Reusing an operationId requires the identical plan, or use a new operationId.",
                    nameof(plan));
            }

            if (IsTerminal(state.State.Phase))
            {
                return BuildTerminalOutcomeOrThrow();
            }

            // A prior, still-in-flight submission is resumed rather than restarted.
            await RunSagaAsync();
            return BuildTerminalOutcomeOrThrow();
        }

        // Fresh saga: validate, stamp handler version tags, seed state, arm the
        // keepalive reminder (before any effect runs, so a crash mid-forward still
        // has a reminder-driven recovery path), then run.
        var options = optionsMonitor.CurrentValue;
        ValidateAndStampPlan(plan, options);

        state.State.Started = true;
        state.State.Phase = AtomicActionPhase.Forward;
        state.State.Steps = plan.Steps;
        state.State.StepStatuses = new List<AtomicActionStepStatus>(plan.Steps.Count);
        for (var i = 0; i < plan.Steps.Count; i++)
        {
            state.State.StepStatuses.Add(AtomicActionStepStatus.Pending);
        }

        state.State.FailedStepIndex = -1;
        state.State.FailureMessage = null;
        state.State.TreeWritePreImages = [];
        state.State.CompensationRetries = 0;
        state.State.PlanFingerprint = ComputePlanFingerprint(plan);
        state.State.StartedAtTicks = DateTimeOffset.UtcNow.UtcTicks;
        await state.WriteStateAsync();

        await RegisterKeepaliveAsync();

        await RunSagaAsync();
        return BuildTerminalOutcomeOrThrow();
    }

    /// <inheritdoc />
    public Task<AtomicActionOutcome?> TryGetOutcomeAsync() =>
        Task.FromResult(state.State.Started && IsTerminal(state.State.Phase)
            ? BuildTerminalOutcome()
            : (AtomicActionOutcome?)null);

    /// <summary>
    /// Drives the saga to a terminal state by repeatedly consulting the pure core
    /// and running the resolved effect, persisting after every transition.
    /// </summary>
    private async Task RunSagaAsync()
    {
        while (true)
        {
            var statuses = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(state.State.StepStatuses);
            var decision = AtomicActionPlanCore.Decide(statuses, state.State.Phase);

            switch (decision.Kind)
            {
                case AtomicActionActionKind.RunForward:
                    if (!await RunForwardStepAsync(decision.Index))
                    {
                        // Forward fault: the saga has pivoted to compensation; the
                        // next loop iteration resolves the first reverse step.
                    }
                    break;

                case AtomicActionActionKind.Compensate:
                    await RunCompensateStepAsync(decision.Index);
                    break;

                case AtomicActionActionKind.Commit:
                    await SettleTerminalAsync(AtomicActionPhase.Committed);
                    return;

                case AtomicActionActionKind.SettleCompensated:
                    await SettleTerminalAsync(AtomicActionPhase.Compensated);
                    return;

                case AtomicActionActionKind.None:
                default:
                    return;
            }

            if (IsTerminal(state.State.Phase))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Runs the forward effect of step <paramref name="index"/>. On success marks it
    /// <see cref="AtomicActionStepStatus.ForwardDone"/>; on fault records the failure
    /// and pivots the saga to <see cref="AtomicActionPhase.Compensate"/>. Returns
    /// <see langword="true"/> on success.
    /// </summary>
    private async Task<bool> RunForwardStepAsync(int index)
    {
        var step = state.State.Steps[index];

        // Resolve (and version-check) a custom handler BEFORE the effect try-block
        // so a fail-closed resolution - an unregistered id or a version tag that
        // changed under an in-flight saga - propagates out and parks the resume,
        // rather than being mistaken for an ordinary forward fault that would
        // compensate against a possibly-changed effect.
        var handler = step.Kind == AtomicActionStepKind.TreeWrite ? null : ResolveHandlerForResume(step);

        try
        {
            if (step.Kind == AtomicActionStepKind.TreeWrite)
            {
                await RunTreeWriteForwardAsync(index, step);
            }
            else
            {
                await handler!.ForwardAsync(BuildContext(step));
            }

            state.State.StepStatuses[index] = AtomicActionStepStatus.ForwardDone;
            await state.WriteStateAsync();
            LatticeMetrics.AtomicActionStep.Add(1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, "forward"),
                new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, "ok"),
                LatticeTenantLabel.Platform);
            return true;
        }
        catch (Exception ex)
        {
            LatticeMetrics.AtomicActionStep.Add(1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, "forward"),
                new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, "fault"),
                LatticeTenantLabel.Platform);
            Logger.LogWarning(ex,
                "Atomic-action saga {OperationId}: forward step {Index} faulted; compensating.",
                OperationId, index);
            state.State.FailedStepIndex = index;
            state.State.FailureMessage = ex.Message;
            state.State.Phase = AtomicActionPhase.Compensate;
            await state.WriteStateAsync();
            return false;
        }
    }

    /// <summary>
    /// Runs the compensating effect of step <paramref name="index"/>. On success
    /// marks it <see cref="AtomicActionStepStatus.Compensated"/>; on fault, after the
    /// per-step retry budget, parks the saga in
    /// <see cref="AtomicActionPhase.CompensationFailed"/>.
    /// </summary>
    private async Task RunCompensateStepAsync(int index)
    {
        var step = state.State.Steps[index];

        // As on the forward path, resolve/version-check the custom handler before
        // the effect try-block so a fail-closed resolution parks the resume rather
        // than being counted as a compensation fault.
        var handler = step.Kind == AtomicActionStepKind.TreeWrite ? null : ResolveHandlerForResume(step);

        try
        {
            if (step.Kind == AtomicActionStepKind.TreeWrite)
            {
                await RunTreeWriteCompensateAsync(index, step);
            }
            else
            {
                await handler!.CompensateAsync(BuildContext(step));
            }

            state.State.StepStatuses[index] = AtomicActionStepStatus.Compensated;
            state.State.CompensationRetries = 0;
            await state.WriteStateAsync();
            LatticeMetrics.AtomicActionStep.Add(1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, "compensate"),
                new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, "ok"),
                LatticeTenantLabel.Platform);
        }
        catch (Exception ex)
        {
            LatticeMetrics.AtomicActionStep.Add(1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, "compensate"),
                new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, "fault"),
                LatticeTenantLabel.Platform);

            if (state.State.CompensationRetries >= MaxCompensationRetries)
            {
                Logger.LogError(ex,
                    "Atomic-action saga {OperationId}: compensation of step {Index} faulted past its retry budget; parking in CompensationFailed.",
                    OperationId, index);
                state.State.FailedStepIndex = index;
                state.State.FailureMessage = ex.Message;
                await SettleTerminalAsync(AtomicActionPhase.CompensationFailed);
                return;
            }

            state.State.CompensationRetries++;
            await state.WriteStateAsync();
            Logger.LogWarning(ex,
                "Atomic-action saga {OperationId}: compensation of step {Index} faulted (retry {Retry}/{Budget}).",
                OperationId, index, state.State.CompensationRetries, MaxCompensationRetries);
        }
    }

    /// <summary>
    /// Runs a built-in tree-write step's forward write: captures each key's pre-image
    /// (once, persisted before the write), then delegates the atomic multi-key write
    /// to the tree's verified atomic-write coordinator.
    /// </summary>
    private async Task RunTreeWriteForwardAsync(int index, AtomicActionStep step)
    {
        var entries = step.Entries ?? [];

        if (!state.State.TreeWritePreImages.Exists(p => p.StepIndex == index))
        {
            var tree = grainFactory.GetGrain<ILattice>(step.TreeId);
            var preImage = new AtomicActionTreePreImage { StepIndex = index };
            foreach (var entry in entries)
            {
                var current = await tree.GetAsync(entry.Key);
                preImage.Values.Add(new AtomicActionTreePreValue
                {
                    Key = entry.Key,
                    Value = current,
                    Existed = current is not null,
                });
            }

            state.State.TreeWritePreImages.Add(preImage);
            await state.WriteStateAsync();
        }

        var (kvEntries, deletes) = ToWriteBatch(entries);
        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{step.TreeId}/{OperationId}::aa::{index}");
        await saga.ExecuteAsync(step.TreeId, kvEntries, deletes);
    }

    /// <summary>
    /// Compensates a built-in tree-write step by restoring the captured pre-images:
    /// re-upserting each key that existed before the step and deleting each key that
    /// was absent, as one atomic write delegated to the atomic-write coordinator.
    /// </summary>
    private async Task RunTreeWriteCompensateAsync(int index, AtomicActionStep step)
    {
        var preImage = state.State.TreeWritePreImages.Find(p => p.StepIndex == index);
        if (preImage is null || preImage.Values.Count == 0)
        {
            return;
        }

        var entries = new List<KeyValuePair<string, byte[]>>(preImage.Values.Count);
        var deletes = new List<bool>(preImage.Values.Count);
        var anyDelete = false;
        foreach (var pre in preImage.Values)
        {
            if (pre.Existed)
            {
                entries.Add(new KeyValuePair<string, byte[]>(pre.Key, pre.Value ?? []));
                deletes.Add(false);
            }
            else
            {
                entries.Add(new KeyValuePair<string, byte[]>(pre.Key, []));
                deletes.Add(true);
                anyDelete = true;
            }
        }

        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{step.TreeId}/{OperationId}::aa::{index}::comp");
        await saga.ExecuteAsync(step.TreeId, entries, anyDelete ? deletes : null);
    }

    /// <summary>
    /// Records the terminal phase, emits the saga's terminal telemetry, arms the
    /// retention reminder, and drops the keepalive reminder.
    /// </summary>
    private async Task SettleTerminalAsync(AtomicActionPhase terminal)
    {
        state.State.Phase = terminal;
        await state.WriteStateAsync();

        var outcomeTag = terminal switch
        {
            AtomicActionPhase.Committed => "committed",
            AtomicActionPhase.Compensated => "compensated",
            _ => "compensation_failed",
        };
        LatticeMetrics.AtomicActionCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcomeTag),
            LatticeTenantLabel.Platform);

        if (state.State.StartedAtTicks > 0)
        {
            var elapsedMs = (DateTimeOffset.UtcNow.UtcTicks - state.State.StartedAtTicks) / (double)TimeSpan.TicksPerMillisecond;
            if (elapsedMs >= 0)
            {
                LatticeMetrics.AtomicActionDuration.Record(elapsedMs,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcomeTag),
                    LatticeTenantLabel.Platform);
            }
        }

        await UnregisterKeepaliveAsync();
        await SlideTtlAsync();
    }

    /// <summary>Builds the invocation context for a custom step's handler.</summary>
    private IAtomicActionContext BuildContext(AtomicActionStep step) =>
        new AtomicActionContext(OperationId, step.ArgsPayload, grainFactory);

    /// <summary>
    /// Re-resolves a custom step's handler on the saga path (including resume),
    /// failing closed if the id is no longer registered and parking-worthy if the
    /// registered version tag no longer matches the one stamped at saga start.
    /// </summary>
    private IAtomicActionHandler ResolveHandlerForResume(AtomicActionStep step)
    {
        var registration = catalog.TryResolve(step.HandlerId)
            ?? throw new AtomicActionHandlerNotRegisteredException(
                $"Atomic-action handler '{step.HandlerId}' is not registered on this silo.",
                step.HandlerId);

        if (!string.Equals(registration.VersionTag, step.VersionTag, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Atomic-action handler '{step.HandlerId}' changed version tag from '{step.VersionTag}' to "
                + $"'{registration.VersionTag}' while a saga was in flight; the saga cannot safely replay a changed effect.");
        }

        return registration.Handler;
    }

    /// <summary>
    /// Validates a fresh plan and stamps each custom step's version tag from the
    /// registered handler. Fails closed on an unregistered handler id.
    /// </summary>
    private void ValidateAndStampPlan(AtomicActionPlan plan, LatticeOptions options)
    {
        if (plan.Steps.Count == 0)
        {
            throw new ArgumentException("An atomic-action plan must contain at least one step.", nameof(plan));
        }

        if (plan.Steps.Count > options.MaxAtomicActionSteps)
        {
            throw new ArgumentException(
                $"An atomic-action plan may contain at most {options.MaxAtomicActionSteps} steps (got {plan.Steps.Count}).",
                nameof(plan));
        }

        foreach (var step in plan.Steps)
        {
            if (step.Kind == AtomicActionStepKind.TreeWrite)
            {
                if (string.IsNullOrEmpty(step.TreeId))
                {
                    throw new ArgumentException("A tree-write step requires a non-empty tree id.", nameof(plan));
                }

                if (step.Entries is null || step.Entries.Count == 0)
                {
                    throw new ArgumentException("A tree-write step requires at least one entry.", nameof(plan));
                }

                continue;
            }

            if (string.IsNullOrEmpty(step.HandlerId))
            {
                throw new ArgumentException("A custom step requires a non-empty handler id.", nameof(plan));
            }

            if (step.ArgsPayload.Length > options.MaxAtomicActionArgsBytes)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(plan),
                    step.ArgsPayload.Length,
                    $"A custom step's args payload of {step.ArgsPayload.Length} bytes exceeds the "
                    + $"{options.MaxAtomicActionArgsBytes}-byte limit for handler '{step.HandlerId}'.");
            }

            var registration = catalog.TryResolve(step.HandlerId)
                ?? throw new AtomicActionHandlerNotRegisteredException(
                    $"Atomic-action handler '{step.HandlerId}' is not registered on this silo.",
                    step.HandlerId);

            step.VersionTag = registration.VersionTag;
        }
    }

    /// <summary>Splits a tree-write step's entries into the value batch and delete channel the atomic-write coordinator expects.</summary>
    private static (List<KeyValuePair<string, byte[]>> Entries, List<bool>? Deletes) ToWriteBatch(List<AtomicActionEntry> entries)
    {
        var kv = new List<KeyValuePair<string, byte[]>>(entries.Count);
        var deletes = new List<bool>(entries.Count);
        var anyDelete = false;
        foreach (var entry in entries)
        {
            kv.Add(new KeyValuePair<string, byte[]>(entry.Key, entry.Delete ? [] : entry.Value));
            deletes.Add(entry.Delete);
            anyDelete |= entry.Delete;
        }

        return (kv, anyDelete ? deletes : null);
    }

    private static bool IsTerminal(AtomicActionPhase phase) =>
        phase is AtomicActionPhase.Committed
            or AtomicActionPhase.Compensated
            or AtomicActionPhase.CompensationFailed;

    private AtomicActionOutcome BuildTerminalOutcome()
    {
        var status = state.State.Phase switch
        {
            AtomicActionPhase.Committed => AtomicActionStatus.Committed,
            AtomicActionPhase.Compensated => AtomicActionStatus.Compensated,
            _ => AtomicActionStatus.CompensationFailed,
        };
        return new AtomicActionOutcome(status, state.State.FailedStepIndex, state.State.FailureMessage);
    }

    private AtomicActionOutcome BuildTerminalOutcomeOrThrow()
    {
        var outcome = BuildTerminalOutcome();
        if (outcome.Status == AtomicActionStatus.CompensationFailed)
        {
            throw new CompensationFailedException(
                $"Atomic-action saga '{OperationId}' faulted on step {state.State.FailedStepIndex} and a compensating "
                + $"effect itself faulted; the saga parked in CompensationFailed and requires operator intervention. "
                + $"Originating failure: {state.State.FailureMessage}",
                state.State.FailedStepIndex);
        }

        return outcome;
    }

    /// <summary>Computes a stable SHA-256 fingerprint of a plan's structure for idempotent-re-entry detection.</summary>
    private static byte[] ComputePlanFingerprint(AtomicActionPlan plan)
    {
        using var buffer = new MemoryStream();

        foreach (var step in plan.Steps)
        {
            buffer.WriteByte((byte)step.Kind);
            WriteText(buffer, step.HandlerId);
            WriteBytes(buffer, step.ArgsPayload);
            WriteText(buffer, step.TreeId);
            if (step.Entries is null)
            {
                WriteBytes(buffer, null);
            }
            else
            {
                WriteInt(buffer, step.Entries.Count);
                foreach (var entry in step.Entries)
                {
                    WriteText(buffer, entry.Key);
                    WriteBytes(buffer, entry.Value);
                    buffer.WriteByte(entry.Delete ? (byte)1 : (byte)0);
                }
            }
        }

        buffer.Position = 0;
        return SHA256.HashData(buffer);
    }

    private static void WriteInt(Stream stream, int value)
    {
        Span<byte> buf = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(buf, value);
        stream.Write(buf);
    }

    private static void WriteBytes(Stream stream, byte[]? bytes)
    {
        WriteInt(stream, bytes?.Length ?? -1);
        if (bytes is not null)
        {
            stream.Write(bytes);
        }
    }

    private static void WriteText(Stream stream, string text) =>
        WriteBytes(stream, Encoding.UTF8.GetBytes(text));

    private async Task RegisterKeepaliveAsync()
    {
        try
        {
            await ReminderServiceReadiness.RetryWhileInitializingAsync(() =>
                ReminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: GrainContext.GrainId,
                    reminderName: KeepaliveReminderName,
                    dueTime: TimeSpan.FromMinutes(1),
                    period: TimeSpan.FromMinutes(1)));
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Atomic-action saga {OperationId}: failed to register keepalive reminder (non-fatal).",
                OperationId);
        }
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await ReminderRegistry.GetReminder(GrainContext.GrainId, KeepaliveReminderName);
            if (reminder is not null)
            {
                await ReminderRegistry.UnregisterReminder(GrainContext.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Atomic-action saga {OperationId}: failed to unregister keepalive reminder (non-fatal).",
                OperationId);
        }
    }

    /// <summary>The concrete <see cref="IAtomicActionContext"/> passed to a handler's effects.</summary>
    private sealed class AtomicActionContext(string operationId, byte[] args, IGrainFactory grainFactory)
        : IAtomicActionContext
    {
        public string OperationId => operationId;

        public ReadOnlyMemory<byte> Args => args;

        public IGrainFactory GrainFactory => grainFactory;

        public CancellationToken CancellationToken => CancellationToken.None;
    }
}
