using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Base class for reminder-anchored, work-pump coordinator grains.
/// <para>
/// Factors out the repeated 1-minute keepalive reminder + 2-second phase
/// grain-timer + <c>DeactivateOnIdle</c>-on-completion pattern used by
/// <see cref="TreeSnapshotGrain"/>, <see cref="TreeResizeGrain"/>,
/// <see cref="TreeShardSplitGrain"/>, and <see cref="TreeReshardGrain"/>.
/// </para>
/// <para>
/// Derived classes:
/// </para>
/// <list type="number">
/// <item><description>Supply a unique <see cref="KeepaliveReminderName"/>.</description></item>
/// <item><description>Expose <see cref="InProgress"/> reading their persisted
/// coordinator state.</description></item>
/// <item><description>Implement <see cref="ProcessNextPhaseAsync"/> as the
/// per-tick phase-machine hook.</description></item>
/// <item><description>Call <see cref="StartCoordinatorAsync"/> once intent
/// has been persisted to begin processing.</description></item>
/// <item><description>Call <see cref="CompleteCoordinatorAsync"/> after
/// persisting the terminal state transition.</description></item>
/// </list>
/// <para>
/// The keepalive reminder guarantees the grain reactivates after a silo
/// restart while <see cref="InProgress"/> is <c>true</c>; on reactivation
/// the reminder handler re-arms the phase timer. When <see cref="InProgress"/>
/// becomes <c>false</c> the reminder handler unregisters itself and
/// deactivates the grain.
/// </para>
/// </summary>
internal abstract class CoordinatorGrain<TSelf>(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILogger<TSelf> logger)
    : IRemindable, IGrainBase
    where TSelf : CoordinatorGrain<TSelf>
{
    private IGrainTimer? _phaseTimer;

    /// <summary>
    /// Consecutive phase ticks whose step threw, reset by the first tick that
    /// returns normally. Drives the log-severity escalation only; the counter
    /// records every failure regardless.
    /// </summary>
    private int _consecutiveTickFailures;

    /// <summary>
    /// Consecutive swallowed ticks after which the warning escalates to an
    /// error. One swallowed tick is a transient the pump absorbs and retries;
    /// a run of them is a phase loop that has stopped advancing, which nothing
    /// else in the system reports.
    /// </summary>
    private const int PhaseTickFailureEscalationThreshold = 3;

    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// Hook invoked on graceful deactivation. Default is a no-op;
    /// derived classes override to flush deferred state, release
    /// pooled resources, etc. Crash deactivations bypass this hook
    /// by design - derived classes must remain crash-safe without
    /// it firing. A storage failure inside the hook must not block
    /// deactivation; derived classes are responsible for catching
    /// and logging their own exceptions.
    /// </summary>
    /// <param name="reason">Why Orleans is deactivating the grain.</param>
    /// <param name="cancellationToken">Soft deadline for the deactivation hook.</param>
    protected virtual Task OnDeactivateCoreAsync(DeactivationReason reason, CancellationToken cancellationToken)
        => Task.CompletedTask;

    Task IGrainBase.OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
        => OnDeactivateCoreAsync(reason, cancellationToken);

    /// <summary>
    /// Hook invoked when Orleans activates the grain, regardless of what
    /// caused the activation (a client call, an incoming message, or a
    /// reminder-driven reactivation). Default is a no-op: the one-shot
    /// coordinators start their phase timer only once intent has been
    /// persisted via <see cref="StartCoordinatorAsync"/>, so they must not
    /// begin processing merely because something activated them. A
    /// perpetual coordinator (one whose <see cref="InProgress"/> is always
    /// <c>true</c>) overrides this to (re)arm its phase timer here, so
    /// steady-state processing is decoupled from the specific call that
    /// happened to activate it and can never be starved by that call
    /// blocking the activation.
    /// </summary>
    /// <param name="cancellationToken">Soft deadline for the activation hook.</param>
    protected virtual Task OnActivateCoreAsync(CancellationToken cancellationToken)
        => Task.CompletedTask;

    Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
        => OnActivateCoreAsync(cancellationToken);

    /// <summary>Reminder-registry handle used by derived classes.</summary>
    protected IReminderRegistry ReminderRegistry => reminderRegistry;

    /// <summary>Typed logger for derived classes.</summary>
    protected ILogger<TSelf> Logger => logger;

    /// <summary>Grain context - exposes the <see cref="GrainId"/> used for reminder registration.</summary>
    protected IGrainContext Context => context;

    /// <summary>
    /// The keepalive reminder name. Must be unique across coordinator kinds
    /// so reminders from different grains sharing a silo do not collide.
    /// </summary>
    protected abstract string KeepaliveReminderName { get; }

    /// <summary>
    /// Whether persisted state indicates work is outstanding. Read on every
    /// keepalive reminder firing to decide between re-arming the phase
    /// timer and self-destructing.
    /// </summary>
    protected abstract bool InProgress { get; }

    /// <summary>
    /// Work-pump hook invoked on every grain-timer tick while
    /// <see cref="InProgress"/> is <c>true</c>. Implementations should
    /// advance their phase machine by one step per call and return.
    /// Exceptions are logged by the base class, counted on
    /// <see cref="LatticeMetrics.CoordinatorPhaseTickFailures"/>, and do not
    /// stop the timer.
    /// </summary>
    protected internal abstract Task ProcessNextPhaseAsync();

    /// <summary>Period of the phase-processing grain timer. Defaults to 2 seconds.</summary>
    protected virtual TimeSpan PhaseTimerPeriod => TimeSpan.FromSeconds(2);

    /// <summary>
    /// Period of the keepalive reminder. Defaults to 1 minute (the Orleans
    /// reminder minimum).
    /// </summary>
    protected virtual TimeSpan KeepaliveReminderPeriod => TimeSpan.FromMinutes(1);

    /// <summary>
    /// Diagnostic context string embedded in keepalive-unregister and
    /// phase-tick warning logs. Defaults to the grain key.
    /// </summary>
    protected virtual string LogContext => context.GrainId.Key.ToString() ?? "";

    /// <summary>
    /// The subject this coordinator serves, used verbatim as the
    /// <see cref="LatticeMetrics.TagTree"/> tag on
    /// <see cref="LatticeMetrics.CoordinatorPhaseTickFailures"/>. Defaults to the
    /// grain key, which is the tree id verbatim for every coordinator addressed
    /// by tree alone. A coordinator with a composite key (<c>tree/shard</c>, or
    /// <c>repo/space</c>) MUST override this to return the subject alone:
    /// tagging the raw key would emit a distinct series per shard, which no
    /// dashboard can group by tree and which is unbounded in principle.
    /// </summary>
    protected virtual string MetricsTreeId => context.GrainId.Key.ToString() ?? "";

    /// <summary>
    /// The bounded inter-attempt backoff used when the keepalive reminder
    /// registration in <see cref="StartCoordinatorAsync"/> races Orleans'
    /// asynchronous reminder-service startup. Defaults to
    /// <see cref="ReminderServiceReadiness.DefaultRegistrationBackoff"/>; exposed as
    /// an override only so a unit test can drive the retry budget without real
    /// delays, exactly as <see cref="ReminderServiceReadiness"/> exposes its
    /// backoff-injectable core for the same reason.
    /// </summary>
    protected virtual IReadOnlyList<TimeSpan> KeepaliveRegistrationBackoff
        => ReminderServiceReadiness.DefaultRegistrationBackoff;

    /// <summary>
    /// Registers the keepalive reminder and starts the phase-processing
    /// grain timer. Derived classes call this after persisting intent.
    /// </summary>
    protected async Task StartCoordinatorAsync()
    {
        // The keepalive reminder is this coordinator's crash-recovery anchor - a
        // durability guarantee for the in-progress work, not a best-effort
        // first-write bootstrap - so it has no natural re-attempt seam and must not
        // be dropped. Orleans' reminder service initialises asynchronously after the
        // silo reaches Active, so a coordinator started inside that window can see
        // the transient "Reminder Service is still initializing" fault. Wait it out
        // with the same bounded retry the atomic-write saga's essential keepalive
        // uses rather than failing the caller's operation; any other fault, and a
        // transient that never clears within the retry budget, still surfaces with
        // its original shape.
        await ReminderServiceReadiness.RetryWhileInitializingAsync(
            () => reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: context.GrainId,
                reminderName: KeepaliveReminderName,
                dueTime: KeepaliveReminderPeriod,
                period: KeepaliveReminderPeriod),
            KeepaliveRegistrationBackoff);
        StartPhaseTimer();
    }

    /// <summary>
    /// Starts the phase-processing grain timer without (re-)registering the
    /// keepalive reminder. Called by the base-class reminder handler on
    /// silo reactivation.
    /// </summary>
    protected void StartPhaseTimer()
    {
        if (_phaseTimer is not null) return;

        // Zero-prime before the pump can fail. A Counter exports no series at all
        // until its first Add, so without this a coordinator that has never failed
        // is indistinguishable from one whose instrument was never wired - which is
        // precisely the defect this counter exists to fix. Adding zero mints the
        // series with the exact tag set a later failure will carry and cannot
        // perturb the value.
        LatticeMetrics.CoordinatorPhaseTickFailures.Add(0, PhaseTickFailureTags());

        _phaseTimer = this.RegisterGrainTimer(
            OnPhaseTimerTickAsync,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: PhaseTimerPeriod));
    }

    /// <summary>
    /// Disposes the phase-processing timer, unregisters the keepalive
    /// reminder, and marks the grain for deactivation. Derived classes
    /// call this after persisting the terminal state transition.
    /// </summary>
    protected async Task CompleteCoordinatorAsync()
    {
        _phaseTimer?.Dispose();
        _phaseTimer = null;
        await UnregisterKeepaliveAsync();
        this.DeactivateOnIdle();
    }

    /// <summary>
    /// Defensive keepalive-reminder unregister. Swallows exceptions so a
    /// transient storage failure on shutdown does not fault the grain.
    /// </summary>
    protected async Task UnregisterKeepaliveAsync()
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
                "Failed to unregister {ReminderName} for coordinator {Context}",
                KeepaliveReminderName, LogContext);
        }
    }

    private async Task OnPhaseTimerTickAsync(CancellationToken ct)
    {
        try
        {
            await ProcessNextPhaseAsync();
            _consecutiveTickFailures = 0;
        }
        catch (Exception ex)
        {
            // Count BEFORE logging. This tick's work is now discarded, and until
            // this counter existed that discard was invisible to every exported
            // series: on the acceptance rig one coordinator swallowed thirty-five
            // ticks across eight and a half hours while telemetry sat flat at zero.
            // A log line is not a measurement - nothing aggregates it, nothing
            // alerts on it, and nothing can answer "how much work did we throw
            // away" from it.
            LatticeMetrics.CoordinatorPhaseTickFailures.Add(1, PhaseTickFailureTags());

            _consecutiveTickFailures++;
            if (_consecutiveTickFailures >= PhaseTickFailureEscalationThreshold)
            {
                logger.LogError(ex,
                    "Coordinator {ReminderName} phase tick failed for {Context} "
                    + "{ConsecutiveFailures} times in a row; the phase machine has stopped advancing "
                    + "and every tick's work is being discarded.",
                    KeepaliveReminderName, LogContext, _consecutiveTickFailures);
            }
            else
            {
                logger.LogWarning(ex,
                    "Coordinator {ReminderName} phase tick failed for {Context}; this tick's work was discarded "
                    + "and the pump will retry on the next tick.",
                    KeepaliveReminderName, LogContext);
            }
        }
    }

    /// <summary>
    /// The tag set for <see cref="LatticeMetrics.CoordinatorPhaseTickFailures"/>:
    /// the coordinator kind, the tree it serves, and the tenant that tree belongs
    /// to. Built per emission rather than cached because
    /// <see cref="MetricsTreeId"/> is a derived-class hook that may only become
    /// resolvable after activation.
    /// </summary>
    private KeyValuePair<string, object?>[] PhaseTickFailureTags()
    {
        var tree = MetricsTreeId;
        return
        [
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, KeepaliveReminderName),
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, tree),
            LatticeTenantLabel.ForTree(tree),
        ];
    }

    /// <summary>
    /// Handles the keepalive reminder. Re-arms the phase timer when work is
    /// still outstanding on reactivation, or unregisters + deactivates when
    /// no work remains.
    /// </summary>
    public virtual async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;

        if (InProgress)
        {
            if (_phaseTimer is null) StartPhaseTimer();
        }
        else
        {
            await UnregisterKeepaliveAsync();
            this.DeactivateOnIdle();
        }
    }
}
