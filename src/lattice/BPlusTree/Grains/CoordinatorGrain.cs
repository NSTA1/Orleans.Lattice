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

    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>Reminder-registry handle used by derived classes.</summary>
    protected IReminderRegistry ReminderRegistry => reminderRegistry;

    /// <summary>Typed logger for derived classes.</summary>
    protected ILogger<TSelf> Logger => logger;

    /// <summary>Grain context — exposes the <see cref="GrainId"/> used for reminder registration.</summary>
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
    /// Exceptions are logged by the base class and do not stop the timer.
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
    /// Registers the keepalive reminder and starts the phase-processing
    /// grain timer. Derived classes call this after persisting intent.
    /// </summary>
    protected async Task StartCoordinatorAsync()
    {
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: KeepaliveReminderPeriod,
            period: KeepaliveReminderPeriod);
        StartPhaseTimer();
    }

    /// <summary>
    /// Starts the phase-processing grain timer without (re-)registering the
    /// keepalive reminder. Called by the base-class reminder handler on
    /// silo reactivation.
    /// </summary>
    protected void StartPhaseTimer()
    {
        _phaseTimer ??= this.RegisterGrainTimer(
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
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Coordinator {ReminderName} phase tick failed for {Context}",
                KeepaliveReminderName, LogContext);
        }
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
