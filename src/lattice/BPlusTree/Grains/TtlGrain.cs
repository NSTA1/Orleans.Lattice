using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Abstract base for transient Lattice grains that clean themselves up
/// after an inactivity or retention TTL. Encapsulates the Orleans
/// reminder register / slide / unregister / dispatch boilerplate so
/// concrete grains only supply a reminder name, a per-activation TTL
/// resolver, and a cleanup action.
/// <para>
/// Two usage patterns are supported:
/// </para>
/// <list type="bullet">
///   <item>
///     <description><b>Sliding idle-TTL</b> (e.g. cursor grains): call
///     <see cref="SlideTtlAsync"/> after every successful operation so
///     an active grain never times out.</description>
///   </item>
///   <item>
///     <description><b>One-shot retention</b> (e.g. atomic-write saga
///     grains): call <see cref="SlideTtlAsync"/> once on entry to the
///     terminal state to arm a deferred self-cleanup.</description>
///   </item>
/// </list>
/// <para>
/// Independent configuration is preserved: each concrete grain reads its
/// own <see cref="LatticeOptions"/> property inside its <see cref="ResolveTtl"/>
/// override.
/// </para>
/// <para>
/// <b>Performance.</b> Every <see cref="SlideTtlAsync"/> call issues a
/// <c>RegisterOrUpdateReminder</c> round-trip to the reminder table
/// (typically a few milliseconds). For chatty workloads (e.g. thousands
/// of cursor pages), override <see cref="SlideDebounce"/> to throttle
/// slides to at most one per interval. The default (<see cref="TimeSpan.Zero"/>)
/// refreshes on every call, which matches the pre-refactor behaviour.
/// </para>
/// </summary>
internal abstract class TtlGrain<TSelf>(
    IGrainContext grainContext,
    IReminderRegistry reminderRegistry,
    ILogger<TSelf> logger) : IRemindable, IGrainBase
    where TSelf : TtlGrain<TSelf>
{
    private DateTime _lastSlideAtUtc = DateTime.MinValue;

    IGrainContext IGrainBase.GrainContext => grainContext;

    /// <summary>
    /// Protected accessor for the grain context, exposed so derived classes
    /// can avoid capturing the primary-constructor parameter (CS9107).
    /// </summary>
    protected IGrainContext GrainContext => grainContext;

    /// <summary>
    /// Protected accessor for the reminder registry, exposed so derived
    /// classes can issue additional reminders (e.g. keepalive) without
    /// re-capturing the primary-constructor parameter (CS9107).
    /// </summary>
    protected IReminderRegistry ReminderRegistry => reminderRegistry;

    /// <summary>
    /// Protected accessor for the logger, exposed so derived classes can
    /// log without capturing their own <c>logger</c> primary-constructor
    /// parameter (CS9107).
    /// </summary>
    protected ILogger<TSelf> Logger => logger;

    /// <summary>
    /// The name used to register this grain's TTL reminder. Must be
    /// stable across activations; a rename breaks pending reminders.
    /// </summary>
    protected abstract string TtlReminderName { get; }

    /// <summary>
    /// Resolves the current TTL for this activation. Called every
    /// <see cref="SlideTtlAsync"/> so per-tree option changes are
    /// honoured without a restart. Return
    /// <see cref="Timeout.InfiniteTimeSpan"/> to disable cleanup.
    /// </summary>
    protected abstract TimeSpan ResolveTtl();

    /// <summary>
    /// Minimum effective reminder interval. Orleans reminder granularity
    /// is 1 minute; values below this floor are clamped. Override only
    /// for tests that need sub-minute dispatch.
    /// </summary>
    protected virtual TimeSpan MinimumTtl => TimeSpan.FromMinutes(1);

    /// <summary>
    /// Minimum interval between consecutive <see cref="SlideTtlAsync"/>
    /// reminder-table writes. Defaults to <see cref="TimeSpan.Zero"/>
    /// (no debounce) so every call refreshes the reminder. Override to
    /// trade a small bounded staleness window for reduced reminder-table
    /// load on chatty grains.
    /// </summary>
    protected virtual TimeSpan SlideDebounce => TimeSpan.Zero;

    /// <summary>
    /// Cleanup action executed when the TTL reminder fires. Implementations
    /// typically clear persisted state. Exceptions are caught and logged;
    /// the grain still unregisters the reminder and deactivates.
    /// </summary>
    protected abstract Task OnTtlExpiredAsync();

    /// <summary>
    /// Dispatch hook for reminders other than <see cref="TtlReminderName"/>.
    /// Default is a no-op. Override when the grain multiplexes additional
    /// reminders (e.g. a keepalive tick) through <see cref="IRemindable"/>.
    /// </summary>
    protected virtual Task OnOtherReminderAsync(string reminderName, TickStatus status) =>
        Task.CompletedTask;

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == TtlReminderName)
        {
            logger.LogInformation(
                "Grain {GrainId}: TTL reminder '{Reminder}' fired; running cleanup.",
                grainContext.GrainId, TtlReminderName);

            try
            {
                await OnTtlExpiredAsync();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Grain {GrainId}: TTL cleanup for reminder '{Reminder}' failed.",
                    grainContext.GrainId, TtlReminderName);
            }

            await UnregisterTtlAsync();
            this.DeactivateOnIdle();
            return;
        }

        await OnOtherReminderAsync(reminderName, status);
    }

    /// <summary>
    /// Registers or refreshes the TTL reminder. Honors
    /// <see cref="Timeout.InfiniteTimeSpan"/> (skips registration) and
    /// clamps values below <see cref="MinimumTtl"/>. When
    /// <see cref="SlideDebounce"/> is non-zero, consecutive slides inside
    /// the debounce window are silently skipped. Exceptions are swallowed
    /// and logged — reminder-service hiccups must never fail user
    /// operations.
    /// </summary>
    protected async Task SlideTtlAsync()
    {
        var ttl = ResolveTtl();
        if (ttl == Timeout.InfiniteTimeSpan) return;
        if (ttl < MinimumTtl) ttl = MinimumTtl;

        var debounce = SlideDebounce;
        if (debounce > TimeSpan.Zero
            && _lastSlideAtUtc != DateTime.MinValue
            && DateTime.UtcNow - _lastSlideAtUtc < debounce)
        {
            return;
        }

        try
        {
            await reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: grainContext.GrainId,
                reminderName: TtlReminderName,
                dueTime: ttl,
                period: ttl);
            _lastSlideAtUtc = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Grain {GrainId}: failed to register TTL reminder '{Reminder}' (non-fatal).",
                grainContext.GrainId, TtlReminderName);
        }
    }

    /// <summary>
    /// Unregisters the TTL reminder if present. Idempotent and non-throwing;
    /// safe to call during close / deactivation paths.
    /// </summary>
    protected async Task UnregisterTtlAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(
                grainContext.GrainId, TtlReminderName);
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(
                    grainContext.GrainId, reminder);
            }
            _lastSlideAtUtc = DateTime.MinValue;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Grain {GrainId}: failed to unregister TTL reminder '{Reminder}' (non-fatal).",
                grainContext.GrainId, TtlReminderName);
        }
    }
}
