using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Classifies transient reminder-registry failures and offers a bounded,
/// backoff retry for the backup scheduler's reminder operations
/// (register / read / unregister).
/// <para>
/// Reminder-table registration is a well-known source of transient failures and
/// timeouts under load: Orleans' <c>LocalReminderService</c> initialises
/// asynchronously after the silo reaches <c>Active</c> (surfacing the transient
/// <c>"Reminder Service is still initializing"</c> condition, matched by the
/// shared <see cref="ReminderServiceReadiness.StillInitializingMarker"/>), and a
/// reminder-table read or write that races that window - or that a loaded runner
/// slows past its deadline - surfaces a <see cref="TimeoutException"/>. Left
/// unhandled, either propagates out of <c>ScheduleRecurringAsync</c> /
/// <c>CancelScheduleAsync</c> to the caller as an opaque server fault. Absorbing
/// the transient with a small bounded retry removes that flake at its source and
/// hardens production scheduling; a genuinely stuck reminder service still
/// surfaces its original exception once the budget is exhausted.
/// </para>
/// </summary>
internal static class BackupReminderResilience
{
    /// <summary>
    /// Default inter-attempt backoff for the reminder-operation retry. One entry
    /// per retry, so the attempt count is <c>Length + 1</c> (here, four total
    /// attempts). The delays are short (~600 ms cumulative) so the retry stays
    /// well inside the Orleans response deadline while riding out a brief
    /// reminder-table contention spike.
    /// </summary>
    internal static readonly IReadOnlyList<TimeSpan> DefaultReminderRetryBackoff =
    [
        TimeSpan.FromMilliseconds(50),
        TimeSpan.FromMilliseconds(150),
        TimeSpan.FromMilliseconds(400),
    ];

    /// <summary>
    /// True when <paramref name="ex"/> is, or wraps anywhere in its inner chain,
    /// a transient reminder-registry failure: either the shared
    /// <see cref="ReminderServiceReadiness.StillInitializingMarker"/> "still
    /// initializing" condition, or a <see cref="TimeoutException"/> from a
    /// reminder-table read / write that timed out under load. Both are
    /// infrastructure-transient for the scheduler's reminder operations (whose
    /// inputs are validated before the call), so both are safe to retry - and,
    /// once a retry budget is exhausted, safe to surface as a retryable status
    /// rather than an opaque internal fault.
    /// </summary>
    internal static bool IsTransientReminderFailure(Exception? ex)
    {
        for (var cur = ex; cur is not null; cur = cur.InnerException)
        {
            if (cur is TimeoutException)
            {
                return true;
            }

            if (cur.Message is { Length: > 0 } message
                && message.Contains(ReminderServiceReadiness.StillInitializingMarker, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Invokes a reminder operation with no result and, when it fails with a
    /// transient reminder-registry failure, waits the corresponding
    /// <see cref="DefaultReminderRetryBackoff"/> delay and retries. Any other
    /// exception propagates immediately without consuming a retry slot; when
    /// every attempt sees the transient, the most-recent exception is rethrown so
    /// a genuinely stuck reminder service still surfaces with its original shape.
    /// </summary>
    internal static Task RunWithRetryAsync(
        Func<Task> operation,
        ILogger logger,
        string operationName,
        string reminderName,
        string scopeKey,
        CancellationToken cancellationToken = default)
        => RunWithRetryAsync(
            operation, DefaultReminderRetryBackoff, logger, operationName, reminderName, scopeKey, cancellationToken);

    /// <summary>
    /// Backoff-injectable core of the result-less
    /// <see cref="RunWithRetryAsync(Func{Task}, ILogger, string, string, string, CancellationToken)"/>.
    /// Exposed with an explicit backoff so unit tests can drive the retry budget
    /// without real delays.
    /// </summary>
    internal static async Task RunWithRetryAsync(
        Func<Task> operation,
        IReadOnlyList<TimeSpan> backoffBetweenAttempts,
        ILogger logger,
        string operationName,
        string reminderName,
        string scopeKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        await RunWithRetryAsync<object?>(
            async () =>
            {
                await operation().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
                return null;
            },
            backoffBetweenAttempts,
            logger,
            operationName,
            reminderName,
            scopeKey,
            cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
    }

    /// <summary>
    /// Invokes a reminder operation that yields a result and, when it fails with
    /// a transient reminder-registry failure, waits the corresponding
    /// <see cref="DefaultReminderRetryBackoff"/> delay and retries, with the same
    /// budget-exhaustion and non-transient-propagation contract as the result-less
    /// overload.
    /// </summary>
    internal static Task<TResult> RunWithRetryAsync<TResult>(
        Func<Task<TResult>> operation,
        ILogger logger,
        string operationName,
        string reminderName,
        string scopeKey,
        CancellationToken cancellationToken = default)
        => RunWithRetryAsync(
            operation, DefaultReminderRetryBackoff, logger, operationName, reminderName, scopeKey, cancellationToken);

    /// <summary>
    /// Backoff-injectable core of the result-bearing
    /// <see cref="RunWithRetryAsync{TResult}(Func{Task{TResult}}, ILogger, string, string, string, CancellationToken)"/>.
    /// The total attempt count is <c>backoffBetweenAttempts.Count + 1</c>; an
    /// empty list means a single attempt with no retry.
    /// </summary>
    internal static async Task<TResult> RunWithRetryAsync<TResult>(
        Func<Task<TResult>> operation,
        IReadOnlyList<TimeSpan> backoffBetweenAttempts,
        ILogger logger,
        string operationName,
        string reminderName,
        string scopeKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(backoffBetweenAttempts);
        ArgumentNullException.ThrowIfNull(logger);

        var maxAttempts = backoffBetweenAttempts.Count + 1;
        Exception? last = null;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                return await operation().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
            catch (Exception ex) when (IsTransientReminderFailure(ex))
            {
                last = ex;
                if (attempt == maxAttempts)
                {
                    break;
                }

                var delay = backoffBetweenAttempts[attempt - 1];
                logger.LogWarning(
                    ex,
                    "Backup schedule reminder operation {Operation} for reminder {Reminder} on scope {Scope} hit a "
                    + "transient reminder-registry failure on attempt {Attempt}/{MaxAttempts}; retrying after {Delay}.",
                    operationName, reminderName, scopeKey, attempt, maxAttempts, delay);
                await Task.Delay(delay, cancellationToken)
                    .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
        }

        // Budget exhausted with every attempt seeing the transient: rethrow the
        // most-recent one so a genuinely stuck reminder service surfaces with the
        // same shape it would have without the retry envelope.
        throw last!;
    }
}
