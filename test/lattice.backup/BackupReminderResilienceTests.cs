using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Pins the classification and bounded-retry contract of
/// <see cref="BackupReminderResilience"/>. The backup scheduler grain's reminder
/// operations (register / read / unregister) are absorbed through this envelope
/// so a transient reminder-registry failure - the reminder service still
/// initializing, or a reminder-table read/write timing out under CI load - does
/// not surface to <c>ScheduleRecurringAsync</c> / <c>CancelScheduleAsync</c>
/// callers as an opaque server fault (the flake observed as
/// <c>CancelScheduleAsync_removes_a_recurring_schedule_over_the_wire</c>). A
/// genuine, unrelated fault must NOT be classified as transient, so it still
/// propagates on the first attempt.
/// </summary>
[TestFixture]
public sealed class BackupReminderResilienceTests
{
    private const string RealStillInitializingMessage =
        "Reminder Service is still initializing and it is taking a long time. Please retry again later.";

    private static readonly TimeSpan[] InstantBackoff = [TimeSpan.Zero, TimeSpan.Zero];

    // ---- Classification -----------------------------------------------------

    [Test]
    public void Still_initializing_condition_is_transient()
    {
        var ex = new InvalidOperationException(RealStillInitializingMessage);

        Assert.That(BackupReminderResilience.IsTransientReminderFailure(ex), Is.True);
    }

    [Test]
    public void Marker_remains_a_substring_of_the_real_orleans_message()
    {
        Assert.That(
            RealStillInitializingMessage.Contains(
                ReminderServiceReadiness.StillInitializingMarker, StringComparison.Ordinal),
            Is.True);
    }

    [Test]
    public void Bare_timeout_is_transient_for_reminder_operations()
    {
        // Unlike the general user-write bootstrap path, a reminder-table read/write
        // that times out under load is infrastructure-transient for the scheduler's
        // reminder operations, so a bare TimeoutException is retried here.
        var ex = new TimeoutException("The operation has timed out.");

        Assert.That(BackupReminderResilience.IsTransientReminderFailure(ex), Is.True);
    }

    [Test]
    public void Transient_wrapped_in_an_inner_chain_is_detected()
    {
        var ex = new InvalidOperationException(
            "register reminder failed",
            new InvalidOperationException(
                RealStillInitializingMessage,
                new TimeoutException("The operation has timed out.")));

        Assert.That(BackupReminderResilience.IsTransientReminderFailure(ex), Is.True);
    }

    [Test]
    public void Unrelated_exception_is_not_transient()
    {
        var ex = new InvalidOperationException("scope was deleted");

        Assert.That(BackupReminderResilience.IsTransientReminderFailure(ex), Is.False);
    }

    [Test]
    public void Null_exception_is_not_transient()
    {
        Assert.That(BackupReminderResilience.IsTransientReminderFailure(null), Is.False);
    }

    // ---- Retry (result-less) ------------------------------------------------

    [Test]
    public async Task RunWithRetry_invokes_operation_once_when_first_attempt_succeeds()
    {
        var calls = 0;
        await BackupReminderResilience.RunWithRetryAsync(
            () => { calls++; return Task.CompletedTask; },
            InstantBackoff, NullLogger.Instance, "op", "reminder", "scope");

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public async Task RunWithRetry_retries_through_transient_then_succeeds()
    {
        var calls = 0;
        await BackupReminderResilience.RunWithRetryAsync(
            () =>
            {
                calls++;
                if (calls == 1) throw new TimeoutException("The operation has timed out.");
                return Task.CompletedTask;
            },
            InstantBackoff, NullLogger.Instance, "op", "reminder", "scope");

        Assert.That(calls, Is.EqualTo(2),
            "The envelope did not retry the transient reminder-registry failure.");
    }

    [Test]
    public void RunWithRetry_rethrows_last_transient_when_budget_exhausted()
    {
        var calls = 0;
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await BackupReminderResilience.RunWithRetryAsync(
                () =>
                {
                    calls++;
                    throw new InvalidOperationException($"{RealStillInitializingMessage} attempt-{calls}");
                },
                InstantBackoff, NullLogger.Instance, "op", "reminder", "scope"));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(InstantBackoff.Length + 1),
                "The envelope did not exhaust the full retry budget before propagating.");
            Assert.That(ex!.Message, Does.EndWith($"attempt-{InstantBackoff.Length + 1}"),
                "The envelope rethrew the wrong attempt's exception (must be the last, not the first).");
        });
    }

    [Test]
    public void RunWithRetry_propagates_unrelated_exception_without_retry()
    {
        var calls = 0;
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await BackupReminderResilience.RunWithRetryAsync(
                () => { calls++; throw new InvalidOperationException("scope was deleted"); },
                InstantBackoff, NullLogger.Instance, "op", "reminder", "scope"));

        Assert.That(calls, Is.EqualTo(1),
            "The envelope must not retry on exceptions other than transient reminder-registry failures.");
    }

    [Test]
    public void RunWithRetry_throws_for_null_operation()
    {
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await BackupReminderResilience.RunWithRetryAsync(
                (Func<Task>)null!, InstantBackoff, NullLogger.Instance, "op", "reminder", "scope"));
    }

    // ---- Retry (result-bearing) ---------------------------------------------

    [Test]
    public async Task RunWithRetry_result_returns_value_after_transient_retry()
    {
        var calls = 0;
        var result = await BackupReminderResilience.RunWithRetryAsync(
            () =>
            {
                calls++;
                if (calls == 1) throw new TimeoutException("The operation has timed out.");
                return Task.FromResult("done");
            },
            InstantBackoff, NullLogger.Instance, "op", "reminder", "scope");

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(result, Is.EqualTo("done"));
        });
    }

    [Test]
    public void RunWithRetry_default_backoff_yields_four_attempts()
    {
        // Pins the production retry budget so a change to the default backoff is a
        // deliberate, reviewed edit rather than an accidental regression.
        Assert.That(BackupReminderResilience.DefaultReminderRetryBackoff.Count + 1, Is.EqualTo(4));
    }
}
