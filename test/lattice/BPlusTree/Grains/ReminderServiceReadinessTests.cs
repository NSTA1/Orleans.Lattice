using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the classification contract of <see cref="ReminderServiceReadiness"/>.
/// The lazy first-write bootstraps on <c>LatticeGrain</c> (the tombstone-
/// compaction keepalive reminder and the hot-shard monitor) register reminders
/// inline on the user write path. Orleans' reminder service initialises
/// asynchronously after the silo reaches Active, so a write in that startup
/// window throws a transient <c>"Reminder Service is still initializing"</c>
/// (with an inner <see cref="TimeoutException"/>). That transient must be
/// recognised so the bootstrap defers instead of failing the user's write - the
/// flake observed as <c>Bidirectional_writes_converge_under_lww_register</c>
/// timing out on a loaded CI runner. A genuine, unrelated fault must NOT be
/// classified as the transient (so it still propagates).
/// </summary>
[TestFixture]
public class ReminderServiceReadinessTests
{
    /// <summary>
    /// The exact message Orleans' <c>LocalReminderService</c> raises. Pinned here
    /// so a divergence between the real message and the matched marker is caught
    /// by this test rather than by a CI flake.
    /// </summary>
    private const string RealOrleansMessage =
        "Reminder Service is still initializing and it is taking a long time. Please retry again later.";

    [Test]
    public void Real_orleans_still_initializing_message_is_recognised()
    {
        var ex = new InvalidOperationException(RealOrleansMessage);

        Assert.That(ReminderServiceReadiness.IsStillInitializing(ex), Is.True);
    }

    [Test]
    public void Marker_is_a_substring_of_the_real_orleans_message()
    {
        Assert.That(
            RealOrleansMessage.Contains(ReminderServiceReadiness.StillInitializingMarker, StringComparison.Ordinal),
            Is.True,
            "The matched marker must remain a substring of the real Orleans reminder-service message.");
    }

    [Test]
    public void Transient_wrapped_in_an_inner_chain_is_detected()
    {
        // The condition arrives as an Orleans exception carrying the message,
        // with an inner TimeoutException; a caller may further wrap it.
        var ex = new InvalidOperationException(
            "lazy bootstrap failed",
            new InvalidOperationException(
                RealOrleansMessage,
                new TimeoutException("The operation has timed out.")));

        Assert.That(ReminderServiceReadiness.IsStillInitializing(ex), Is.True);
    }

    [Test]
    public void Unrelated_exception_is_not_classified_as_the_transient()
    {
        var ex = new InvalidOperationException("writing to a deleted tree");

        Assert.That(ReminderServiceReadiness.IsStillInitializing(ex), Is.False);
    }

    [Test]
    public void Bare_timeout_without_the_marker_is_not_classified_as_the_transient()
    {
        // A plain TimeoutException with no reminder-service marker must not be
        // swallowed - only the specific "still initializing" condition is.
        var ex = new TimeoutException("The operation has timed out.");

        Assert.That(ReminderServiceReadiness.IsStillInitializing(ex), Is.False);
    }

    [Test]
    public void Null_exception_is_not_classified_as_the_transient()
    {
        Assert.That(ReminderServiceReadiness.IsStillInitializing(null), Is.False);
    }

    // -------------------------------------------------------------------------
    // RetryWhileInitializingAsync - the bounded wait-out retry essential
    // reminders (the atomic-write saga keepalive) use instead of deferring. It
    // retries only the "still initializing" transient, rethrows the last one on
    // budget exhaustion, and propagates any unrelated exception immediately. A
    // zero/short injected backoff keeps these tests instant.
    // -------------------------------------------------------------------------

    private static readonly TimeSpan[] InstantBackoff =
        [TimeSpan.Zero, TimeSpan.Zero];

    [Test]
    public async Task RetryWhileInitializing_invokes_operation_once_when_first_attempt_succeeds()
    {
        var calls = 0;
        await ReminderServiceReadiness.RetryWhileInitializingAsync(
            () => { calls++; return Task.CompletedTask; },
            InstantBackoff);

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public async Task RetryWhileInitializing_retries_through_transient_then_succeeds()
    {
        var calls = 0;
        await ReminderServiceReadiness.RetryWhileInitializingAsync(
            () =>
            {
                calls++;
                if (calls == 1) throw new InvalidOperationException(RealOrleansMessage);
                return Task.CompletedTask;
            },
            InstantBackoff);

        Assert.That(calls, Is.EqualTo(2),
            "Envelope did not retry the transient reminder-service-initializing fault.");
    }

    [Test]
    public void RetryWhileInitializing_rethrows_last_transient_when_budget_exhausted()
    {
        var calls = 0;
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ReminderServiceReadiness.RetryWhileInitializingAsync(
                () =>
                {
                    calls++;
                    throw new InvalidOperationException($"{RealOrleansMessage} attempt-{calls}");
                },
                InstantBackoff));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(InstantBackoff.Length + 1),
                "Envelope did not exhaust the full retry budget before propagating.");
            Assert.That(ex!.Message, Does.EndWith($"attempt-{InstantBackoff.Length + 1}"),
                "Envelope rethrew the wrong attempt's exception (must be the last, not the first).");
        });
    }

    [Test]
    public void RetryWhileInitializing_propagates_unrelated_exception_without_retry()
    {
        var calls = 0;
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ReminderServiceReadiness.RetryWhileInitializingAsync(
                () => { calls++; throw new InvalidOperationException("writing to a deleted tree"); },
                InstantBackoff));

        Assert.That(calls, Is.EqualTo(1),
            "Envelope must not retry on exceptions other than the still-initializing transient.");
    }

    [Test]
    public void RetryWhileInitializing_with_empty_backoff_makes_a_single_attempt()
    {
        var calls = 0;
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ReminderServiceReadiness.RetryWhileInitializingAsync(
                () => { calls++; throw new InvalidOperationException(RealOrleansMessage); },
                Array.Empty<TimeSpan>()));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(1),
                "An empty backoff list must mean exactly one attempt with no retry.");
            Assert.That(ReminderServiceReadiness.IsStillInitializing(ex), Is.True);
        });
    }

    [Test]
    public void RetryWhileInitializing_throws_for_null_operation()
    {
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await ReminderServiceReadiness.RetryWhileInitializingAsync(null!, InstantBackoff));
    }

    [Test]
    public void RetryWhileInitializing_default_backoff_yields_five_attempts()
    {
        // Pins the production retry budget so a change to the default backoff is
        // a deliberate, reviewed edit rather than an accidental regression.
        Assert.That(ReminderServiceReadiness.DefaultRegistrationBackoff.Length + 1, Is.EqualTo(5));
    }
}
