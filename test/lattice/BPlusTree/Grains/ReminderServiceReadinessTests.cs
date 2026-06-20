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
}
