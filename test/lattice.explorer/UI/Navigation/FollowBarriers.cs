namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The two barriers every follower and poller fixture needs: wait until a read
/// count reaches a figure, and wait until the next wait is armed before moving the
/// manual clock past it.
/// </summary>
/// <remarks>
/// <para>
/// A follower arms its next wait on the continuation of its last read, which runs
/// on whichever thread completed that read rather than on the test's. Advancing
/// the clock before that wait is armed moves time past a timer that does not yet
/// exist, so the tick is lost and the fixture then waits the whole barrier timeout
/// for a read that was never scheduled. <see cref="Advance"/> closes that window.
/// </para>
/// <para>
/// Every barrier fails reporting what it waited for <em>and what it last saw</em>,
/// so a timeout reads as the condition that did not hold rather than as a bare
/// "Expected: True, But was: False". The message is built only on failure.
/// </para>
/// </remarks>
internal static class FollowBarriers
{
    /// <summary>How long a barrier waits for its condition before it fails.</summary>
    /// <remarks>
    /// Generous on purpose: it is only ever paid in full by a failing run, and a
    /// loaded CI agent must not turn a slow continuation into a red test.
    /// </remarks>
    internal static readonly TimeSpan BarrierTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Waits until <paramref name="reads"/> reports exactly <paramref name="expected"/>.</summary>
    /// <param name="reads">Reads the count, which another thread advances.</param>
    /// <param name="expected">The count to wait for.</param>
    /// <param name="subject">What is doing the reading, for the failure message.</param>
    internal static void ReadsReach(Func<int> reads, int expected, string subject = "the follower") =>
        Assert.That(
            SpinWait.SpinUntil(() => reads() == expected, BarrierTimeout),
            Is.True,
            () => $"{subject} reads {expected} time(s), but it has read {reads()} time(s)");

    /// <summary>Waits until the next wait is armed, then moves the clock by <paramref name="delta"/>.</summary>
    /// <param name="time">The manual clock.</param>
    /// <param name="delta">How far to move it once the wait is armed.</param>
    /// <param name="subject">What is doing the arming, for the failure message.</param>
    internal static void Advance(ManualTimeProvider time, TimeSpan delta, string subject = "the follower")
    {
        ArmsNextWait(time, subject);
        time.Advance(delta);
    }

    /// <summary>Waits until exactly one wait is armed on <paramref name="time"/>.</summary>
    /// <param name="time">The manual clock.</param>
    /// <param name="subject">What is doing the arming, for the failure message.</param>
    internal static void ArmsNextWait(ManualTimeProvider time, string subject = "the follower") =>
        Assert.That(
            SpinWait.SpinUntil(() => time.ArmedTimers == 1, BarrierTimeout),
            Is.True,
            () => $"{subject} re-arms, but {time.ArmedTimers} timer(s) are armed");

    /// <summary>Waits until <paramref name="condition"/> holds, failing with <paramref name="expectation"/> if it never does.</summary>
    /// <param name="condition">The condition to wait for.</param>
    /// <param name="expectation">What the caller was waiting for, phrased as the expectation.</param>
    internal static void Reaches(Func<bool> condition, string expectation) =>
        Assert.That(SpinWait.SpinUntil(condition, BarrierTimeout), Is.True, expectation);
}
