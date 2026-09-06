using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// The one shared bounded-poll barrier for tests that must wait on an
/// observation made by a background worker (a timer callback, a fire-and-forget
/// write-through, a drain loop) rather than by the awaited call itself.
/// </summary>
/// <remarks>
/// <para>
/// Before this type each fixture carried its own private <c>WaitUntilAsync</c>,
/// and the copies had drifted into two shapes with materially different failure
/// behaviour: one returned a <see cref="bool"/> the caller was expected to
/// assert on, and one returned <see cref="Task"/> and simply fell through when
/// the deadline elapsed. The second shape is a silent-failure generator - a
/// caller that samples a baseline immediately after the wait samples it from a
/// state the barrier never actually reached, so the real failure surfaces later
/// (or not at all) and never names the condition that timed out.
/// </para>
/// <para>
/// <see cref="UntilAsync(Func{bool}, string, TimeSpan?, TimeSpan?)"/> is
/// therefore the default: it fails the test at the point the barrier gave up,
/// quoting the caller's own description of what it was waiting for.
/// <see cref="TryUntilAsync(Func{bool}, TimeSpan?, TimeSpan?)"/> keeps the
/// boolean shape for the negative assertions that legitimately expect the
/// condition never to hold.
/// </para>
/// <para>
/// Both re-evaluate the condition once after the deadline elapses, so a
/// condition that became true while the final delay was in flight is still
/// observed and the barrier cannot fail purely because a scheduling hiccup
/// straddled the deadline.
/// </para>
/// </remarks>
public static class TestPoll
{
    /// <summary>The deadline used when a caller does not supply one.</summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The sampling interval used when a caller does not supply one.</summary>
    public static readonly TimeSpan DefaultCadence = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds or the deadline
    /// elapses, and returns whether it was ever observed to hold. Use this only
    /// for a negative assertion, where the caller asserts on the returned value;
    /// a positive wait should use
    /// <see cref="UntilAsync(Func{bool}, string, TimeSpan?, TimeSpan?)"/> so a
    /// timeout fails where it happened.
    /// </summary>
    /// <param name="condition">The observation to poll for.</param>
    /// <param name="timeout">The deadline; defaults to <see cref="DefaultTimeout"/>.</param>
    /// <param name="cadence">The sampling interval; defaults to <see cref="DefaultCadence"/>.</param>
    public static async Task<bool> TryUntilAsync(
        Func<bool> condition,
        TimeSpan? timeout = null,
        TimeSpan? cadence = null)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var deadline = Environment.TickCount64 + (long)(timeout ?? DefaultTimeout).TotalMilliseconds;
        var delay = cadence ?? DefaultCadence;

        while (Environment.TickCount64 < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(delay);
        }

        // One final sample: the condition may have become true while the last
        // delay was in flight.
        return condition();
    }

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds, failing the test with
    /// <paramref name="because"/> if the deadline elapses first, so a barrier
    /// that never opened is reported at the barrier rather than as a confusing
    /// downstream assertion against a state the test never reached.
    /// </summary>
    /// <param name="condition">The observation to wait for.</param>
    /// <param name="because">What the caller is waiting for, quoted in the failure.</param>
    /// <param name="timeout">The deadline; defaults to <see cref="DefaultTimeout"/>.</param>
    /// <param name="cadence">The sampling interval; defaults to <see cref="DefaultCadence"/>.</param>
    public static async Task UntilAsync(
        Func<bool> condition,
        string because,
        TimeSpan? timeout = null,
        TimeSpan? cadence = null)
    {
        var effectiveTimeout = timeout ?? DefaultTimeout;
        if (await TryUntilAsync(condition, effectiveTimeout, cadence))
        {
            return;
        }

        Assert.Fail(
            $"Timed out after {effectiveTimeout.TotalMilliseconds:0}ms waiting for: {because}");
    }

    /// <summary>
    /// Asynchronous-probe form of
    /// <see cref="TryUntilAsync(Func{bool}, TimeSpan?, TimeSpan?)"/>, for a
    /// condition that can only be observed by awaiting something (a grain call,
    /// a store read). Use it only for a negative assertion, where the caller
    /// asserts on the returned value.
    /// </summary>
    /// <param name="condition">The observation to poll for.</param>
    /// <param name="timeout">The deadline; defaults to <see cref="DefaultTimeout"/>.</param>
    /// <param name="cadence">The sampling interval; defaults to <see cref="DefaultCadence"/>.</param>
    public static async Task<bool> TryUntilAsync(
        Func<Task<bool>> condition,
        TimeSpan? timeout = null,
        TimeSpan? cadence = null)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var deadline = Environment.TickCount64 + (long)(timeout ?? DefaultTimeout).TotalMilliseconds;
        var delay = cadence ?? DefaultCadence;

        while (Environment.TickCount64 < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(delay);
        }

        // One final sample, for the same reason as the synchronous form: the
        // condition may have become true while the last delay was in flight.
        return await condition();
    }

    /// <summary>
    /// Asynchronous-probe form of
    /// <see cref="UntilAsync(Func{bool}, string, TimeSpan?, TimeSpan?)"/>, for a
    /// condition that can only be observed by awaiting something. Fails at the
    /// barrier when the deadline elapses, so a caller that samples a value
    /// immediately afterwards can never sample it from a state the barrier
    /// never reached.
    /// </summary>
    /// <param name="condition">The observation to wait for.</param>
    /// <param name="because">What the caller is waiting for, quoted in the failure.</param>
    /// <param name="timeout">The deadline; defaults to <see cref="DefaultTimeout"/>.</param>
    /// <param name="cadence">The sampling interval; defaults to <see cref="DefaultCadence"/>.</param>
    public static async Task UntilAsync(
        Func<Task<bool>> condition,
        string because,
        TimeSpan? timeout = null,
        TimeSpan? cadence = null)
    {
        var effectiveTimeout = timeout ?? DefaultTimeout;
        if (await TryUntilAsync(condition, effectiveTimeout, cadence))
        {
            return;
        }

        Assert.Fail(
            $"Timed out after {effectiveTimeout.TotalMilliseconds:0}ms waiting for: {because}");
    }
}
