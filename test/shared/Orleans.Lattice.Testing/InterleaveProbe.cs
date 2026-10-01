using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// The one shared, deterministic proof that a grain call answers while a turn
/// the test holds is still held - the "an interleaved read answers while the
/// phase holds the coordinator" claim, made without timing it.
/// </summary>
/// <remarks>
/// <para>
/// The shape it replaces asserted the read answered within a few seconds. On a
/// loaded runner a correctly interleaved read can miss a few seconds, and that
/// failure is indistinguishable from the defect the test exists to catch, a read
/// that queued behind the held turn (issue #4142).
/// </para>
/// <para>
/// The claim does not need a clock. The test parks a turn on a
/// <see cref="TaskCompletionSource"/> and releases it only after this probe
/// returns, so an interleaved call always answers while the hold is in force,
/// and a call that queued behind the held turn can never answer at all: a grain
/// call gives up at its runtime response timeout, which the probe reports as the
/// regression. The race is between the call and the hold's release, and the test
/// decides when the release happens. <see cref="HangBound"/> only stops a call
/// with no timeout of its own from stalling the run; it is far above anything a
/// correct call needs on a loaded runner, and below CI's ten-minute hang blame, so
/// the regression fails here with a message that names it rather than aborting
/// the test host.
/// </para>
/// </remarks>
public static class InterleaveProbe
{
    /// <summary>
    /// How long a call may stay unanswered before the probe calls it a hang. Not
    /// the claim under test: see the type remarks.
    /// </summary>
    public static readonly TimeSpan HangBound = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Waits for <paramref name="call"/> to answer while the hold whose release
    /// completes <paramref name="released"/> is still in force, and returns its
    /// result. Fails the test if the hold was released first, or if the call
    /// never answers.
    /// </summary>
    /// <typeparam name="T">The call's result type.</typeparam>
    /// <param name="call">The call issued while the turn was held.</param>
    /// <param name="released">
    /// Completes when the hold ends - the test releases it, or the held turn gives
    /// up by itself. The test must not release it until this probe returns.
    /// </param>
    /// <param name="what">What the call is, quoted in a failure.</param>
    /// <param name="hangBound">The hang bound; defaults to <see cref="HangBound"/>.</param>
    /// <returns>The call's result.</returns>
    public static async Task<T> AnswersWhileHeldAsync<T>(
        Task<T> call, Task released, string what, TimeSpan? hangBound = null)
    {
        await AwaitAnswerAsync(call, released, what, hangBound);
        return await call;
    }

    /// <summary>
    /// Waits for <paramref name="call"/> to answer while the hold whose release
    /// completes <paramref name="released"/> is still in force. Fails the test if
    /// the hold was released first, or if the call never answers.
    /// </summary>
    /// <param name="call">The call issued while the turn was held.</param>
    /// <param name="released">
    /// Completes when the hold ends - the test releases it, or the held turn gives
    /// up by itself. The test must not release it until this probe returns.
    /// </param>
    /// <param name="what">What the call is, quoted in a failure.</param>
    /// <param name="hangBound">The hang bound; defaults to <see cref="HangBound"/>.</param>
    public static async Task AnswersWhileHeldAsync(
        Task call, Task released, string what, TimeSpan? hangBound = null)
    {
        await AwaitAnswerAsync(call, released, what, hangBound);
        await call;
    }

    private static async Task AwaitAnswerAsync(Task call, Task released, string what, TimeSpan? hangBound)
    {
        ArgumentNullException.ThrowIfNull(call);
        ArgumentNullException.ThrowIfNull(released);
        ArgumentNullException.ThrowIfNull(what);

        if (released.IsCompleted)
        {
            Assert.Fail($"precondition: the hold was already released when {what} was probed, so whether it "
                + "answers proves nothing about interleaving.");
        }

        var bound = hangBound ?? HangBound;
        using var stop = new CancellationTokenSource();
        var hang = Task.Delay(bound, stop.Token);
        var first = await Task.WhenAny(call, released, hang);
        stop.Cancel();

        // The verdict is which finished first, never whether the call is complete
        // by now: a call queued behind a turn that gave up on its own answers a
        // moment after the hold ends, and would read as complete here.
        if (first == call)
        {
            // A grain call queued behind the held turn faults with the runtime's
            // response timeout, usually before the hang bound, and that is the
            // regression this probe exists to name.
            if (call.IsFaulted && call.Exception!.InnerException is TimeoutException timeout)
            {
                Assert.Fail($"{what} did not answer while the hold was in force: it timed out waiting, which is "
                    + "what a call queued behind the held turn does. Timeout: " + Truncate(timeout.Message));
            }

            return;
        }

        if (first == released)
        {
            Assert.Fail($"{what} did not answer before the hold ended (the test released it, or the held turn gave "
                + "up on its own). A call queued behind a held turn answers only once the turn ends, so this is that "
                + "regression - unless the test released the hold before the probe returned, which proves nothing.");
        }

        Assert.Fail($"{what} did not answer while the hold was in force. The hold is released only after the "
            + "call answers, so an interleaved call always answers and one queued behind the held turn never "
            + $"does: it queued behind the turn it must not wait on. ({bound.TotalSeconds:0} s is a hang guard, "
            + "not the claim.)");
    }

    private static string Truncate(string message) =>
        message.Length <= 400 ? message : message[..400] + "...";
}
