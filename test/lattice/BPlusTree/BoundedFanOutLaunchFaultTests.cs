using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for <see cref="BoundedFanOut"/>'s launch-fault drain - the arm that
/// runs when the per-slot <c>body</c> throws <b>synchronously</b> partway through
/// the launch loop instead of returning a faulted task.
/// <para>
/// The loop has already handed out real tasks for the earlier slots at that
/// point. Abandoning them would leave any that fault unobserved, so the drain
/// awaits every launched slot - swallowing their faults deliberately, because the
/// synchronous launch fault is the one the caller must see - and only then
/// re-throws the original. These tests pin both halves of that contract: the
/// original fault is what surfaces, and no launched slot is left unobserved.
/// </para>
/// </summary>
[TestFixture]
public class BoundedFanOutLaunchFaultTests
{
    /// <summary>
    /// Forces the un-gated path (bound >= count) so the launch loop is entered
    /// directly rather than through the semaphore wrapper.
    /// </summary>
    private const int UnboundedConcurrency = 64;

    [Test]
    public void RunAsync_with_results_rethrows_a_synchronous_launch_fault()
    {
        var started = 0;

        Assert.That(
            async () => await BoundedFanOut.RunAsync(
                count: 4,
                maxConcurrency: UnboundedConcurrency,
                body: slot =>
                {
                    if (slot == 2)
                    {
                        throw new InvalidOperationException("launch boom");
                    }

                    Interlocked.Increment(ref started);
                    return Task.FromResult(slot);
                }),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("launch boom"),
            "the synchronous launch fault is the one the caller must see");

        Assert.That(started, Is.EqualTo(2), "only the slots before the throwing one launch");
    }

    [Test]
    public void RunAsync_with_results_observes_already_launched_faults_without_masking_the_launch_fault()
    {
        // Slot 0 returns a faulted task; slot 1 then throws synchronously. The
        // drain must observe slot 0's fault (so it is not unobserved) yet still
        // surface slot 1's, which is the fault that stopped the launch.
        var slotZero = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        slotZero.SetException(new TimeoutException("slot zero boom"));

        Assert.That(
            async () => await BoundedFanOut.RunAsync(
                count: 3,
                maxConcurrency: UnboundedConcurrency,
                body: slot => slot == 0
                    ? slotZero.Task
                    : throw new InvalidOperationException("launch boom")),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("launch boom"),
            "an already-launched slot's own fault must not mask the launch fault");

        Assert.That(slotZero.Task.IsFaulted, Is.True);
        Assert.DoesNotThrow(() => _ = slotZero.Task.Exception,
            "the drain must have observed the launched slot's fault");
    }

    [Test]
    public void RunAsync_result_free_rethrows_a_synchronous_launch_fault()
    {
        var started = 0;

        Assert.That(
            async () => await BoundedFanOut.RunAsync(
                count: 4,
                maxConcurrency: UnboundedConcurrency,
                body: slot =>
                {
                    if (slot == 3)
                    {
                        throw new InvalidOperationException("launch boom");
                    }

                    Interlocked.Increment(ref started);
                    return Task.CompletedTask;
                }),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("launch boom"));

        Assert.That(started, Is.EqualTo(3));
    }

    [Test]
    public void RunAsync_result_free_observes_already_launched_faults_during_the_drain()
    {
        var slotZero = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        slotZero.SetException(new TimeoutException("slot zero boom"));

        Assert.That(
            async () => await BoundedFanOut.RunAsync(
                count: 3,
                maxConcurrency: UnboundedConcurrency,
                body: slot => slot == 0
                    ? slotZero.Task
                    : throw new InvalidOperationException("launch boom")),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("launch boom"));

        Assert.DoesNotThrow(() => _ = slotZero.Task.Exception,
            "the drain must have observed the launched slot's fault");
    }

    [Test]
    public void RunAsync_gated_path_surfaces_a_body_fault_after_every_slot_settles()
    {
        // Below-count concurrency takes the semaphore-gated path. There the body
        // runs inside an async wrapper, so a throw becomes a faulted task rather
        // than a synchronous launch fault: every other slot still runs, and the
        // fault surfaces once the whole batch has quiesced. That quiescence is the
        // property the gated path exists to give a caller's catch.
        var started = 0;

        Assert.That(
            async () => await BoundedFanOut.RunAsync(
                count: 6,
                maxConcurrency: 2,
                body: slot =>
                {
                    if (slot == 4)
                    {
                        throw new InvalidOperationException("gated body boom");
                    }

                    Interlocked.Increment(ref started);
                    return Task.CompletedTask;
                }),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("gated body boom"));

        Assert.That(started, Is.EqualTo(5),
            "every non-faulting slot must still have run before the fault surfaced");
    }
}
