using VehicleFleetSimulator.AzureThroughput.Silo;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins the warm-up retry-classification contract of
/// <see cref="WarmUpRetryClassifier.IsTransientActivationCancellation"/>:
/// the silo's proactive shard warm-up loop must treat an activation-timeout
/// cancellation (a bare <see cref="TaskCanceledException"/> raised when a
/// shard root's grain-state read exceeds the activation deadline against a
/// transiently-throttled storage account) as a retriable, self-healing
/// failure rather than letting it escape the <c>BackgroundService</c> and
/// StopHost the silo - the spurious-WEDGE host-death of issue #821. Genuine
/// non-cancellation faults must NOT be classified as transient cancellations
/// (the caller still aborts on those); the shutdown-vs-transient distinction
/// is the caller's responsibility (a stopping-token guard), so this predicate
/// classifies any <see cref="OperationCanceledException"/> in the chain as a
/// cancellation regardless of origin.
/// </summary>
[TestFixture]
public class WarmUpRetryClassifierTests
{
    [Test]
    public void Bare_task_canceled_exception_is_a_transient_activation_cancellation()
    {
        // The exact shape Orleans surfaces out of ActivationData.ActivateAsync
        // when a warm-up probe's activation read times out.
        var ex = new TaskCanceledException("A task was canceled.");

        Assert.That(WarmUpRetryClassifier.IsTransientActivationCancellation(ex), Is.True);
    }

    [Test]
    public void Bare_operation_canceled_exception_is_a_transient_activation_cancellation()
    {
        var ex = new OperationCanceledException("The operation was canceled.");

        Assert.That(WarmUpRetryClassifier.IsTransientActivationCancellation(ex), Is.True);
    }

    [Test]
    public void Cancellation_wrapped_in_an_inner_chain_is_detected()
    {
        // Some grain-call faults arrive wrapped (e.g. an aggregate or an
        // Orleans transport exception carrying the cancellation as inner).
        var ex = new InvalidOperationException(
            "warm-up probe failed",
            new ApplicationException(
                "shard activation faulted",
                new TaskCanceledException("A task was canceled.")));

        Assert.That(WarmUpRetryClassifier.IsTransientActivationCancellation(ex), Is.True);
    }

    [Test]
    public void Non_cancellation_exception_is_not_a_transient_activation_cancellation()
    {
        var ex = new InvalidOperationException("cold tree - warm-up genuinely failed");

        Assert.That(WarmUpRetryClassifier.IsTransientActivationCancellation(ex), Is.False);
    }

    [Test]
    public void Non_cancellation_inner_chain_is_not_misclassified()
    {
        var ex = new InvalidOperationException(
            "outer",
            new TimeoutException("inner, but not a cancellation"));

        Assert.That(WarmUpRetryClassifier.IsTransientActivationCancellation(ex), Is.False);
    }

    [Test]
    public void Null_exception_is_not_a_transient_activation_cancellation()
    {
        Assert.That(WarmUpRetryClassifier.IsTransientActivationCancellation(null), Is.False);
    }
}
