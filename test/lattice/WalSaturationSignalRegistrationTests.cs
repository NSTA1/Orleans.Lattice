using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

[TestFixture]
public sealed class WalSaturationSignalRegistrationTests
{
    [TestCase(true)]
    [TestCase(false)]
    public void Completion_before_registration_installation_does_not_retain_the_callback(bool recover)
    {
        using var source = new CancellationTokenSource();
        var callbacks = 0;
        var entry = new WalSaturationSignal.WaiterEntry
        {
            Tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
        };

        // The entry is already linked; completion can beat Register's return.
        if (recover)
            entry.Tcs.TrySetResult();
        else
            entry.Tcs.TrySetCanceled();
        entry.DisposeRegistration();
        entry.SetRegistration(source.Token.Register(() => callbacks++));

        source.Cancel();
        Assert.That(callbacks, Is.Zero, "a completed wait must not retain a callback on its caller's token");
    }

    [Test]
    public void Registration_installed_before_completion_is_disposed()
    {
        using var source = new CancellationTokenSource();
        var callbacks = 0;
        var entry = new WalSaturationSignal.WaiterEntry { Tcs = new TaskCompletionSource() };
        entry.SetRegistration(source.Token.Register(() => callbacks++));
        entry.DisposeRegistration();
        source.Cancel();
        Assert.That(callbacks, Is.Zero);
    }

    [Test]
    public void Cancellation_during_registration_can_complete_without_deadlock()
    {
        using var source = new CancellationTokenSource();
        source.Cancel();
        var entry = new WalSaturationSignal.WaiterEntry { Tcs = new TaskCompletionSource() };
        entry.SetRegistration(source.Token.Register(() =>
        {
            entry.Tcs.TrySetCanceled();
            entry.DisposeRegistration();
        }));
        Assert.That(entry.Tcs.Task.IsCanceled, Is.True);
    }
}
