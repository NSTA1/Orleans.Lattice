namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The runtime premise under #5992's G1 (issue #6006): <see cref="SemaphoreSlim"/>
/// hands a released permit straight to the earliest parked waiter instead of
/// raising <see cref="SemaphoreSlim.CurrentCount"/>, so a positive count is a
/// permit nobody is queued for. The replay gate relies on it and does not
/// implement it; these tests pin the behaviour of the runtime it ships on.
/// Refinement: the gap bullet on direct hand-off in
/// <c>spec/scheduling/ReplayPermitsRefinement.md</c>.
/// </summary>
[TestFixture]
public sealed class SemaphoreSlimHandOffPremiseTests
{
    private static readonly TimeSpan Observation = TimeSpan.FromMilliseconds(500);

    [Test]
    public async Task A_release_hands_the_permit_to_a_parked_async_waiter_and_a_zero_timeout_probe_is_refused()
    {
        using var semaphore = new SemaphoreSlim(1, 1);
        Assert.That(semaphore.Wait(0), Is.True, "precondition: the only permit is held");
        var parked = semaphore.WaitAsync();
        Assert.That(parked.IsCompleted, Is.False, "precondition: the waiter is parked");

        semaphore.Release();
        var probe = semaphore.Wait(0);
        var count = semaphore.CurrentCount;
        var granted = await Task.WhenAny(parked, Task.Delay(Observation)) == parked;

        Assert.Multiple(() =>
        {
            Assert.That(probe, Is.False, "a zero-timeout probe just after the release must not take the waiter's permit");
            Assert.That(count, Is.Zero, "the release must not raise the free count while a waiter is parked");
            Assert.That(granted, Is.True, "the parked waiter must be granted the released permit");
        });

        if (probe)
            semaphore.Release();
        if (granted)
            semaphore.Release();
    }

    [Test]
    public async Task A_later_blocking_wait_is_not_served_ahead_of_a_parked_async_waiter()
    {
        using var semaphore = new SemaphoreSlim(1, 1);
        Assert.That(semaphore.Wait(0), Is.True, "precondition: the only permit is held");
        var parked = semaphore.WaitAsync();
        var blocking = Task.Run(() => semaphore.Wait(TimeSpan.FromSeconds(3)));
        await Task.WhenAny(blocking, Task.Delay(Observation));
        Assert.That(blocking.IsCompleted, Is.False, "precondition: the later blocking wait is still waiting");

        semaphore.Release();
        var granted = await Task.WhenAny(parked, Task.Delay(Observation)) == parked;
        var blockingServedEarly = blocking.IsCompleted && blocking.Result;

        Assert.Multiple(() =>
        {
            Assert.That(granted, Is.True, "the earlier parked async waiter must be granted the released permit");
            Assert.That(blockingServedEarly, Is.False, "a later blocking wait must not be served ahead of it");
        });

        if (granted)
            semaphore.Release();
        if (await blocking)
            semaphore.Release();
    }
}
