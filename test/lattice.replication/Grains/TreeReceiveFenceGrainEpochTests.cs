using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #4593: the receive fence's epoch. Every new pause bumps it, a resume
/// never does, and an observation reports it, so an admitted entry can be stamped
/// with the pause it was admitted after.
/// </summary>
[TestFixture]
public class TreeReceiveFenceGrainEpochTests
{
    private static (TreeReceiveFenceGrain Grain, FakePersistentState<TreeReceiveFenceState> State) CreateGrain()
    {
        var state = new FakePersistentState<TreeReceiveFenceState>();
        return (new TreeReceiveFenceGrain(state, NullLogger<TreeReceiveFenceGrain>.Instance), state);
    }

    [Test]
    public async Task A_fresh_fence_observes_unpaused_at_epoch_zero()
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.ObserveAsync(), Is.EqualTo(new ReceiveFenceObservation { Paused = false, Epoch = 0 }));
    }

    [Test]
    public async Task Each_new_pause_bumps_the_epoch_and_a_resume_keeps_it()
    {
        var (grain, state) = CreateGrain();

        var first = await grain.PauseAsync("saga-1");
        var again = await grain.PauseAsync("saga-1");
        await grain.ResumeAsync("saga-1");
        var afterResume = await grain.ObserveAsync();
        var second = await grain.PauseAsync("saga-2");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(1));
            Assert.That(again, Is.EqualTo(1), "re-pausing under the owning saga is idempotent");
            Assert.That(afterResume, Is.EqualTo(new ReceiveFenceObservation { Paused = false, Epoch = 1 }));
            Assert.That(second, Is.EqualTo(2));
            Assert.That(state.State.Epoch, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_takeover_by_another_saga_is_a_new_pause()
    {
        var (grain, _) = CreateGrain();
        await grain.PauseAsync("saga-1");

        var takeover = await grain.PauseAsync("saga-2");

        Assert.That(takeover, Is.EqualTo(2));
        Assert.That((await grain.ObserveAsync()).Paused, Is.True);
    }

    [Test]
    public async Task A_pause_whose_write_fails_keeps_the_previous_epoch()
    {
        var (grain, state) = CreateGrain();
        await grain.PauseAsync("saga-1");
        await grain.ResumeAsync("saga-1");
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PauseAsync("saga-2"));

        Assert.That(await grain.ObserveAsync(), Is.EqualTo(new ReceiveFenceObservation { Paused = false, Epoch = 1 }));
    }
}
