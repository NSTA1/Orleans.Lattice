using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage for <see cref="TreeReceiveFenceGrain"/> (issue #1173), the
/// durable per-tree inbound-apply gate. Verifies pause / resume, idempotency,
/// and that a superseded saga cannot unpause a tree a newer saga owns, and that a
/// failed persist leaves the in-memory owner as storage holds it so a retry writes
/// again.
/// </summary>
[TestFixture]
public class TreeReceiveFenceGrainTests
{
    private static (TreeReceiveFenceGrain Grain, FakePersistentState<TreeReceiveFenceState> State) CreateGrain()
    {
        var state = new FakePersistentState<TreeReceiveFenceState>();
        var grain = new TreeReceiveFenceGrain(state, NullLogger<TreeReceiveFenceGrain>.Instance);
        return (grain, state);
    }

    [Test]
    public async Task Pause_marks_the_tree_paused_and_persists()
    {
        var (grain, state) = CreateGrain();

        await grain.PauseAsync("saga-1");

        Assert.That(await grain.IsPausedAsync(), Is.True);
        Assert.That(state.State.PauseSagaId, Is.EqualTo("saga-1"));
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task IsPaused_false_on_fresh_grain()
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.IsPausedAsync(), Is.False);
    }

    [Test]
    public async Task Pause_is_idempotent_for_the_same_saga()
    {
        var (grain, state) = CreateGrain();

        await grain.PauseAsync("saga-1");
        await grain.PauseAsync("saga-1");

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Resume_clears_the_pause_for_the_owning_saga()
    {
        var (grain, state) = CreateGrain();
        await grain.PauseAsync("saga-1");

        await grain.ResumeAsync("saga-1");

        Assert.That(await grain.IsPausedAsync(), Is.False);
        Assert.That(state.State.PauseSagaId, Is.Null);
    }

    [Test]
    public async Task Resume_for_a_non_owning_saga_is_a_no_op()
    {
        var (grain, state) = CreateGrain();
        await grain.PauseAsync("saga-1");

        // A superseded saga must not unpause a tree owned by a newer saga.
        await grain.ResumeAsync("saga-2");

        Assert.That(await grain.IsPausedAsync(), Is.True);
        Assert.That(state.State.PauseSagaId, Is.EqualTo("saga-1"));
    }

    [Test]
    public void Pause_rejects_null_or_empty_saga()
    {
        var (grain, _) = CreateGrain();

        Assert.That(() => grain.PauseAsync(null!), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => grain.PauseAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task Pause_failed_persist_leaves_the_tree_unpaused()
    {
        var (grain, state) = CreateGrain();
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(
            async () => await grain.PauseAsync("saga-1"),
            Throws.TypeOf<InvalidOperationException>());
        var paused = await grain.IsPausedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PauseSagaId, Is.Null);
            Assert.That(paused, Is.False);
        });
    }

    [Test]
    public async Task Pause_retry_after_failed_persist_writes_again()
    {
        // The same-saga idempotency guard short-circuits on the in-memory owner,
        // so a failed persist that left it assigned turned the saga's retry into a
        // no-op and the pause never reached storage: the next activation reads the
        // tree as unpaused while the saga still believes it is fenced.
        var (grain, state) = CreateGrain();
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(
            async () => await grain.PauseAsync("saga-1"),
            Throws.TypeOf<InvalidOperationException>());

        await grain.PauseAsync("saga-1");

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.PauseSagaId, Is.EqualTo("saga-1"));
        });
    }

    [Test]
    public async Task Pause_failed_persist_restores_the_previous_owner()
    {
        var (grain, state) = CreateGrain();
        await grain.PauseAsync("saga-1");
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(
            async () => await grain.PauseAsync("saga-2"),
            Throws.TypeOf<InvalidOperationException>());

        Assert.That(state.State.PauseSagaId, Is.EqualTo("saga-1"));
    }

    [Test]
    public async Task Resume_failed_persist_keeps_the_tree_paused()
    {
        var (grain, state) = CreateGrain();
        await grain.PauseAsync("saga-1");
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(
            async () => await grain.ResumeAsync("saga-1"),
            Throws.TypeOf<InvalidOperationException>());
        var paused = await grain.IsPausedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PauseSagaId, Is.EqualTo("saga-1"));
            Assert.That(paused, Is.True);
        });
    }

    [Test]
    public async Task Resume_retry_after_failed_persist_writes_again()
    {
        // The owner check fails once the in-memory owner is cleared, so a failed
        // persist turned the saga's retry into a no-op: storage kept the pause,
        // and once the saga finished no owner remained that could ever lift it.
        var (grain, state) = CreateGrain();
        await grain.PauseAsync("saga-1");
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(
            async () => await grain.ResumeAsync("saga-1"),
            Throws.TypeOf<InvalidOperationException>());

        await grain.ResumeAsync("saga-1");

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(2));
            Assert.That(state.State.PauseSagaId, Is.Null);
        });
    }
}
