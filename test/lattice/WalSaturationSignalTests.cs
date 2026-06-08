using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalSaturationSignal"/> covering the
/// polling getters (<see cref="IWalSaturationSignal.GetCurrentState"/>
/// / <see cref="IWalSaturationSignal.GetAggregateState"/>), the
/// await-able gate (<see cref="IWalSaturationSignal.WaitForHealthyAsync"/>),
/// and the sampler-side state-update path.
/// </summary>
[TestFixture]
public class WalSaturationSignalTests
{
    [Test]
    public void GetCurrentState_throws_on_null_treeId()
    {
        var signal = new WalSaturationSignal();
        Assert.That(() => signal.GetCurrentState(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void GetCurrentState_returns_Healthy_for_unobserved_tree()
    {
        var signal = new WalSaturationSignal();
        Assert.That(signal.GetCurrentState("unseen-tree"), Is.EqualTo(WalSaturationState.Healthy));
    }

    [Test]
    public void GetCurrentState_reflects_most_recent_UpdateState()
    {
        var signal = new WalSaturationSignal();
        signal.UpdateState("tree-A", WalSaturationState.Throttled);
        Assert.That(signal.GetCurrentState("tree-A"), Is.EqualTo(WalSaturationState.Throttled));

        signal.UpdateState("tree-A", WalSaturationState.Saturated);
        Assert.That(signal.GetCurrentState("tree-A"), Is.EqualTo(WalSaturationState.Saturated));
    }

    [Test]
    public void GetAggregateState_returns_Healthy_when_no_trees_observed()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        Assert.That(signal.GetAggregateState(), Is.EqualTo(WalSaturationState.Healthy));
    }

    [Test]
    public void GetAggregateState_returns_worst_case_across_observed_trees()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("a", WalSaturationState.Healthy);
        signal.UpdateState("b", WalSaturationState.Throttled);
        signal.UpdateState("c", WalSaturationState.Healthy);
        Assert.That(signal.GetAggregateState(), Is.EqualTo(WalSaturationState.Throttled));

        signal.UpdateState("d", WalSaturationState.Saturated);
        Assert.That(signal.GetAggregateState(), Is.EqualTo(WalSaturationState.Saturated));
    }

    [Test]
    public void UpdateState_returns_previous_state_for_attribution()
    {
        var signal = new WalSaturationSignal();
        var first = signal.UpdateState("tree-prev", WalSaturationState.Throttled);
        Assert.That(first, Is.EqualTo(WalSaturationState.Healthy),
            "first observation must report the implicit Healthy baseline as previous");

        var second = signal.UpdateState("tree-prev", WalSaturationState.Saturated);
        Assert.That(second, Is.EqualTo(WalSaturationState.Throttled),
            "subsequent UpdateState must report the prior state for transition attribution");

        var third = signal.UpdateState("tree-prev", WalSaturationState.Saturated);
        Assert.That(third, Is.EqualTo(WalSaturationState.Saturated),
            "a no-op update must report the same state as previous so callers can short-circuit");
    }

    [Test]
    public void WaitForHealthyAsync_throws_on_null_treeId()
    {
        var signal = new WalSaturationSignal();
        Assert.That(() => signal.WaitForHealthyAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void WaitForHealthyAsync_completes_synchronously_when_already_Healthy()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        var task = signal.WaitForHealthyAsync("never-touched");
        Assert.That(task.IsCompletedSuccessfully, Is.True,
            "an already-Healthy tree must short-circuit to a completed task with no allocation");
    }

    [Test]
    public async Task WaitForHealthyAsync_completes_when_state_transitions_back_to_Healthy()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-wait", WalSaturationState.Saturated);

        var wait = signal.WaitForHealthyAsync("tree-wait");
        Assert.That(wait.IsCompleted, Is.False, "must not complete while tree is Saturated");

        signal.UpdateState("tree-wait", WalSaturationState.Healthy);

        // The TCS is RunContinuationsAsynchronously so await it.
        await wait.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(wait.IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task WaitForHealthyAsync_throws_OperationCanceledException_when_cancelled()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-cancel", WalSaturationState.Saturated);

        using var cts = new CancellationTokenSource();
        var wait = signal.WaitForHealthyAsync("tree-cancel", cts.Token);

        cts.Cancel();
        Assert.That(async () => await wait, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void WaitForHealthyAsync_with_pre_cancelled_token_throws_immediately()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-pre-cancel", WalSaturationState.Saturated);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            () => signal.WaitForHealthyAsync("tree-pre-cancel", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task WaitForHealthyAsync_multiple_waiters_all_complete_on_single_recovery()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-many", WalSaturationState.Throttled);

        var waits = new[]
        {
            signal.WaitForHealthyAsync("tree-many"),
            signal.WaitForHealthyAsync("tree-many"),
            signal.WaitForHealthyAsync("tree-many"),
        };

        signal.UpdateState("tree-many", WalSaturationState.Healthy);

        await Task.WhenAll(waits).WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(waits.All(w => w.IsCompletedSuccessfully), Is.True);
    }

    [Test]
    public void WaitForHealthyAsync_for_different_trees_is_independent()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-A", WalSaturationState.Throttled);
        signal.UpdateState("tree-B", WalSaturationState.Throttled);

        var waitA = signal.WaitForHealthyAsync("tree-A");
        var waitB = signal.WaitForHealthyAsync("tree-B");

        signal.UpdateState("tree-A", WalSaturationState.Healthy);

        // A completes; B still parked.
        Assert.That(waitA.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(waitB.IsCompleted, Is.False,
            "recovering tree A must not complete a wait registered against tree B");
    }
}
