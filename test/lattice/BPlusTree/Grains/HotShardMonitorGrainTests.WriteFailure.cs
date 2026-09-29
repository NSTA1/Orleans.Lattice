using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class HotShardMonitorGrainTests
{
    /// <summary>
    /// Regression for Class B "persisted/in-memory divergence on failing
    /// <c>WriteStateAsync</c>" in
    /// <c>HotShardMonitorGrain.GetOrSetActivationUtcAsync</c>. The method
    /// assigns <c>state.State.ActivationUtc = nowUtc</c> before
    /// <c>await state.WriteStateAsync()</c>. If the write throws, the
    /// in-memory <c>ActivationUtc</c> is left non-null while disk stays
    /// at its prior value, and the guard
    /// <c>if (state.State.ActivationUtc is DateTime v) return v;</c>
    /// short-circuits every subsequent call from the same activation -
    /// so disk never receives the activation timestamp and the
    /// <see cref="LatticeOptions.AutoSplitMinTreeAge"/> grace clock
    /// effectively restarts on every cluster restart.
    /// <para>
    /// Uses the lifecycle harness, which wires a timer registry: the sampling
    /// timer is armed before the activation-time write (#3713), so without one
    /// the call would fault at the timer and never reach the write under test.
    /// </para>
    /// </summary>
    [Test]
    public void EnsureRunningAsync_reverts_ActivationUtc_when_WriteStateAsync_throws()
    {
        var sharedState = new FakePersistentState<HotShardMonitorState>();
        var h = CreateLifecycleGrain(
            options: new LatticeOptions
            {
                AutoSplitEnabled = true,
                AutoSplitMinTreeAge = TimeSpan.FromMinutes(5),
                HotShardOpsPerSecondThreshold = 100,
                MaxConcurrentAutoSplits = 1,
            },
            state: sharedState);

        sharedState.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.EnsureRunningAsync());

        Assert.That(sharedState.State.ActivationUtc, Is.Null,
            "ActivationUtc must remain null in-memory when WriteStateAsync throws, otherwise the " +
            "idempotency guard short-circuits every retry from this activation and disk stays stale.");
    }
}
