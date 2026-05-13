using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

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
    /// </summary>
    [Test]
    public void EnsureRunningAsync_reverts_ActivationUtc_when_WriteStateAsync_throws()
    {
        var sharedState = new FakePersistentState<HotShardMonitorState>();
        var opts = new LatticeOptions
        {
            AutoSplitEnabled = true,
            AutoSplitMinTreeAge = TimeSpan.FromMinutes(5),
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };

        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("monitor", TreeId));
        var gf = Substitute.For<IGrainFactory>();
        var om = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        om.Get(Arg.Any<string>()).Returns(opts);
        var resolver = TestOptionsResolver.ForFactory(gf, opts);

        var grain = new HotShardMonitorGrain(
            ctx, gf, Substitute.For<IReminderRegistry>(), om, resolver,
            new LoggerFactory().CreateLogger<HotShardMonitorGrain>(), sharedState);

        sharedState.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.EnsureRunningAsync());

        Assert.That(sharedState.State.ActivationUtc, Is.Null,
            "ActivationUtc must remain null in-memory when WriteStateAsync throws, otherwise the " +
            "idempotency guard short-circuits every retry from this activation and disk stays stale.");
    }
}