using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 1965: <c>BPlusLeafGrain.OnDeactivateAsync</c>
/// performs storage I/O, and two of its three flushes took no cancellation
/// token, so they could not be interrupted by Orleans' deactivation deadline.
/// <para>
/// The failure mode is subtle. The hook already wraps its whole body in a bare
/// <c>catch</c>, so it looks defended - but an untokened flush means the grain
/// never <em>returns</em>, and Orleans reports the overrun as a
/// <c>TaskCanceledException</c> raised in its own frame
/// (<c>ActivationData.FinishDeactivating</c>) that the grain's own catch can
/// never see. At the end of WAL replay several thousand leaves go idle at once
/// and are collected together, so they contend for the same storage provider,
/// each flush slows, and the overruns arrive as a burst of error-level noise.
/// </para>
/// <para>
/// These tests therefore assert on the property that actually matters:
/// deactivation <b>returns promptly</b> when the deadline has fired, rather
/// than running its flushes to completion.
/// </para>
/// </summary>
[TestFixture]
public class BPlusLeafGrainDeactivationDeadlineTests
{
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateGrain(
        IGrainFactory? factory = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("bplusleaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "deactivation-tree";

        factory ??= Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 1, factory: factory);

        var grain = new BPlusLeafGrain(
            context, state, factory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state);
    }

    /// <summary>
    /// The core property: an already-cancelled deactivation token must not
    /// leave the hook running. Before the fix the untokened flushes ran to
    /// completion regardless, which is exactly what overran Orleans' deadline.
    /// </summary>
    [Test]
    public async Task Deactivation_returns_promptly_when_the_deadline_has_already_fired()
    {
        var (grain, _) = CreateGrain();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        var deactivate = ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            cancelled.Token);

        var finished = await Task.WhenAny(deactivate, Task.Delay(TimeSpan.FromSeconds(5)));

        Assert.That(finished, Is.SameAs(deactivate),
            "deactivation must observe the deadline and return, not run its storage "
            + "flushes to completion - overrunning is what Orleans reports as a "
            + "TaskCanceledException from its own frame");
    }

    /// <summary>
    /// Cancellation must be swallowed, not surfaced. The hook's existing
    /// contract is that a storage failure on shutdown never blocks
    /// deactivation; a deadline cancellation is just another such failure.
    /// </summary>
    [Test]
    public void Deactivation_does_not_throw_when_the_deadline_fires()
    {
        var (grain, _) = CreateGrain();
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(async () => await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
                cancelled.Token),
            Throws.Nothing,
            "a cancelled deactivation must complete quietly; rethrowing would "
            + "reintroduce the error-level log line this fix removes");
    }

    /// <summary>
    /// The uncancelled path must be unaffected - the fix is about honouring a
    /// deadline that has fired, not about skipping work when there is time to
    /// do it.
    /// </summary>
    [Test]
    public void Deactivation_still_completes_normally_without_a_deadline()
    {
        var (grain, _) = CreateGrain();

        Assert.That(async () => await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
                CancellationToken.None),
            Throws.Nothing);
    }
}
