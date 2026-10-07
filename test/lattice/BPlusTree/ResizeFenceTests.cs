using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="ResizeFence"/>, the pure core deciding which calls an
/// online resize's fenced old copy still admits (#4369) and whether a failed alias
/// flip lifts the fence again (#4362). <c>ShardRootGrainShadowForwardTests</c> and
/// <c>TreeResizeGrainTests</c> prove the grains consult it, and
/// <c>ResizeFenceModel</c> proves the property it guarantees; these pin the core's
/// own contract over every input combination.
/// </summary>
[TestFixture]
public sealed class ResizeFenceTests
{
    private const string Old = "tree";
    private const string Resized = "tree/resized/op";

    [Test]
    public void AdmitsBoundSaga_never_admits_on_an_unfenced_shard(
        [Values] bool directTerminal,
        [Values] bool preparedScope)
    {
        // An unfenced shard is served by the ordinary gate; this rule only widens a
        // fence, so it must never claim to admit outside one.
        Assert.That(
            ResizeFence.AdmitsBoundSaga(rejecting: false, directTerminal, preparedScope, Old, Old),
            Is.False);
    }

    [Test]
    public void AdmitsBoundSaga_admits_a_direct_terminal_through_the_fence(
        [Values] bool preparedScope,
        [Values(null, Old, Resized)] string? binding)
    {
        Assert.That(
            ResizeFence.AdmitsBoundSaga(rejecting: true, directTerminal: true, preparedScope, binding, Old),
            Is.True);
    }

    [Test]
    public void AdmitsBoundSaga_admits_a_prepared_batch_bound_to_this_copy()
    {
        Assert.That(
            ResizeFence.AdmitsBoundSaga(rejecting: true, directTerminal: false, preparedScope: true, Old, Old),
            Is.True);
    }

    [Test]
    public void AdmitsBoundSaga_refuses_a_prepared_batch_bound_elsewhere_or_unbound(
        [Values(null, Resized)] string? binding)
    {
        Assert.That(
            ResizeFence.AdmitsBoundSaga(rejecting: true, directTerminal: false, preparedScope: true, binding, Old),
            Is.False);
    }

    [Test]
    public void AdmitsBoundSaga_refuses_a_binding_outside_a_prepared_scope()
    {
        // The binding travels only with a saga's prepared dispatch; on any other
        // call it is not a saga's batch and must not pass the fence.
        Assert.That(
            ResizeFence.AdmitsBoundSaga(rejecting: true, directTerminal: false, preparedScope: false, Old, Old),
            Is.False);
    }

    [Test]
    public void AdmitsBoundSaga_compares_physical_ids_ordinally()
    {
        Assert.That(
            ResizeFence.AdmitsBoundSaga(rejecting: true, directTerminal: false, preparedScope: true, "TREE", "tree"),
            Is.False);
    }

    [Test]
    public void LiftsFenceAfterFailedFlip_keeps_the_fence_when_the_flip_landed()
    {
        Assert.That(ResizeFence.LiftsFenceAfterFailedFlip(Resized, Resized), Is.False);
    }

    [Test]
    public void LiftsFenceAfterFailedFlip_lifts_the_fence_when_the_alias_did_not_move(
        [Values(null, Old)] string? resolved)
    {
        Assert.That(ResizeFence.LiftsFenceAfterFailedFlip(resolved, Resized), Is.True);
    }
}
