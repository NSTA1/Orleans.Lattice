using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="SagaCopyBinding"/> and
/// <see cref="SagaCopyBindingVerdict"/>, the pure core that holds an atomic-write
/// saga's batch on the physical copy it commits on (#4358, #4369).
/// <c>LatticeGrainTests.AtomicBinding</c> and <c>AtomicWriteGrainTests.AtomicBinding</c>
/// prove the grains consult it, and <c>SagaCopyBindingModel</c> proves the property
/// it guarantees; these pin the core's own contract.
/// </summary>
[TestFixture]
public sealed class SagaCopyBindingTests
{
    private const string Old = "tree";
    private const string Resized = "tree/resized/op";

    [Test]
    public void AdmitsDispatch_admits_any_copy_for_an_unbound_dispatch([Values(Old, Resized)] string resolved)
    {
        Assert.That(SagaCopyBinding.AdmitsDispatch(null, resolved), Is.True);
    }

    [Test]
    public void AdmitsDispatch_admits_only_the_bound_copy_for_a_bound_dispatch()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SagaCopyBinding.AdmitsDispatch(Old, Old), Is.True);
            Assert.That(SagaCopyBinding.AdmitsDispatch(Old, Resized), Is.False);
        });
    }

    [Test]
    public void BeforeDecision_commits_when_the_tree_still_resolves_to_the_bound_copy(
        [Values(null, Old, Resized)] string? mirror)
    {
        // The mirror answer is irrelevant while the tree has not moved; production
        // does not even ask for it then.
        Assert.That(SagaCopyBinding.BeforeDecision(Old, Old, mirror), Is.EqualTo(SagaCopyBindingVerdict.Commit));
    }

    [Test]
    public void BeforeDecision_stays_bound_when_the_bound_copy_mirrors_into_the_new_one()
    {
        Assert.That(
            SagaCopyBinding.BeforeDecision(Old, Resized, boundMirrorDestination: Resized),
            Is.EqualTo(SagaCopyBindingVerdict.StayBound));
    }

    [Test]
    public void BeforeDecision_rebinds_when_the_bound_copy_mirrors_nowhere_or_elsewhere(
        [Values(null, "tree/resized/other")] string? mirror)
    {
        Assert.That(SagaCopyBinding.BeforeDecision(Old, Resized, mirror), Is.EqualTo(SagaCopyBindingVerdict.Rebind));
    }

    [Test]
    public void RebindsAfterRefusal_rebinds_only_on_a_refusal_naming_the_bound_copy()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SagaCopyBinding.RebindsAfterRefusal(Old, Old), Is.True);
            Assert.That(SagaCopyBinding.RebindsAfterRefusal(Old, Resized), Is.False);
            Assert.That(SagaCopyBinding.RebindsAfterRefusal(Old, null), Is.False);
            Assert.That(SagaCopyBinding.RebindsAfterRefusal(null, Old), Is.False);
        });
    }

    [Test]
    public void DispatchCopy_places_an_unbound_or_bound_to_resolved_dispatch_on_the_resolved_copy(
        [Values(null, Resized)] string? mirror)
    {
        Assert.Multiple(() =>
        {
            Assert.That(SagaCopyBinding.DispatchCopy(null, Resized, mirror), Is.EqualTo(Resized));
            Assert.That(SagaCopyBinding.DispatchCopy(Resized, Resized, mirror), Is.EqualTo(Resized));
        });
    }

    [Test]
    public void DispatchCopy_places_a_bound_dispatch_on_the_bound_copy_when_it_mirrors_into_the_resolved_one()
    {
        // #4454: the batch belongs on the resize source, which takes it through its
        // fence and mirrors it, not on the destination the tree flipped to.
        Assert.That(SagaCopyBinding.DispatchCopy(Old, Resized, boundMirrorDestination: Resized), Is.EqualTo(Old));
    }

    [Test]
    public void DispatchCopy_refuses_a_bound_dispatch_when_the_bound_copy_mirrors_nowhere_or_elsewhere(
        [Values(null, "tree/resized/other")] string? mirror)
    {
        Assert.That(SagaCopyBinding.DispatchCopy(Old, Resized, mirror), Is.Null);
    }

    [Test]
    public void AfterRefusal_stays_bound_when_the_bound_copy_mirrors_into_the_new_one_and_rebinds_otherwise()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SagaCopyBinding.AfterRefusal(Old, Resized, Resized), Is.EqualTo(SagaCopyBindingVerdict.StayBound));
            Assert.That(SagaCopyBinding.AfterRefusal(Old, Resized, null), Is.EqualTo(SagaCopyBindingVerdict.Rebind));
            Assert.That(SagaCopyBinding.AfterRefusal(Old, Old, null), Is.EqualTo(SagaCopyBindingVerdict.Commit));
        });
    }
}
