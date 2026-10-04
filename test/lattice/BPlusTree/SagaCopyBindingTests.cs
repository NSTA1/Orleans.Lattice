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
}
