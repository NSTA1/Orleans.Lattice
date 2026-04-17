using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class SplitStateTests
{
    [Test]
    public void Merge_returns_maximum()
    {
        Assert.That(SplitState.Unsplit.Merge(SplitState.SplitInProgress), Is.EqualTo(SplitState.SplitInProgress));
        Assert.That(SplitState.SplitInProgress.Merge(SplitState.SplitComplete), Is.EqualTo(SplitState.SplitComplete));
        Assert.That(SplitState.Unsplit.Merge(SplitState.SplitComplete), Is.EqualTo(SplitState.SplitComplete));
    }

    [Test]
    public void Merge_is_commutative()
    {
        Assert.That(
            SplitState.SplitComplete.Merge(SplitState.Unsplit),
            Is.EqualTo(SplitState.Unsplit.Merge(SplitState.SplitComplete)));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        Assert.That(SplitState.SplitInProgress.Merge(SplitState.SplitInProgress), Is.EqualTo(SplitState.SplitInProgress));
    }

    [Test]
    public void Merge_never_goes_backwards()
    {
        Assert.That(SplitState.SplitComplete.Merge(SplitState.Unsplit), Is.EqualTo(SplitState.SplitComplete));
        Assert.That(SplitState.SplitComplete.Merge(SplitState.SplitInProgress), Is.EqualTo(SplitState.SplitComplete));
    }

    [Test]
    public void TryAdvanceTo_succeeds_when_target_is_higher()
    {
        Assert.That(SplitState.Unsplit.TryAdvanceTo(SplitState.SplitInProgress, out var result), Is.True);
        Assert.That(result, Is.EqualTo(SplitState.SplitInProgress));
    }

    [Test]
    public void TryAdvanceTo_fails_when_target_is_same_or_lower()
    {
        Assert.That(SplitState.SplitComplete.TryAdvanceTo(SplitState.Unsplit, out var result), Is.False);
        Assert.That(result, Is.EqualTo(SplitState.SplitComplete));

        Assert.That(SplitState.SplitInProgress.TryAdvanceTo(SplitState.SplitInProgress, out result), Is.False);
        Assert.That(result, Is.EqualTo(SplitState.SplitInProgress));
    }
}
