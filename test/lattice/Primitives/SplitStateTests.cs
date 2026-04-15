using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class SplitStateTests
{
    [Fact]
    public void Merge_returns_maximum()
    {
        Assert.Equal(SplitState.SplitInProgress, SplitState.Unsplit.Merge(SplitState.SplitInProgress));
        Assert.Equal(SplitState.SplitComplete, SplitState.SplitInProgress.Merge(SplitState.SplitComplete));
        Assert.Equal(SplitState.SplitComplete, SplitState.Unsplit.Merge(SplitState.SplitComplete));
    }

    [Fact]
    public void Merge_is_commutative()
    {
        Assert.Equal(
            SplitState.Unsplit.Merge(SplitState.SplitComplete),
            SplitState.SplitComplete.Merge(SplitState.Unsplit));
    }

    [Fact]
    public void Merge_is_idempotent()
    {
        Assert.Equal(SplitState.SplitInProgress, SplitState.SplitInProgress.Merge(SplitState.SplitInProgress));
    }

    [Fact]
    public void Merge_never_goes_backwards()
    {
        Assert.Equal(SplitState.SplitComplete, SplitState.SplitComplete.Merge(SplitState.Unsplit));
        Assert.Equal(SplitState.SplitComplete, SplitState.SplitComplete.Merge(SplitState.SplitInProgress));
    }

    [Fact]
    public void TryAdvanceTo_succeeds_when_target_is_higher()
    {
        Assert.True(SplitState.Unsplit.TryAdvanceTo(SplitState.SplitInProgress, out var result));
        Assert.Equal(SplitState.SplitInProgress, result);
    }

    [Fact]
    public void TryAdvanceTo_fails_when_target_is_same_or_lower()
    {
        Assert.False(SplitState.SplitComplete.TryAdvanceTo(SplitState.Unsplit, out var result));
        Assert.Equal(SplitState.SplitComplete, result);

        Assert.False(SplitState.SplitInProgress.TryAdvanceTo(SplitState.SplitInProgress, out result));
        Assert.Equal(SplitState.SplitInProgress, result);
    }
}
