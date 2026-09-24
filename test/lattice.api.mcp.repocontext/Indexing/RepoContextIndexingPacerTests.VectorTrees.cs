using NSubstitute;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Pins <see cref="RepoContextIndexingPacer.FindVectorTree"/>, the one read of the
/// platform's saturation verdict over the vector trees that both the pacer and the
/// embedding ingestor consult (issue #2683).
/// </summary>
public sealed partial class RepoContextIndexingPacerTests
{
    [Test]
    public void FindVectorTree_with_no_signal_returns_null()
    {
        Assert.That(
            RepoContextIndexingPacer.FindVectorTree(null, WalSaturationState.Healthy),
            Is.Null,
            "a host with no signal has no verdict to report, not a healthy one");
    }

    [Test]
    public void FindVectorTree_with_every_tree_healthy_matches_nothing_above_healthy()
    {
        var signal = Signal();

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextIndexingPacer.FindVectorTree(signal, WalSaturationState.Throttled), Is.Null);
            Assert.That(RepoContextIndexingPacer.FindVectorTree(signal, WalSaturationState.Saturated), Is.Null);
        });
    }

    [Test]
    public void FindVectorTree_matches_a_throttled_tree_only_at_or_below_its_state()
    {
        // Issue #2683's measured case: the membership tree was Throttled, and a
        // Throttled tree still admits appends, so it must not read as Saturated.
        var signal = Signal(RepoContextTrees.VectorMembership, WalSaturationState.Throttled);

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextIndexingPacer.FindVectorTree(signal, WalSaturationState.Throttled),
                Is.EqualTo(RepoContextTrees.VectorMembership));
            Assert.That(
                RepoContextIndexingPacer.FindVectorTree(signal, WalSaturationState.Saturated),
                Is.Null,
                "Throttled is not Saturated");
        });
    }

    [Test]
    public void FindVectorTree_reports_a_saturated_tree_whichever_vector_tree_it_is()
    {
        foreach (var tree in new[]
                 {
                     RepoContextTrees.VectorMembership,
                     RepoContextTrees.VectorMetadata,
                     RepoContextTrees.VectorPayload,
                 })
        {
            var signal = Signal(tree, WalSaturationState.Saturated);

            Assert.That(
                RepoContextIndexingPacer.FindVectorTree(signal, WalSaturationState.Saturated),
                Is.EqualTo(tree),
                $"a saturated {tree} is reported");
        }
    }

    [Test]
    public void FindVectorTree_ignores_trees_outside_the_vector_plane()
    {
        var signal = Signal(RepoContextTrees.Symbol, WalSaturationState.Saturated);

        Assert.That(
            RepoContextIndexingPacer.FindVectorTree(signal, WalSaturationState.Saturated),
            Is.Null,
            "the ingestor's store and record stages write only the vector trees");
    }

    [Test]
    public void FindVectorTree_returns_the_first_matching_tree_in_write_order()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Healthy);
        signal.GetCurrentState(RepoContextTrees.VectorMetadata).Returns(WalSaturationState.Saturated);
        signal.GetCurrentState(RepoContextTrees.VectorPayload).Returns(WalSaturationState.Saturated);

        Assert.That(
            RepoContextIndexingPacer.FindVectorTree(signal, WalSaturationState.Saturated),
            Is.EqualTo(RepoContextTrees.VectorMetadata));
    }
}
