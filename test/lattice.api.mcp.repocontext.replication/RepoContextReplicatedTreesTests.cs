using Orleans.Lattice.Api.Mcp.RepoContext;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication.Tests;

/// <summary>
/// Coverage for the reserved <see cref="RepoContextReplicatedTrees"/> enrolment map:
/// it enrols exactly the repository-context layout contract
/// (<see cref="RepoContextTrees.All"/>) - so adding a tree to the layout without a
/// deliberate replication mode fails the build rather than defaulting silently - and
/// pins the vector-membership presence tree to the add-wins
/// <see cref="LatticeMergeMode.OrFlag"/> while every other tree resolves to
/// <see cref="LatticeMergeMode.LwwRegister"/>.
/// </summary>
[TestFixture]
public class RepoContextReplicatedTreesTests
{
    [Test]
    public void BuildEnrolmentMap_enrols_exactly_the_repo_context_layout_contract()
    {
        var map = RepoContextReplicatedTrees.BuildEnrolmentMap();

        Assert.That(map.Keys, Is.EquivalentTo(RepoContextTrees.All));
    }

    [Test]
    public void BuildEnrolmentMap_pins_vector_membership_to_or_flag()
    {
        var map = RepoContextReplicatedTrees.BuildEnrolmentMap();

        Assert.That(map[RepoContextTrees.VectorMembership], Is.EqualTo(LatticeMergeMode.OrFlag));
    }

    [Test]
    public void BuildEnrolmentMap_defaults_every_non_membership_tree_to_lww()
    {
        var map = RepoContextReplicatedTrees.BuildEnrolmentMap();

        Assert.Multiple(() =>
        {
            foreach (var kv in map)
            {
                var expected = kv.Key == RepoContextTrees.VectorMembership
                    ? LatticeMergeMode.OrFlag
                    : LatticeMergeMode.LwwRegister;
                Assert.That(kv.Value, Is.EqualTo(expected), kv.Key);
            }
        });
    }
}
