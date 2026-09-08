using Orleans.Lattice.Api.Mcp.RepoContext;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication.Tests;

/// <summary>
/// Coverage for the reserved <see cref="RepoContextReplicatedTrees"/> enrolment map:
/// it enrols exactly the repository-context layout contract
/// (<see cref="RepoContextTrees.All"/>) - so adding a tree to the layout without a
/// deliberate replication mode fails the build rather than defaulting silently - and
/// pins the vector-membership presence tree to the add-wins
/// <see cref="LatticeMergeMode.OrFlag"/> and the agent-memory tree to the multi-value
/// <see cref="LatticeMergeMode.MvRegister"/>, while every index-plane tree resolves to
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
    public void BuildEnrolmentMap_pins_memory_to_mv_register()
    {
        var map = RepoContextReplicatedTrees.BuildEnrolmentMap();

        Assert.That(map[RepoContextTrees.Memory], Is.EqualTo(LatticeMergeMode.MvRegister));
    }

    [Test]
    public void BuildEnrolmentMap_defaults_every_index_plane_tree_to_lww()
    {
        var map = RepoContextReplicatedTrees.BuildEnrolmentMap();

        // Both halves of the claim need witnesses: an empty map would satisfy
        // the loop, and a map holding only the two pinned exceptions would
        // satisfy it without any tree actually taking the LWW default.
        Assert.That(map, Is.Not.Empty, "the enrolment map must enrol at least one tree");
        Assert.That(
            map.Keys.Any(k => k != RepoContextTrees.VectorMembership && k != RepoContextTrees.Memory),
            Is.True,
            "at least one tree must fall through to the LWW default for this test to test it");

        Assert.Multiple(() =>
        {
            foreach (var kv in map)
            {
                var expected = kv.Key switch
                {
                    var k when k == RepoContextTrees.VectorMembership => LatticeMergeMode.OrFlag,
                    var k when k == RepoContextTrees.Memory => LatticeMergeMode.MvRegister,
                    _ => LatticeMergeMode.LwwRegister,
                };
                Assert.That(kv.Value, Is.EqualTo(expected), kv.Key);
            }
        });
    }
}
