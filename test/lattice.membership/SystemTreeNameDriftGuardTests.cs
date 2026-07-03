using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Drift guard for the reserved membership tree ids mirrored into
/// <see cref="LatticeSystemTreeNames"/>. The replication package cannot reference
/// <c>Orleans.Lattice.Membership</c> (it sits below it), so it re-declares the
/// membership tree ids as its own public constants; this test - which can see
/// both the canonical internal <see cref="MembershipConstants"/> and the mirrored
/// public constants - fails the moment they diverge, so a rename can never
/// silently stop a membership tree from replicating (which would let identity
/// diverge across clusters).
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class SystemTreeNameDriftGuardTests
{
    [Test]
    public void Membership_tree_mirrors_match_the_canonical_constants()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeSystemTreeNames.MembershipUsers, Is.EqualTo(MembershipConstants.UsersTree));
            Assert.That(LatticeSystemTreeNames.MembershipGroups, Is.EqualTo(MembershipConstants.GroupsTree));
            Assert.That(LatticeSystemTreeNames.MembershipEdges, Is.EqualTo(MembershipConstants.EdgesTree));
        });
    }
}
