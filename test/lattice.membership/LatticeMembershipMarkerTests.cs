using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Trivial build-and-run guard for the scaffolding assembly. It asserts the
/// placeholder marker type exists in its expected namespace, proving the
/// project compiles, references resolve, and the test host runs.
/// </summary>
[TestFixture]
public class LatticeMembershipMarkerTests
{
    [Test]
    public void Marker_type_lives_in_the_membership_namespace()
    {
        var markerType = typeof(LatticeMembershipMarker);

        Assert.That(markerType.Namespace, Is.EqualTo("Orleans.Lattice.Membership"));
    }
}
