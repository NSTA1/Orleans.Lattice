using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Trivial build-and-run guard for the scaffolding assembly. It asserts the
/// placeholder marker type exists in its expected namespace, proving the
/// project compiles, references resolve, and the test host runs.
/// </summary>
[TestFixture]
public class LatticeAuthMarkerTests
{
    [Test]
    public void Marker_type_lives_in_the_auth_namespace()
    {
        var markerType = typeof(LatticeAuthMarker);

        Assert.That(markerType.Namespace, Is.EqualTo("Orleans.Lattice.Auth"));
    }
}
