using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Tests.Topology;

[TestFixture]
public class RadialLayoutTests
{
    [Test]
    public void RingRadius_SingleRoot_PlacesRootAtCentre()
    {
        Assert.That(RadialLayout.RingRadius(0, rootCount: 1), Is.EqualTo(0));
    }

    [Test]
    public void RingRadius_MultipleRoots_PlacesRootsOnInnerRing()
    {
        Assert.That(RadialLayout.RingRadius(0, rootCount: 3), Is.EqualTo(RadialLayout.InnerRingRadius));
    }

    [Test]
    public void RingRadius_GrowsOneStepPerLevel()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RadialLayout.RingRadius(1, rootCount: 1), Is.EqualTo(RadialLayout.LevelStep));
            Assert.That(RadialLayout.RingRadius(2, rootCount: 1), Is.EqualTo(2 * RadialLayout.LevelStep));
            Assert.That(
                RadialLayout.RingRadius(2, rootCount: 2),
                Is.EqualTo(RadialLayout.InnerRingRadius + (2 * RadialLayout.LevelStep)));
        });
    }

    [Test]
    public void Project_SingleRootAtLevelZero_LandsOnOrigin()
    {
        var point = RadialLayout.Project(column: 2.5, level: 0, columnCount: 6, rootCount: 1);

        Assert.Multiple(() =>
        {
            Assert.That(point.Radius, Is.EqualTo(0));
            Assert.That(point.X, Is.EqualTo(0).Within(1e-9));
            Assert.That(point.Y, Is.EqualTo(0).Within(1e-9));
        });
    }

    [Test]
    public void Project_PlacesNodeOnRingAtItsRadius()
    {
        var point = RadialLayout.Project(column: 1, level: 2, columnCount: 4, rootCount: 1);

        var distance = Math.Sqrt((point.X * point.X) + (point.Y * point.Y));
        Assert.Multiple(() =>
        {
            Assert.That(point.Radius, Is.EqualTo(RadialLayout.RingRadius(2, 1)));
            Assert.That(distance, Is.EqualTo(point.Radius).Within(1e-9));
        });
    }

    [Test]
    public void Project_ZeroColumnCount_DoesNotDivideByZero()
    {
        var point = RadialLayout.Project(column: 0, level: 1, columnCount: 0, rootCount: 1);

        Assert.That(point.AngleRadians, Is.EqualTo(0));
        Assert.That(point.Radius, Is.EqualTo(RadialLayout.LevelStep));
    }

    [Test]
    public void Extent_FramesOutermostRingPlusNodeAndPadding()
    {
        var extent = RadialLayout.Extent(levelCount: 3, rootCount: 1);

        Assert.That(
            extent,
            Is.EqualTo(RadialLayout.RingRadius(2, 1) + RadialLayout.NodeRadius + RadialLayout.Padding));
    }

    [Test]
    public void Extent_EmptyForest_IsNonNegative()
    {
        Assert.That(RadialLayout.Extent(levelCount: 0, rootCount: 0), Is.GreaterThan(0));
    }
}
