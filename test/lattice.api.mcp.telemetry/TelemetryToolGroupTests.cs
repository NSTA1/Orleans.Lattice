namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="TelemetryToolGroup"/>: it serves the telemetry group and
/// - in C1 - contributes no tools.
/// </summary>
[TestFixture]
public sealed class TelemetryToolGroupTests
{
    [Test]
    public void Group_is_telemetry()
        => Assert.That(new TelemetryToolGroup().Group, Is.EqualTo(LatticeApiMcpGroup.Telemetry));

    [Test]
    public void Tools_are_empty_in_the_skeleton()
        => Assert.That(new TelemetryToolGroup().Tools, Is.Empty);

    [Test]
    public void Tools_is_a_stable_non_null_instance()
    {
        var group = new TelemetryToolGroup();
        Assert.That(group.Tools, Is.Not.Null.And.SameAs(group.Tools));
    }
}
