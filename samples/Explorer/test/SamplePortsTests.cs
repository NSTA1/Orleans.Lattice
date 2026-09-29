namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class SamplePortsTests
{
    [Test]
    public void The_default_ports_are_distinct_and_keep_the_documented_console_and_grpc_ports()
    {
        var ports = SamplePorts.Default;
        var all = new[] { ports.EastGrpc, ports.EastWeb, ports.EastSilo, ports.EastGateway, ports.WestGrpc, ports.WestSilo, ports.WestGateway };

        Assert.That(all, Is.Unique);
        Assert.That(ports.EastWeb, Is.EqualTo(5080));
        Assert.That(ports.EastGrpc, Is.EqualTo(5199));
    }

    [Test]
    public void Offset_shifts_every_port()
    {
        var shifted = SamplePorts.Default.Offset(10);

        Assert.That(shifted, Is.EqualTo(new SamplePorts(5209, 5090, 11121, 30010, 5208, 11122, 30011)));
    }

    [Test]
    public void A_zero_offset_changes_nothing() =>
        Assert.That(SamplePorts.Default.Offset(0), Is.EqualTo(SamplePorts.Default));

    [Test]
    public void A_negative_offset_throws() =>
        Assert.That(() => SamplePorts.Default.Offset(-1), Throws.InstanceOf<ArgumentOutOfRangeException>());

    [Test]
    public void The_largest_offset_keeps_every_port_valid()
    {
        var shifted = SamplePorts.Default.Offset(ExplorerSampleOptions.MaxPortOffset);

        Assert.That(
            new[] { shifted.EastGrpc, shifted.EastWeb, shifted.EastSilo, shifted.EastGateway, shifted.WestGrpc, shifted.WestSilo, shifted.WestGateway },
            Has.All.LessThanOrEqualTo(65535));
    }
}
