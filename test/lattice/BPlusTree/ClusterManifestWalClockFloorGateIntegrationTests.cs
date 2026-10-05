using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The capability gate against a real cluster: every silo of a current build
/// advertises the marker, so the gate is open on each of them.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ClusterManifestWalClockFloorGateIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task The_gate_is_open_on_every_silo_of_a_floor_capable_cluster()
    {
        var silos = _fixture.Cluster.GetActiveSilos().Cast<InProcessSiloHandle>().ToList();
        var deadline = DateTime.UtcNow.AddSeconds(30);
        var open = false;
        while (!open && DateTime.UtcNow < deadline)
        {
            open = silos.All(s => s.SiloHost.Services.GetRequiredService<IWalClockFloorGate>().IsOpen);
            if (!open)
            {
                await Task.Delay(200);
            }
        }

        Assert.That(open, Is.True, "every silo of this build advertises the floor capability in its manifest");
    }
}
