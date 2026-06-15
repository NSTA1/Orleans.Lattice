using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for probe-only background reconciliation: when the
/// coordinator runs in audit mode it detects divergent trees but never repairs
/// them, so orphan membership rows survive the sweep. Runs against a dedicated
/// cluster whose global reconciliation options set <c>ProbeOnly = true</c>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeTagIndexReconcileProbeOnlyIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<ProbeOnlyConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task RunSweepAsync_probe_only_detects_but_does_not_repair()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var index = $"colors-{sfx}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"items-{sfx}");
        await tree.SetAsync("d", Bytes("1"));
        var idx = tree.TagIndex(_cluster.GrainFactory, index);
        await idx.Key("d").AddAsync(["red"]);

        await tree.DeleteAsync("d");
        var report = await _cluster.GrainFactory
            .GetGrain<ITagIndexReconcileGrain>(index).RunSweepAsync();

        // Probe-only never repairs: no orphan rows are removed and the stale
        // membership row still resolves on a tag query.
        Assert.That(report.OrphanRowsRemoved, Is.Zero);
        Assert.That(await idx.WithAnyTags("red").CountAsync(), Is.EqualTo(1));
    }

    private sealed class ProbeOnlyConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLatticeTagIndexReconciliation(o => o.ProbeOnly = true);
        }
    }
}
