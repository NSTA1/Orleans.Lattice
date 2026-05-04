using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Acceptance fixture for the <see cref="LatticeOptions.LeafShadowWrites"/>
/// default flip: confirms that reads continue to return correct values
/// after the option flips <c>true → false → true</c> across silo
/// restarts (each restart materialised here as a full
/// <see cref="TestCluster"/> teardown and rebuild). The default-flip
/// regression coverage in
/// <c>LatticeOptionsValidatorTests.DefaultLeafShadowWrites_is_false</c>
/// asserts the production default; this fixture asserts the live
/// commit path stays correct under both option values.
/// </summary>
[TestFixture]
public class LeafShadowWritesRestartTests
{
    private const string TreeName = "leaf-shadow-restart";

    private static async Task<TestCluster> BuildClusterAsync(bool leafShadowWrites)
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<ShadowWritesConfigurator>();
        ShadowWritesConfigurator.LeafShadowWrites = leafShadowWrites;
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static async Task<byte[]?> WriteAndReadAsync(TestCluster cluster, string key, string value)
    {
        var lattice = cluster.Client.GetGrain<ILattice>(TreeName);
        await lattice.SetAsync(key, Encoding.UTF8.GetBytes(value));
        return await lattice.GetAsync(key);
    }

    [Test]
    public async Task Reads_remain_correct_after_LeafShadowWrites_flips_true_false_true()
    {
        // Phase 1: cluster with LeafShadowWrites = true (legacy fallback mode).
        var clusterTrue1 = await BuildClusterAsync(leafShadowWrites: true);
        try
        {
            var observed = await WriteAndReadAsync(clusterTrue1, "k1", "phase1");
            Assert.That(observed, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(observed!), Is.EqualTo("phase1"));
        }
        finally
        {
            await clusterTrue1.StopAllSilosAsync();
            await clusterTrue1.DisposeAsync();
        }

        // Phase 2: silo restart; cluster boots with LeafShadowWrites = false
        // (the new default). Reads against fresh writes must still return
        // the committed value through the WAL-only commit path.
        var clusterFalse = await BuildClusterAsync(leafShadowWrites: false);
        try
        {
            var observed = await WriteAndReadAsync(clusterFalse, "k2", "phase2");
            Assert.That(observed, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(observed!), Is.EqualTo("phase2"));
        }
        finally
        {
            await clusterFalse.StopAllSilosAsync();
            await clusterFalse.DisposeAsync();
        }

        // Phase 3: another silo restart; rollback to LeafShadowWrites = true.
        // Reads continue to return correct values once the option flips back.
        var clusterTrue2 = await BuildClusterAsync(leafShadowWrites: true);
        try
        {
            var observed = await WriteAndReadAsync(clusterTrue2, "k3", "phase3");
            Assert.That(observed, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(observed!), Is.EqualTo("phase3"));
        }
        finally
        {
            await clusterTrue2.StopAllSilosAsync();
            await clusterTrue2.DisposeAsync();
        }
    }

    /// <summary>
    /// Silo configurator that wires <c>AddLattice</c> with memory grain
    /// storage and applies the requested
    /// <see cref="LatticeOptions.LeafShadowWrites"/> value via
    /// <c>ConfigureLattice</c>. The option value is communicated through
    /// a static field because <see cref="TestClusterBuilder"/> requires
    /// a parameterless configurator type.
    /// </summary>
    private sealed class ShadowWritesConfigurator : ISiloConfigurator
    {
        public static bool LeafShadowWrites { get; set; }

        public void Configure(ISiloBuilder siloBuilder)
        {
            var leafShadowWrites = LeafShadowWrites;
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.LeafShadowWrites = leafShadowWrites);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
