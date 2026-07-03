using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture that pins the opt-in per-tree admission-control caps
/// (<see cref="LatticeOptions.MaxLiveKeys"/> and the advisory ceilings) on two
/// named trees so the integration and chaos tests can prove the best-effort,
/// eventually-consistent enforcement path: an enforcing tree eventually rejects
/// writes past its cap with <see cref="LatticeQuotaExceededException"/>, while an
/// advisory-only tree never rejects. A short
/// <see cref="LatticeOptions.StorageUsageCacheTtl"/> keeps the coalesced
/// aggregate fresh so the cap bites promptly under test.
/// </summary>
public sealed class AdmissionControlClusterFixture
{
    /// <summary>Tree with an enforcing live-key cap.</summary>
    public const string EnforcingTreeId = "adm-enforce";

    /// <summary>Tree with only an advisory (non-enforcing) live-key ceiling.</summary>
    public const string AdvisoryTreeId = "adm-advisory";

    /// <summary>The enforcing live-key cap configured on <see cref="EnforcingTreeId"/>.</summary>
    public const long MaxLiveKeys = 5;

    /// <summary>The advisory live-key ceiling configured on <see cref="AdvisoryTreeId"/>.</summary>
    public const long AdvisoryLiveKeys = 5;

    private const int TestShardCount = 4;
    private const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();

        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(EnforcingTreeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = TestShardCount,
        });
        await registry.RegisterAsync(AdvisoryTreeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = TestShardCount,
        });
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            // Keep the coalesced aggregate fresh so the best-effort cap bites
            // quickly under test, and pin the synchronous digest shape.
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.StorageUsageCacheTtl = TimeSpan.FromMilliseconds(100);
            });
            siloBuilder.ConfigureLattice(EnforcingTreeId, o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.StorageUsageCacheTtl = TimeSpan.FromMilliseconds(100);
                o.MaxLiveKeys = MaxLiveKeys;
            });
            siloBuilder.ConfigureLattice(AdvisoryTreeId, o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.StorageUsageCacheTtl = TimeSpan.FromMilliseconds(100);
                o.AdmissionAdvisoryLiveKeys = AdvisoryLiveKeys;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
