using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// The named provider key a dedicated-WAL tenant is pinned to in the integration
/// fixture, and the shared inspectable in-memory WAL providers the silo wires it
/// to. The baseline ("default") provider backs every non-dedicated tree; the
/// dedicated provider backs a tenant whose <see cref="TenantPlacement"/> names it.
/// </summary>
internal static class TenantWalProviders
{
    /// <summary>The catalog key the dedicated-WAL tenant "acme" is bound to.</summary>
    public const string DedicatedKey = "wal-acme";

    /// <summary>The "default" baseline WAL provider (backs shared / non-tenant trees).</summary>
    public static InMemoryWalStorageProvider Baseline { get; private set; } = new();

    /// <summary>The dedicated named WAL provider a tenant's trees are isolated to.</summary>
    public static InMemoryWalStorageProvider Dedicated { get; private set; } = new();

    /// <summary>Resets both providers to empty stores between fixtures.</summary>
    public static void Reset()
    {
        Baseline = new InMemoryWalStorageProvider();
        Dedicated = new InMemoryWalStorageProvider();
    }
}

/// <summary>
/// Single-silo <see cref="TestCluster"/> for the per-tenant WAL isolation
/// integration test. It wires the core lattice, the membership and auth add-ons
/// the tenancy registry depends on, the tenancy add-on (whose
/// <see cref="TenantWalPlacementResolver"/> replaces the core null seam), a
/// baseline WAL provider, and a dedicated named provider
/// (<see cref="TenantWalProviders.DedicatedKey"/>) pointing at the shared
/// inspectable instances so the test can observe which store a tree's WAL lands
/// in.
/// </summary>
public sealed class TenantWalPlacementClusterFixture
{
    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side tenant registry the test seeds tenant records into.</summary>
    public ITenantRegistry Registry => SiloServices.GetRequiredService<ITenantRegistry>();

    /// <summary>Deploys the cluster with fresh (empty) WAL providers.</summary>
    public async Task InitializeAsync()
    {
        TenantWalProviders.Reset();
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Registers a single-shard, single-partition tree through the internal
    /// registry (the seam T18 hooks) and returns a reference to it. Registration
    /// is what triggers the placement resolver, so the tenant's record must be
    /// seeded before this is called.
    /// </summary>
    public async Task<ILattice> RegisterTreeAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, WalPartitions = 1 });
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    /// <summary>The cluster-wide admin surface, used to read a tree's WAL placement.</summary>
    public ILatticeAdmin Admin =>
        Cluster.Client.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);

    /// <summary>
    /// Forces the silo's tenant-placement snapshot to rebuild from the registry so a
    /// just-seeded tenant record becomes visible to the placement resolver
    /// deterministically. Production keeps the snapshot current off the core
    /// change-feed (an eventual, background rebuild); the test drives it explicitly
    /// here so the "seed then register" sequence has no wall-clock race - never a
    /// <c>Task.Delay</c> or ordering assumption. This addresses the resolver's
    /// re-entrancy fix: registration reads the in-memory snapshot, not a live
    /// registry, so the snapshot must reflect the seed before the tree is registered.
    /// </summary>
    public Task WarmPlacementSnapshotAsync() =>
        SiloServices.GetRequiredService<TenantPlacementSnapshotMaintainer>().RebuildNowAsync();

    /// <summary>
    /// Addresses a tree through the INTERNAL, unguarded <see cref="ISystemLattice"/>
    /// surface. The public <see cref="ILattice"/> surface rejects a write to a
    /// reserved <c>t/</c> structural tenant id, so the integration tests drive real
    /// WAL traffic into a tenant tree through this surface (the same one the registry
    /// itself uses). The WAL routing and partition-provider machinery is identical to
    /// the public path, so this observes exactly the placement the resolver pinned.
    /// </summary>
    /// <remarks>
    /// Resolved from the <b>silo's own</b> grain factory rather than the external
    /// test client. <see cref="ISystemLattice"/> asserts internal origin (issue
    /// #2062), and this fixture's silo registers the auth add-on, so the
    /// capability-stripping filter strips the internal-origin marker from a call
    /// arriving over the external client and the surface correctly refuses it. The
    /// silo's hosted client is inside the trust boundary, which is the origin the
    /// production caller of this surface - the registry, running in-silo - actually
    /// has, so this addresses the tree exactly as production does.
    /// </remarks>
    internal ISystemLattice SystemTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return SiloServices.GetRequiredService<IGrainFactory>().GetGrain<ISystemLattice>(treeId);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            // This fixture exercises WAL PLACEMENT, not authorization. The tenancy
            // add-on requires the auth add-on (its ordering guard resolves the
            // decision engine), but a deny-by-default gate would refuse the
            // anonymous test writes before placement is ever reached (FINDING 2).
            // Flip the closed-world fallback to Allow so no rule is needed and the
            // tests observe routing rather than access control. Real gate
            // enforcement is T7's concern, not this fixture's.
            siloBuilder.AddLatticeAuth(o => o.DefaultEffect = LatticeEffect.Allow);
            siloBuilder.AddLatticeTenancy();
            // Baseline ("default") and the dedicated named provider both point at
            // the shared inspectable instances so the test can observe routing.
            siloBuilder.AddWalStorage(_ => TenantWalProviders.Baseline);
            siloBuilder.AddLatticeWalStorageProvider(
                TenantWalProviders.DedicatedKey, _ => TenantWalProviders.Dedicated);
        }
    }
}
