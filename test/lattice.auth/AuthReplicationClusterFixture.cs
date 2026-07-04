using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// A two-site integration harness for the auth/membership system-tree replication
/// story (issue #982). Each "site" is an independent single-silo
/// <see cref="TestCluster"/> wired with the core lattice, membership, auth (live
/// <see cref="PolicyAccessGate"/>, default-deny), and the replication add-on with
/// the reserved system trees enrolled via
/// <see cref="LatticeReplicationServiceCollectionExtensions.ReplicateLatticeSystemTrees"/>.
/// One tree id is additionally opted into the strict-consistency epoch fence so a
/// single fixture serves both the convergence and the fence tests. No real network
/// is used: cross-site delivery is simulated by scanning the source policy tree and
/// driving the destination's <see cref="IReplicationApplier"/> directly.
/// </summary>
public sealed class AuthReplicationClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on every silo.</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>A tree id opted into the strict-consistency epoch fence.</summary>
    public const string StrictTree = "strict-app";

    /// <summary>Cluster id assigned to the first site.</summary>
    public const string SiteAClusterId = "site-a";

    /// <summary>Cluster id assigned to the second site.</summary>
    public const string SiteBClusterId = "site-b";

    /// <summary>The first site's single-silo cluster (the source of policy edits).</summary>
    public TestCluster SiteA { get; private set; } = null!;

    /// <summary>The second site's single-silo cluster (the replication receiver).</summary>
    public TestCluster SiteB { get; private set; } = null!;

    private static IServiceProvider Services(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>Site A's silo-side service provider.</summary>
    public IServiceProvider SiteAServices => Services(SiteA);

    /// <summary>Site B's silo-side service provider.</summary>
    public IServiceProvider SiteBServices => Services(SiteB);

    /// <summary>Site A's authorization policy store.</summary>
    public ILatticeAuthorizationPolicyStore StoreA =>
        SiteAServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    /// <summary>Site B's authorization policy store.</summary>
    public ILatticeAuthorizationPolicyStore StoreB =>
        SiteBServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    /// <summary>Site B's replication applier (the receiver-side apply seam).</summary>
    public IReplicationApplier ApplierB =>
        SiteBServices.GetRequiredService<IReplicationApplier>();

    internal CompiledPolicySnapshotMaintainer MaintainerA =>
        SiteAServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    internal CompiledPolicySnapshotMaintainer MaintainerB =>
        SiteBServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    /// <summary>Opens a site A client-side lattice handle for <paramref name="treeId"/>.</summary>
    public ILattice LatticeA(string treeId) => SiteA.Client.GetGrain<ILattice>(treeId);

    /// <summary>Opens a site B client-side lattice handle for <paramref name="treeId"/>.</summary>
    public ILattice LatticeB(string treeId) => SiteB.Client.GetGrain<ILattice>(treeId);

    /// <summary>Forces a synchronous policy-snapshot rebuild on site A and returns its epoch.</summary>
    public Task<long> RebuildAAsync() => MaintainerA.RebuildNowAsync();

    /// <summary>Forces a synchronous policy-snapshot rebuild on site B and returns its epoch.</summary>
    public Task<long> RebuildBAsync() => MaintainerB.RebuildNowAsync();

    /// <summary>
    /// Simulates replication of the reserved auth policy tree from site A to site
    /// B: scans every entry of site A's policy tree (read under the bootstrap
    /// administrator, which bypasses policy) and applies each one to site B
    /// through <see cref="ApplierB"/> with a site-a origin and a high source clock,
    /// exactly as a receiver would apply shipped entries.
    /// </summary>
    public async Task ReplicatePolicyTreeAtoBAsync()
    {
        var source = LatticeA(LatticeSystemTreeNames.AuthPolicy);
        var entries = new List<KeyValuePair<string, byte[]>>();
        using (AsSubject(BootstrapAdmin))
        {
            var cursor = await source.OpenEntryCursorAsync();
            while (true)
            {
                var page = await source.NextEntriesAsync(cursor, 128);
                entries.AddRange(page.Entries);
                if (!page.HasMore)
                {
                    break;
                }
            }
        }

        var applier = ApplierB;
        // A high wall clock so the replicated write wins LWW against anything the
        // receiver might already hold, and a stable per-origin monotonic counter.
        var baseTicks = DateTime.UtcNow.Ticks;
        for (var i = 0; i < entries.Count; i++)
        {
            await applier.ApplyAsync(new WalRecord
            {
                TreeId = LatticeSystemTreeNames.AuthPolicy,
                Op = MutationKind.Set,
                Key = entries[i].Key,
                Value = entries[i].Value,
                Timestamp = new HybridLogicalClock { WallClockTicks = baseTicks, Counter = i },
                OriginClusterId = SiteAClusterId,
                Mode = LatticeMergeMode.LwwRegister,
            });
        }
    }

    /// <summary>
    /// Simulates replication of a rule <b>revocation</b> from site A to site B:
    /// ships a delete of the removed rule's policy-tree entry through
    /// <see cref="ApplierB"/> with a site-a origin and a high source clock, exactly
    /// as a receiver would apply a shipped deletion. Pairs with
    /// <see cref="ReplicatePolicyTreeAtoBAsync"/> (which ships present entries) so a
    /// test can converge a revoke that the present-entry scan alone cannot express.
    /// </summary>
    /// <param name="treeId">The governed tree id of the revoked rule.</param>
    /// <param name="ruleId">The revoked rule id.</param>
    public async Task ReplicatePolicyRevokeAtoBAsync(string treeId, string ruleId)
    {
        var key = $"{treeId}{AuthConstants.RuleKeySeparator}{ruleId}";
        await ApplierB.ApplyAsync(new WalRecord
        {
            TreeId = LatticeSystemTreeNames.AuthPolicy,
            Op = MutationKind.Delete,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 0 },
            OriginClusterId = SiteAClusterId,
            Mode = LatticeMergeMode.LwwRegister,
        });
    }

    /// <summary>
    /// Stamps <paramref name="subject"/> (with optional token-asserted
    /// <paramref name="groups"/>) as the ambient caller for the lifetime of the
    /// returned scope.
    /// </summary>
    public static IDisposable AsSubject(string subject, params string[] groups)
    {
        IReadOnlyDictionary<string, string>? metadata = groups is { Length: > 0 }
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [TestCredentialAuthenticator.GroupsMetadataKey] = string.Join(',', groups),
            }
            : null;

        return LatticeCredentialContext.Use(subject, scheme: TestCredentialAuthenticator.Scheme, metadata: metadata);
    }

    /// <summary>Deploys both sites.</summary>
    public async Task InitializeAsync()
    {
        SiteA = await BuildSiteAsync<SiteASiloConfigurator>();
        SiteB = await BuildSiteAsync<SiteBSiloConfigurator>();
    }

    /// <summary>Stops and disposes both sites.</summary>
    public async Task DisposeAsync()
    {
        if (SiteA is not null)
        {
            await SiteA.StopAllSilosAsync();
            await SiteA.DisposeAsync();
        }

        if (SiteB is not null)
        {
            await SiteB.StopAllSilosAsync();
            await SiteB.DisposeAsync();
        }
    }

    private static async Task<TestCluster> BuildSiteAsync<TConfigurator>()
        where TConfigurator : ISiloConfigurator, new()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<TConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static void ConfigureSilo(ISiloBuilder siloBuilder, string clusterId)
    {
        siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
        siloBuilder.UseInMemoryReminderService();
        siloBuilder.AddLatticeMembership();
        siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, TestCredentialAuthenticator>();
        siloBuilder.AddLatticeAuth(options =>
        {
            options.DefaultEffect = LatticeEffect.Deny;
            options.BootstrapAdministrators.Add(BootstrapAdmin);
            options.StrictConsistencyTrees = new HashSet<string>(StringComparer.Ordinal) { StrictTree };
        });
        siloBuilder.AddLatticeReplication(opts => opts.ClusterId = clusterId);
        siloBuilder.ReplicateLatticeSystemTrees();
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => ConfigureSilo(siloBuilder, SiteAClusterId);
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => ConfigureSilo(siloBuilder, SiteBClusterId);
    }
}
