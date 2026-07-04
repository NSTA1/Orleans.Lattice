using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// membership add-on (with a deterministic in-test authenticator), and the
/// authorization add-on - so the enforcing <see cref="ILatticeAccessGate"/> is
/// live. Over in-memory grain storage; no network or external store is involved.
/// Shared by the policy-store, decision-engine, and enforcement integration
/// tests.
/// </summary>
public sealed class AuthClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider (source of the silo-side authorization services).</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side authorization policy store.</summary>
    public ILatticeAuthorizationPolicyStore Store =>
        SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    /// <summary>The silo-side compiled-policy snapshot maintainer.</summary>
    internal CompiledPolicySnapshotMaintainer Maintainer =>
        SiloServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    /// <summary>Opens a client-side <see cref="ILattice"/> handle for <paramref name="treeId"/>.</summary>
    /// <remarks>
    /// Client-originated so the ambient credential stamped with
    /// <see cref="AsSubject"/> propagates on the Orleans request context from the
    /// client through the grain, exactly as a real caller's would.
    /// </remarks>
    public ILattice Lattice(string treeId) => Cluster.Client.GetGrain<ILattice>(treeId);

    /// <summary>
    /// Stamps <paramref name="subject"/> (with optional token-asserted
    /// <paramref name="groups"/>) as the ambient caller for the lifetime of the
    /// returned scope, so operations authored inside it are authorized as that
    /// subject.
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

    /// <summary>
    /// Forces a synchronous rebuild of the compiled policy snapshot and returns
    /// its new epoch, so a test observes an authored rule without polling the
    /// asynchronous change-feed.
    /// </summary>
    public Task<long> RebuildPolicyAsync() => Maintainer.RebuildNowAsync();

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
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

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, TestCredentialAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
        }
    }
}
