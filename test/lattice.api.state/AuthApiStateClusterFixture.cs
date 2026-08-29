using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// read-only state API, the membership add-on (with a deterministic in-test
/// authenticator), and the authorization add-on - so the enforcing
/// <see cref="ILatticeAccessGate"/> is live and the state API's auth-backed read
/// visibility (issue #981) is active. Over in-memory grain storage; no network
/// or external store is involved. Shared by the visibility integration tests.
/// </summary>
internal sealed class AuthApiStateClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust bypass).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The state-API read facade under test.</summary>
    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    /// <summary>The state-API change-feed facade under test.</summary>
    public ILatticeStateObserver Observer => SiloServices.GetRequiredService<ILatticeStateObserver>();

    /// <summary>The silo-side authorization policy store (rule authoring).</summary>
    public ILatticeAuthorizationPolicyStore Store =>
        SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    private CompiledPolicySnapshotMaintainer Maintainer =>
        SiloServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    /// <summary>
    /// Stamps <paramref name="subject"/> (with optional token-asserted
    /// <paramref name="groups"/>) as the ambient caller for the lifetime of the
    /// returned scope, so state-API reads inside it are authorized as that
    /// subject.
    /// </summary>
    public static IDisposable AsSubject(string subject, params string[] groups)
    {
        IReadOnlyDictionary<string, string>? metadata = groups is { Length: > 0 }
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [ApiStateTestCredentialAuthenticator.GroupsMetadataKey] = string.Join(',', groups),
            }
            : null;

        return LatticeCredentialContext.Use(
            subject,
            scheme: ApiStateTestCredentialAuthenticator.Scheme,
            metadata: metadata);
    }

    /// <summary>
    /// Authors one or more rules and forces a synchronous compiled-policy rebuild
    /// so a test observes them without polling the asynchronous change feed.
    /// </summary>
    public async Task GrantAsync(params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            await Store.PutRuleAsync(rule);
        }

        await Maintainer.RebuildNowAsync();
    }

    /// <summary>Forces a synchronous compiled-policy rebuild.</summary>
    public Task<long> RebuildPolicyAsync() => Maintainer.RebuildNowAsync();

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
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
    /// Registers <paramref name="treeId"/> and writes <paramref name="keys"/>
    /// (each a distinct single-byte value), authored under the bootstrap
    /// administrator so the write itself is authorized. Returns the grain handle.
    /// </summary>
    public async Task<ILattice> CreatePopulatedTreeAsync(string treeId, params string[] keys)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = 2,
        });

        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        using (AsSubject(BootstrapAdmin))
        {
            byte value = 0;
            foreach (var key in keys)
            {
                await tree.SetAsync(key, new[] { value++ });
            }
        }

        return tree;
    }

    /// <summary>
    /// Registers an empty single-WAL-partition tree (so the merged change stream
    /// is a strict per-tree order) and returns the grain handle. Used by the
    /// change-feed visibility tests, which write live after subscribing.
    /// </summary>
    public async Task<ILattice> RegisterTreeAsync(string treeId)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = 2,
            WalPartitions = 1,
        });

        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    /// <summary>
    /// Opens a live change subscription for <paramref name="request"/> under the
    /// currently ambient subject, pins it to the tree's current write-ahead-log
    /// tail, then runs <paramref name="mutate"/> and drains up to
    /// <paramref name="expectedCount"/> notifications (or until the timeout
    /// elapses). The subscription captures the ambient subject at open, so the
    /// caller must invoke this inside the observer's <c>AsSubject</c> scope;
    /// <paramref name="mutate"/> may re-scope internally to author writes as
    /// another subject without changing the subscriber's identity.
    /// </summary>
    public async Task<IReadOnlyList<StateChangeNotification>> ObserveWhileAsync(
        StateObserveRequest request,
        int expectedCount,
        Func<Task> mutate)
    {
        // Pin the observed window to a cursor captured BEFORE the mutation
        // rather than to wherever a fresh subscription happens to seed its tail.
        // An earlier revision slept 300ms and hoped the seeding had finished; a
        // slow run (the coverage job's instrumentation being the reliable one)
        // let the first write land ahead of the tail, so the change was never
        // delivered and the test failed having silently missed it. See
        // StateObserveTailCursor for why pinning closes the race rather than
        // narrowing it. A request that already carries a cursor keeps it.
        var pinned = request;
        if (request.ContinuationToken is null)
        {
            // Scaffolding, not the behaviour under test: capture as the
            // bootstrap administrator so the probe itself is never refused for a
            // partially authorized subscriber. The scope is disposed before the
            // subscription opens, so the subscriber's own identity is unchanged.
            string token;
            using (AsSubject(BootstrapAdmin))
            {
                token = await StateObserveTailCursor.CaptureAsync(Cluster.Client, SiloServices, request.TreeId);
            }

            pinned = request with { ContinuationToken = token };
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var collectTask = CollectAsync(pinned, expectedCount, cts.Token);
        await mutate();

        return await collectTask;
    }

    private async Task<IReadOnlyList<StateChangeNotification>> CollectAsync(
        StateObserveRequest request,
        int count,
        CancellationToken cancellationToken)
    {
        var collected = new List<StateChangeNotification>(count);
        try
        {
            await foreach (var notification in Observer.ObserveAsync(request, cancellationToken))
            {
                collected.Add(notification);
                if (collected.Count >= count)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timed out waiting for the expected count; return what arrived.
        }

        return collected;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeStateApi(o =>
            {
                o.ChangeObservationPollInterval = TimeSpan.FromMilliseconds(25);
                o.ChangeObservationPageSize = 64;
            });
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, ApiStateTestCredentialAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
        }
    }
}
