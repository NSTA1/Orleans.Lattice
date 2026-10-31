using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Issue #5482: an uninstall whose steps outlast the silo's grain response timeout must still
/// run to completion and report its definite, recorded outcome. The run withdraws the app's
/// rules and replication, soft-deletes each owned tree, transitions the registry and records
/// the outcome, all inside one <see cref="IAppActivationGrain.ExecuteAsync"/> call. Before the
/// fix that call carried the default response timeout, so on a loaded estate the caller saw an
/// opaque <see cref="TimeoutException"/> while the run was cancelled part-way and left nothing
/// recorded.
/// <para>
/// The slow step is pinned deterministically: the app's tree soft-delete is held until the test
/// releases it. The silo's runtime response timeout is shortened only for the uninstall, so the
/// hold outlasts it in seconds; setup runs under the default timeout.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AppActivationResponseTimeoutClusterTests
{
    private const string Resource = "app.manifest.json";

    /// <summary>The silo's response timeout while the uninstall runs.</summary>
    private static readonly TimeSpan ShortResponseTimeout = TimeSpan.FromSeconds(3);

    /// <summary>The bound on every wait for a condition the test expects to become true.</summary>
    private static readonly TimeSpan HangBound = TimeSpan.FromSeconds(60);

    private static readonly AppSlug Slug = AppSlug.Parse("slow-app");

    private static readonly SeededAppRegistryStore RegistryStore = new();

    private TestCluster _cluster = null!;

    private IServiceProvider Silo => _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private static string Manifest => $$"""
        {
          "identity": { "slug": "{{Slug}}", "version": "1.0.0" },
          "trees": [{ "name": "records", "shardCount": 2 }],
          "roles": [{ "name": "reader", "operations": ["Read"], "scopes": [{ "tree": "records" }] }],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        RegistryStore.Seed(AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, slug: Slug) with
        {
            RoleBindings = new[] { AppRoleBinding.Create("reader", "readers") },
        });

        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        SoftDeleteGate.Release.TrySetResult();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task An_uninstall_whose_tree_soft_delete_outlasts_the_response_timeout_completes_and_records_its_outcome()
    {
        var pipeline = Silo.GetRequiredService<IAppActivationPipeline>();
        var grains = Silo.GetRequiredService<IGrainFactory>();
        var tree = AppActivationTreeNames.LocalStructuralTree(Slug, "records");

        // Setup under the default timeout: the startup reconcile enables the app, and a disable
        // warms every grain the uninstall touches besides the held soft-delete.
        await TestPoll.UntilAsync(
            async () => await pipeline.GetStatusAsync(TenantId.Default, Slug) is { LastOutcome.Succeeded: true },
            "the startup reconcile to enable the app",
            timeout: HangBound);
        AppActivationOutcome disabled;
        using (LatticeSystemOrigin.Enter())
        {
            disabled = await pipeline.DisableAsync(TenantId.Default, Slug);
        }

        Assert.That(disabled.Succeeded, Is.True, () => string.Join("; ", disabled.Diagnostics.Select(d => d.Message)));
        Assert.That(await grains.GetLatticeRegistry().ExistsAsync(tree), Is.True, "precondition: the app's tree is provisioned");

        var siloTimeout = SiloResponseTimeout.Resolve(Silo);
        var defaultTimeout = siloTimeout.Current;
        AppActivationOutcome outcome;
        SoftDeleteGate.Arm(tree);
        siloTimeout.Set(ShortResponseTimeout);
        try
        {
            Task<AppActivationOutcome> uninstall;
            try
            {
                using (LatticeSystemOrigin.Enter())
                {
                    uninstall = pipeline.UninstallAsync(TenantId.Default, Slug);
                }

                await SoftDeleteGate.Entered.Task.WaitAsync(HangBound);

                // The run in flight is reported while it is held: the query interleaves with it
                // rather than queueing behind it (it would time out at the shortened timeout).
                Assert.That(await pipeline.GetRunningOperationAsync(TenantId.Default, Slug),
                    Is.EqualTo(AppActivationOperation.Uninstall), "the held uninstall is reported in flight");

                // The soft-delete is held. Wait for the uninstall call to resolve, bounded at a
                // multiple of the response timeout: a call that gives up at the timeout resolves
                // inside the bound, and one that waits for its run does not.
                var first = await Task.WhenAny(uninstall, Task.Delay(ShortResponseTimeout * 3));
                Assert.That(first, Is.Not.SameAs(uninstall),
                    () => $"the uninstall call resolved while its run was still held: {Describe(uninstall)}");
            }
            finally
            {
                SoftDeleteGate.Release.TrySetResult();
            }

            outcome = await uninstall.WaitAsync(HangBound);
        }
        finally
        {
            siloTimeout.Set(defaultTimeout);
        }

        var deleted = await grains.GetGrain<ITreeDeletionGrain>(tree).IsDeletedAsync();
        var status = await pipeline.GetStatusAsync(TenantId.Default, Slug);
        var running = await pipeline.GetRunningOperationAsync(TenantId.Default, Slug);
        Assert.Multiple(() =>
        {
            Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics.Select(d => d.Message)));
            Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Uninstalled));
            Assert.That(deleted, Is.True, "the owned tree is soft-deleted");
            Assert.That(status?.LastOutcome.Operation, Is.EqualTo(AppActivationOperation.Uninstall), "the outcome is recorded");
            Assert.That(status?.LastOutcome.Succeeded, Is.True);
            Assert.That(running, Is.Null, "no run is reported once it has ended");
        });
    }

    private static string Describe(Task<AppActivationOutcome> task) => task.Status switch
    {
        TaskStatus.Faulted => task.Exception!.GetBaseException().ToString(),
        TaskStatus.RanToCompletion => $"outcome {task.Result.Failure}",
        _ => task.Status.ToString(),
    };

    /// <summary>Holds the soft-delete of the armed tree until the test releases it.</summary>
    private static class SoftDeleteGate
    {
        private static volatile string? _treeId;

        public static TaskCompletionSource Entered { get; private set; } = NewSource();

        public static TaskCompletionSource Release { get; private set; } = NewSource();

        public static void Arm(string treeId)
        {
            Entered = NewSource();
            Release = NewSource();
            _treeId = treeId;
        }

        public static async Task HoldAsync(string treeId)
        {
            if (string.Equals(treeId, _treeId, StringComparison.Ordinal))
            {
                Entered.TrySetResult();
                await Release.Task;
            }
        }

        private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>The real provisioner, with each soft-delete passing through <see cref="SoftDeleteGate"/>.</summary>
    private sealed class GatedTreeProvisioner(IAppTreeProvisioner inner) : IAppTreeProvisioner
    {
        public Task EnsureAsync(string treeId, AppTreeDeclaration declaration, CancellationToken cancellationToken) =>
            inner.EnsureAsync(treeId, declaration, cancellationToken);

        public async Task SoftDeleteAsync(string treeId, CancellationToken cancellationToken)
        {
            await SoftDeleteGate.HoldAsync(treeId);
            await inner.SoftDeleteAsync(treeId, cancellationToken);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.AddLatticeAuth();
            siloBuilder.AddLatticeApps()
                .AddLatticeApp(Slug.Value, new FakeAppAssembly(Resource, Manifest), Resource);
            siloBuilder.Services.Replace(ServiceDescriptor.Singleton<IAppRegistryStore>(RegistryStore));
            siloBuilder.Services.Replace(ServiceDescriptor.Singleton<IAppTreeProvisioner>(
                services => new GatedTreeProvisioner(new LatticeAppTreeProvisioner(services.GetRequiredService<IGrainFactory>()))));
        }
    }
}
