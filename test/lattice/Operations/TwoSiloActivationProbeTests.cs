using Microsoft.Extensions.DependencyInjection;
using Orleans.Concurrency;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Guards the two-silo harness the operation chaos tests rely on (issue #4194):
/// grain state written through one silo's storage is read through the other's,
/// and no grain id - an ordinary grain, the coordinated-operation grains, or the
/// B+ tree's leaves - has more than one activation, judged by grain id from
/// detailed grain statistics rather than by per-type activation counts (one
/// activation of a type on each silo is normally two different ids).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TwoSiloActivationProbeTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_cluster);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task Grain_state_written_through_one_silos_storage_is_read_through_the_others()
    {
        var primary = Storage(_cluster.Primary);
        var secondary = Storage(_cluster.SecondarySilos[0]);
        Assert.That(primary, Is.Not.SameAs(secondary), "Each silo resolves its own provider instance.");

        var grainId = GrainId.Create("probe", Guid.NewGuid().ToString("N"));
        await primary.WriteStateAsync("probe-state", grainId, new GrainState<string> { State = "written-on-primary" });

        var read = new GrainState<string>();
        await secondary.ReadStateAsync("probe-state", grainId, read);

        Assert.Multiple(() =>
        {
            Assert.That(read.RecordExists, Is.True);
            Assert.That(read.State, Is.EqualTo("written-on-primary"));
        });
    }

    [Test]
    public async Task An_ordinary_grain_id_answers_from_one_activation_whichever_silo_calls_it()
    {
        for (var i = 0; i < 64; i++)
        {
            var id = $"probe-{i}";
            var tags = new HashSet<string>(StringComparer.Ordinal)
            {
                await _cluster.Client.GetGrain<IActivationIdentityProbeGrain>(id).GetActivationTagAsync(),
                await Factory(_cluster.Primary).GetGrain<IActivationIdentityProbeGrain>(id).GetActivationTagAsync(),
                await Factory(_cluster.SecondarySilos[0]).GetGrain<IActivationIdentityProbeGrain>(id).GetActivationTagAsync(),
            };
            Assert.That(tags, Has.Count.EqualTo(1), $"Grain id {id} answered from more than one activation.");
        }

        await AssertOneActivationPerGrainIdAsync();
    }

    [Test]
    public async Task Operation_progress_reported_on_one_silo_is_read_from_the_other_silo_and_the_client()
    {
        var runner = Runner(_cluster.SecondarySilos[0]);
        var reader = Runner(_cluster.Primary);
        var never = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var operationIds = new List<string>();

        for (var i = 0; i < 20; i++)
        {
            var operationId = LatticeOperationKey.NewId();
            operationIds.Add(operationId);
            var reported = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await runner.StartAsync(
                new LatticeOperationStart
                {
                    TenantId = "default",
                    OperationId = operationId,
                    Kind = "probe.test",
                    TreeIds = ["probe-tree"],
                    Phases = ["Working"],
                },
                async (progress, ct) =>
                {
                    await progress.ReportAsync("Working", 4, 20, "units");
                    reported.SetResult();
                    return await never.Task.WaitAsync(ct);
                },
                static _ => LatticeOperationCompletion.Succeeded());
            await reported.Task;

            var fromPrimary = await reader.GetAsync("default", operationId);
            var fromClient = await _cluster.Client
                .GetGrain<ILatticeOperationGrain>(LatticeOperationKey.For("default", operationId))
                .GetAsync();

            Assert.Multiple(() =>
            {
                Assert.That(fromPrimary?.CompletedUnits, Is.EqualTo(4), $"Primary read of {operationId}.");
                Assert.That(fromPrimary?.TotalUnits, Is.EqualTo(20), $"Primary read of {operationId}.");
                Assert.That(fromClient?.CompletedUnits, Is.EqualTo(4), $"Client read of {operationId}.");
            });
        }

        await AssertOneActivationPerGrainIdAsync();

        foreach (var operationId in operationIds)
        {
            await runner.RequestCancelAsync("default", operationId);
        }
    }

    [Test]
    public async Task Every_bplus_tree_grain_id_has_one_activation_after_writes_and_reads_from_both_silos()
    {
        const string treeId = "activation-probe-tree";
        var entries = Enumerable.Range(0, 2048)
            .Select(i => KeyValuePair.Create($"k{i:D5}", new byte[] { (byte)i }))
            .ToList();

        await Factory(_cluster.SecondarySilos[0]).GetGrain<ILattice>(treeId).SetManyAsync(entries);
        await Factory(_cluster.Primary).GetGrain<ILattice>(treeId).SetManyAsync(entries[..1024]);
        await _cluster.Client.GetGrain<ILattice>(treeId).SetManyAsync(entries[1024..]);

        foreach (var source in new[] { Factory(_cluster.Primary), Factory(_cluster.SecondarySilos[0]), _cluster.Client })
        {
            Assert.That(await source.GetGrain<ILattice>(treeId).CountAsync(), Is.EqualTo(entries.Count));
        }

        var leaves = await AssertOneActivationPerGrainIdAsync();
        Assert.That(
            leaves.Count(s => s.GrainType.Contains(".BPlusLeafGrain,", StringComparison.Ordinal)),
            Is.GreaterThan(0),
            "The probe must observe BPlusLeafGrain activations, or it proves nothing about them.");
    }

    private async Task<DetailedGrainStatistic[]> AssertOneActivationPerGrainIdAsync()
    {
        var stats = await _cluster.Client.GetGrain<IManagementGrain>(0).GetDetailedGrainStatistics();
        var duplicates = stats
            .Where(s => !IsStatelessWorker(s.GrainType))
            .GroupBy(s => s.GrainId)
            .Where(g => g.Count() > 1)
            .Select(g => $"{g.Key} on [{string.Join(", ", g.Select(s => s.SiloAddress))}]")
            .ToList();
        Assert.That(duplicates, Is.Empty, "Grain ids with more than one activation.");
        return stats;
    }

    private static bool IsStatelessWorker(string grainType) =>
        Type.GetType(grainType) is { } type
        && type.GetCustomAttributes(typeof(StatelessWorkerAttribute), inherit: true).Length > 0;

    private static IServiceProvider Services(SiloHandle silo) => ((InProcessSiloHandle)silo).SiloHost.Services;

    private static IGrainFactory Factory(SiloHandle silo) => Services(silo).GetRequiredService<IGrainFactory>();

    private static LatticeOperationRunner Runner(SiloHandle silo) => Services(silo).GetRequiredService<LatticeOperationRunner>();

    private static IGrainStorage Storage(SiloHandle silo) =>
        Services(silo).GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<IGrainStorage>(
                    name,
                    (_, _) => new Orleans.Lattice.Tests.BPlusTree.PublicApiContract.ProcessScopeMemoryGrainStorage()));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.Configure<LatticeOperationOptions>(o =>
            {
                o.HeartbeatInterval = TimeSpan.FromSeconds(1);
                o.HeartbeatLease = TimeSpan.FromSeconds(15);
            });
        }
    }
}
