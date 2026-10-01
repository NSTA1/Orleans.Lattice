using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Silo-loss behaviour of the coordinated-operation engine (#4122): an operation
/// whose runner's silo is killed mid-run must read as
/// <see cref="LatticeOperationState.Failed"/> from a surviving silo, never stay
/// <see cref="LatticeOperationState.Running"/>. Resumption is out of scope, so
/// failure is the specified outcome.
/// </summary>
/// <remarks>
/// Grain state lives in a process-scope store shared by every silo, so the
/// operation record survives the killed silo exactly as it would on a durable
/// provider; only the runner and its in-process work are lost. The wait is a
/// bounded poll on the observable state - cluster membership declaring the silo
/// dead, or the shortened heartbeat lease lapsing - never a fixed sleep.
/// </remarks>
[TestFixture]
[Category("Chaos")]
public sealed class LatticeOperationSiloLossChaosTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task An_operation_whose_runner_silo_is_killed_reads_as_failed_from_a_survivor()
    {
        var secondary = _cluster.SecondarySilos[0];
        var secondaryRunner = ((InProcessSiloHandle)secondary).SiloHost.Services
            .GetRequiredService<LatticeOperationRunner>();
        var primaryRunner = ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services
            .GetRequiredService<LatticeOperationRunner>();

        var operationId = LatticeOperationKey.NewId();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        await secondaryRunner.StartAsync(
            new LatticeOperationStart
            {
                TenantId = "default",
                OperationId = operationId,
                Kind = "chaos.test",
                TreeIds = ["chaos-tree"],
                Phases = ["Working"],
            },
            async (progress, ct) =>
            {
                await progress.ReportAsync("Working", 3, 10, "units");
                started.SetResult();
                return await never.Task.WaitAsync(ct);
            },
            static _ => LatticeOperationCompletion.Succeeded());
        await started.Task;

        await TestPoll.UntilAsync(
            async () => (await primaryRunner.GetAsync("default", operationId))?.State == LatticeOperationState.Running,
            "the survivor to observe the running operation");

        await _cluster.KillSiloAsync(secondary);

        await TestPoll.UntilAsync(
            async () => (await primaryRunner.GetAsync("default", operationId))?.State == LatticeOperationState.Failed,
            "the operation of the killed silo to be failed rather than left running",
            timeout: TimeSpan.FromMinutes(2),
            cadence: TimeSpan.FromMilliseconds(250));

        var record = await primaryRunner.GetAsync("default", operationId);
        Assert.Multiple(() =>
        {
            Assert.That(record!.FailureReason, Does.Contain("not supported"));
            Assert.That(record.CompletedUnits, Is.EqualTo(3), "Progress made before the loss is kept.");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new Orleans.Lattice.Tests.BPlusTree.PublicApiContract.ProcessScopeMemoryGrainStorage()));
            siloBuilder.UseInMemoryReminderService();

            // A short lease is the fallback when membership is slow to declare
            // the killed silo dead; either path must end in Failed.
            siloBuilder.Services.Configure<LatticeOperationOptions>(o =>
            {
                o.HeartbeatInterval = TimeSpan.FromSeconds(1);
                o.HeartbeatLease = TimeSpan.FromSeconds(15);
            });
        }
    }
}
