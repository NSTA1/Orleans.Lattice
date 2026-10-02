using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests.Chaos;

/// <summary>
/// Issue #4123, silo loss: a tracked remediation whose runner's silo is killed in
/// the middle of its build must read as <see cref="LatticeOperationState.Failed"/>
/// from a survivor, never stay <see cref="LatticeOperationState.Running"/>; and
/// because the remediation itself is durable and sliced, starting the same
/// remediation again from a survivor resumes it from its last recorded slice and
/// cuts the tree over.
/// </summary>
/// <remarks>
/// Grain state and the write-ahead log are shared by every silo in the process,
/// so the kill loses only the runner, its in-process work and the killed silo's
/// activations. The build is pinned by an outgoing-call filter that holds its
/// first destination write, so the kill lands inside the build without timing.
/// Waits are bounded polls on observable state.
/// </remarks>
[TestFixture]
[Category("Chaos")]
public sealed class SchemaOperationSiloLossChaosTests
{
    private static readonly InMemoryWalStorageProvider SharedWal = new();

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
        HeldBuildWrite.Release.TrySetResult();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static IServiceProvider Services(SiloHandle silo) => ((InProcessSiloHandle)silo).SiloHost.Services;

    [Test]
    public async Task A_remediation_whose_runner_silo_is_killed_fails_then_resumes_when_started_again()
    {
        var treeId = $"chaos-remediate-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < 4; i++)
        {
            await tree.SetAsync($"k{i}", Encoding.UTF8.GetBytes($"{{\"v\":{i}}}"));
        }

        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("status", LatticeValueTransform.Const(LatticeConstant.Text("ok"))));
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
        var secondary = _cluster.SecondarySilos[0];
        var survivor = Services(_cluster.Primary).GetRequiredService<SchemaOperationService>();

        HeldBuildWrite.Arm(treeId);
        var operationId = LatticeOperationKey.NewId();
        await Services(secondary).GetRequiredService<SchemaOperationService>()
            .StartRemediationAsync("default", operationId, treeId, transform, policy);
        await HeldBuildWrite.Entered.Task.WaitAsync(InterleaveProbe.HangBound);
        await TestPoll.UntilAsync(
            async () => (await survivor.Runner.GetAsync("default", operationId)) is { Phase: SchemaOperationPhases.Build },
            "the survivor to observe the remediation in its build");

        await _cluster.KillSiloAsync(secondary);
        HeldBuildWrite.Release.TrySetResult();

        await TestPoll.UntilAsync(
            async () => (await survivor.Runner.GetAsync("default", operationId))?.State == LatticeOperationState.Failed,
            "the remediation of the killed runner to read as failed rather than running",
            timeout: TimeSpan.FromMinutes(2),
            cadence: TimeSpan.FromMilliseconds(250));
        var lost = await survivor.Runner.GetAsync("default", operationId);

        // Starting it again joins the remediation still recorded in flight and
        // drives it from its last slice. Membership may still be settling, so a
        // start that fails on a transient fault is retried with a fresh id.
        LatticeOperationRecord? resumed = null;
        await TestPoll.UntilAsync(
            async () =>
            {
                var retryId = LatticeOperationKey.NewId();
                var launch = await survivor.StartRemediationAsync("default", retryId, treeId, transform, policy);
                try
                {
                    await launch.Completion!;
                }
                catch (Exception)
                {
                    // Recorded on the operation; the poll starts another.
                }

                resumed = await survivor.Runner.GetAsync("default", retryId);
                return resumed?.State == LatticeOperationState.Succeeded;
            },
            "a fresh start of the same remediation to resume it and cut the tree over",
            timeout: TimeSpan.FromMinutes(2),
            cadence: TimeSpan.FromMilliseconds(500));

        var k0 = Encoding.UTF8.GetString(await tree.GetAsync("k0") ?? []);
        Assert.Multiple(() =>
        {
            Assert.That(lost!.Phase, Is.EqualTo(SchemaOperationPhases.Build), "the failure keeps the phase it was lost in");
            Assert.That(resumed!.Result[SchemaOperationResultKeys.ValuesProcessed], Is.EqualTo("4"));
            Assert.That(resumed.Result[SchemaOperationResultKeys.RemediationOperationId], Is.EqualTo(operationId),
                "the restart drives the remediation the lost operation accepted");
            Assert.That(k0, Does.Contain("status"), "the tree serves the remediated values");
        });
    }

    /// <summary>Holds the first destination write of the armed tree's remediation.</summary>
    private sealed class HeldBuildWrite : IOutgoingGrainCallFilter
    {
        private static volatile string? _armed;

        public static TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static void Arm(string treeId) => _armed = treeId;

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (_armed is { } armed
                && context.InterfaceMethod?.DeclaringType == typeof(ILattice)
                && context.MethodName == nameof(ILattice.SetAsync)
                && context.TargetId.Key.ToString() is { } key
                && key.StartsWith(armed + "/remediated/", StringComparison.Ordinal))
            {
                Entered.TrySetResult();
                await Release.Task;
            }

            await context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name, (_, _) => new ProcessScopeSchemaGrainStorage()));
            siloBuilder.AddWalStorage(_ => SharedWal);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddOutgoingGrainCallFilter<HeldBuildWrite>();
            siloBuilder.Services.Configure<LatticeOperationOptions>(o =>
            {
                o.HeartbeatInterval = TimeSpan.FromSeconds(1);
                o.HeartbeatLease = TimeSpan.FromSeconds(15);
            });
        }
    }
}
