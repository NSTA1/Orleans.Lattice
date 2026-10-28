using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
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
/// Waits are bounded polls on observable state. A second case injects one lost
/// status response after the kill, proving the observation retries without
/// treating the timeout as a failed operation or masking a permanently stuck one.
/// The restart likewise retries a start that times out while the cluster
/// reconverges after the kill, within a bounded poll that still fails if the
/// remediation never resumes.
/// </remarks>
[TestFixture]
[Category("Chaos")]
public sealed class SchemaOperationSiloLossChaosTests
{
    private static readonly InMemoryWalStorageProvider SharedWal = new();

    private TestCluster _cluster = null!;

    [SetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        HeldBuildWrite.Release.TrySetResult();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static IServiceProvider Services(SiloHandle silo) => ((InProcessSiloHandle)silo).SiloHost.Services;

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_remediation_whose_runner_silo_is_killed_fails_then_resumes_when_started_again(
        bool loseFirstStatusResponse)
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
        if (loseFirstStatusResponse)
        {
            HeldBuildWrite.LoseNextStatusResponse(operationId);
        }

        var lost = await SchemaOperationStatusPoll.WaitForFailedAsync(
            () => survivor.Runner.GetAsync("default", operationId));
        Assert.That(HeldBuildWrite.LostStatusResponses, Is.EqualTo(loseFirstStatusResponse ? 1 : 0),
            "The controlled response-loss case must exercise the timeout branch.");

        // Starting it again joins the remediation still recorded in flight and
        // drives it from its last slice. While the cluster reconverges after the
        // kill, addressing a fresh operation grain can stall until the response
        // timeout, so a start that fails on a transient fault - the start call
        // itself as well as its completion - is retried with a fresh id.
        LatticeOperationRecord? resumed = null;
        await TestPoll.UntilAsync(
            async () =>
            {
                var retryId = LatticeOperationKey.NewId();
                try
                {
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
                }
                catch (Exception ex) when (IsTransientSiloLossFault(ex))
                {
                    TestContext.Progress.WriteLine($"Restart attempt hit a transient silo-loss fault; retrying within the poll deadline: {ex.Message}");
                    return false;
                }

                return resumed?.State == LatticeOperationState.Succeeded;
            },
            "a fresh start of the same remediation to resume it and cut the tree over",
            timeout: TimeSpan.FromMinutes(2),
            cadence: TimeSpan.FromMilliseconds(500));

        Assert.Multiple(() =>
        {
            Assert.That(lost.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(lost.Phase, Is.EqualTo(SchemaOperationPhases.Build), "the failure keeps the phase it was lost in");
            Assert.That(resumed!.Result[SchemaOperationResultKeys.ValuesProcessed], Is.EqualTo("4"));
            Assert.That(resumed.Result[SchemaOperationResultKeys.RemediationOperationId], Is.EqualTo(operationId),
                "the restart drives the remediation the lost operation accepted");
        });
        for (var i = 0; i < 4; i++)
        {
            var value = await tree.GetAsync($"k{i}");
            Assert.That(value, Is.Not.Null, $"Remediation must retain k{i}.");
            using var json = JsonDocument.Parse(value!);
            Assert.Multiple(() =>
            {
                Assert.That(json.RootElement.GetProperty("v").GetInt32(), Is.EqualTo(i));
                Assert.That(json.RootElement.GetProperty("status").GetString(), Is.EqualTo("ok"));
                Assert.That(json.RootElement.EnumerateObject().Count(), Is.EqualTo(2));
            });
        }
    }

    private static bool IsTransientSiloLossFault(Exception ex)
        => ex is TimeoutException or SiloUnavailableException or OrleansMessageRejectionException;

    /// <summary>Holds the first destination write of the armed tree's remediation.</summary>
    private sealed class HeldBuildWrite : IOutgoingGrainCallFilter
    {
        private static volatile string? _armed;
        private static string? _lostStatusKey;

        public static TaskCompletionSource Entered { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static TaskCompletionSource Release { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static int LostStatusResponses { get; private set; }

        public static void Arm(string treeId)
        {
            _armed = treeId;
            _lostStatusKey = null;
            LostStatusResponses = 0;
            Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public static void LoseNextStatusResponse(string operationId)
            => _lostStatusKey = LatticeOperationKey.For("default", operationId);

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            var lostStatusKey = Volatile.Read(ref _lostStatusKey);
            if (lostStatusKey is not null
                && context.InterfaceMethod?.DeclaringType == typeof(ILatticeOperationGrain)
                && context.MethodName == nameof(ILatticeOperationGrain.GetAsync)
                && context.TargetId.Key.ToString() is { } statusKey
                && string.Equals(statusKey, lostStatusKey, StringComparison.Ordinal)
                && ReferenceEquals(Interlocked.CompareExchange(ref _lostStatusKey, null, lostStatusKey), lostStatusKey))
            {
                LostStatusResponses++;
                throw new TimeoutException("Controlled loss of the first post-kill operation-status response (issue #4842).");
            }

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
