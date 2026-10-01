using System.Collections.Concurrent;
using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #4123, end to end on a real silo scheduler: a schema-remediation status
/// read must answer while a remediation run holds the coordinator's turn. Before
/// the fix <see cref="ILatticeSchemaRemediationGrain.StartAsync"/> drove the whole
/// run inside one non-reentrant turn and <see cref="ILatticeSchemaRemediationGrain.GetStatusAsync"/>
/// queued behind it, so the Explorer's Schema operation page timed out instead of
/// showing progress.
/// <para>
/// The run is pinned deterministically: an outgoing-call filter holds the build
/// phase's first write into the destination tree on a
/// <see cref="TaskCompletionSource"/> before it is sent, so no response timeout
/// ends the turn and the coordinator sits inside
/// <c>BuildDestinationAsync</c> for as long as the test chooses, and releases it
/// only after the status read has answered, so <see cref="InterleaveProbe"/>
/// decides the claim without timing it (issue #4142).
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationStatusInterleaveIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        BuildWriteGate.ReleaseAll();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_status_read_issued_while_a_remediation_build_is_held_answers_while_held_with_the_build_phase()
    {
        var treeId = $"status-interleave-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("{\"v\":1}"));
        await tree.SetAsync("k2", Encoding.UTF8.GetBytes("{\"v\":2}"));
        await tree.SetAsync("k3", Encoding.UTF8.GetBytes("{\"v\":3}"));

        var remediation = _cluster.GrainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId);
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
        var hold = BuildWriteGate.Arm(treeId);
        Task<LatticeSchemaRemediationReport>? start = null;
        LatticeSchemaRemediationReport during;
        try
        {
            start = remediation.StartAsync(LatticeValueTransform.Passthrough(), policy);
            await hold.Entered.Task.WaitAsync(InterleaveProbe.HangBound);

            during = await InterleaveProbe.AnswersWhileHeldAsync(remediation.GetStatusAsync(), hold.Release.Task,
                "the status read");

            Assert.That(start.IsCompleted, Is.False,
                "precondition: the remediation run was still held when the status read answered");
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        var report = await start!.WaitAsync(TimeSpan.FromSeconds(30));
        var after = await remediation.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(during.InProgress, Is.True);
            Assert.That(during.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build));
            Assert.That(during.ScannedCount, Is.EqualTo(3), "the durable dry-run count is published to the read");
            Assert.That(during.OperationId, Is.Not.Null);
            Assert.That(report.Succeeded, Is.True);
            Assert.That(report.OperationId, Is.EqualTo(during.OperationId));
            Assert.That(after, Is.EqualTo(report));
        });
    }

    /// <summary>
    /// Holds the first <see cref="ILattice.SetAsync"/> into an armed tree's
    /// remediation destination (<c>{treeId}/remediated/{operationId}</c>) until the
    /// test releases it, keeping the coordinator inside its build phase.
    /// </summary>
    private sealed class BuildWriteGate : IOutgoingGrainCallFilter
    {
        private const string DestinationInfix = "/remediated/";

        private static readonly ConcurrentDictionary<string, Hold> Holds = new(StringComparer.Ordinal);

        internal static Hold Arm(string treeId) => Holds.GetOrAdd(treeId, static _ => new Hold());

        internal static void ReleaseAll()
        {
            foreach (var hold in Holds.Values) hold.Release.TrySetResult();
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.DeclaringType == typeof(ILattice)
                && context.MethodName == nameof(ILattice.SetAsync)
                && context.TargetId.Key.ToString() is { } key
                && key.IndexOf(DestinationInfix, StringComparison.Ordinal) is var at and > 0
                && Holds.TryGetValue(key[..at], out var hold))
            {
                hold.Entered.TrySetResult();
                await hold.Release.Task;
            }

            await context.Invoke();
        }
    }

    private sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOutgoingGrainCallFilter<BuildWriteGate>();
        }
    }
}
