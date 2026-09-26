using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #3592, end to end against the real Orleans reminder service. A new
/// <see cref="LatticeGrain"/> worker must not move a tree's compaction
/// schedule. Before the fix its first write re-registered the reminder, which
/// Orleans treats as a fresh schedule starting one period after the call, so a
/// tree that kept getting new workers - after restarts, idle gaps or load
/// spikes - never reached its compaction pass.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TombstoneCompactionReminderPhaseIntegrationTests
{
    private const string ReminderName = "tombstone-compaction";

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
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_new_lattice_worker_does_not_move_the_compaction_schedule()
    {
        var treeId = $"reminder-phase-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var management = _cluster.Client.GetGrain<IManagementGrain>(0);
        var compactionGrainId = _cluster.Client.GetGrain<ITombstoneCompactionGrain>(treeId).GetGrainId();

        await tree.SetAsync("k0", [0]);

        var registered = await ReadReminderAsync(compactionGrainId);
        Assert.Multiple(() =>
        {
            Assert.That(registered, Is.Not.Null,
                "precondition: the first write must register the compaction reminder.");
            Assert.That(EnsureReminderCalls(treeId), Is.EqualTo(1),
                "precondition: the first write's worker must have asked for the reminder once.");
        });

        // A re-registration after this point would land on a measurably later StartAt.
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        // Retire the worker and write again until a new worker serves a write.
        // A surviving worker has already asked and does not ask again, so a
        // second EnsureReminderAsync call proves the write reached a new worker.
        var write = 0;
        await TestPoll.UntilAsync(
            async () =>
            {
                await management.ForceActivationCollection(TimeSpan.Zero);
                await tree.SetAsync($"k{++write}", [1]);
                return EnsureReminderCalls(treeId) >= 2;
            },
            "a write served by a new LatticeGrain worker",
            timeout: TimeSpan.FromSeconds(30),
            cadence: TimeSpan.FromMilliseconds(200));

        var afterNewWorker = await ReadReminderAsync(compactionGrainId);
        Assert.Multiple(() =>
        {
            Assert.That(afterNewWorker, Is.Not.Null, "the reminder must still be registered.");
            Assert.That(afterNewWorker!.StartAt, Is.EqualTo(registered!.StartAt),
                "a new worker's first write moved the compaction schedule. Each move postpones the next "
                + "pass by a full period, so a tree that keeps getting new workers never compacts.");
            Assert.That(afterNewWorker.Period, Is.EqualTo(registered.Period));
            Assert.That(afterNewWorker.ETag, Is.EqualTo(registered.ETag),
                "the reminder row must not have been rewritten at all.");
        });
    }

    private Task<ReminderEntry> ReadReminderAsync(GrainId grainId)
    {
        var silo = (InProcessSiloHandle)_cluster.Primary;
        var table = silo.SiloHost.Services.GetRequiredService<IReminderTable>();
        return table.ReadRow(grainId, ReminderName);
    }

    private static int EnsureReminderCalls(string treeId) =>
        EnsureReminderCallCounter.Calls.GetValueOrDefault(treeId);

    /// <summary>
    /// Counts <see cref="ITombstoneCompactionGrain.EnsureReminderAsync"/> calls
    /// per tree. <c>GetDetailedGrainStatistics</c> does not list stateless-worker
    /// activations, so this call is the observable that a new worker ran.
    /// </summary>
    private sealed class EnsureReminderCallCounter : IIncomingGrainCallFilter
    {
        internal static readonly ConcurrentDictionary<string, int> Calls = new(StringComparer.Ordinal);

        public Task Invoke(IIncomingGrainCallContext context)
        {
            if (context.MethodName == nameof(ITombstoneCompactionGrain.EnsureReminderAsync))
            {
                Calls.AddOrUpdate(context.TargetId.Key.ToString()!, 1, static (_, count) => count + 1);
            }

            return context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddIncomingGrainCallFilter<EnsureReminderCallCounter>();
        }
    }
}
