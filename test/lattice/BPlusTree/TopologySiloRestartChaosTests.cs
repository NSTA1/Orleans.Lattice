using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Reliability of the online topology coordinators when a silo restarts while they
/// are mid-flight, with atomic batches written and read throughout. The secondary
/// silo of a two-silo cluster is restarted after the coordinator has started its
/// first migrations, so split, fold and resize coordinators, shard roots and leaves
/// hosted there are torn down mid-phase and reactivated from durable state.
/// <para>
/// Faults a caller may legitimately see while a silo leaves and rejoins are
/// tolerated (see <see cref="IsSiloChurnFault"/>); nothing else is. A batch whose
/// call faulted may or may not have committed, but it must have committed whole,
/// and every batch whose call returned must survive the restart. After the change
/// completes the tree must hold every key at the last committed round and count and
/// scan exactly the universe.
/// </para>
/// <para>
/// Grain state and the write-ahead log are process-scoped and shared by both silos
/// (as in <see cref="MultiSiloRestartChaosTests"/>), so the restart exercises
/// membership churn and reactivation, not the disappearance of storage.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class TopologySiloRestartChaosTests
{
    private static readonly TimeSpan PhaseBudget = TimeSpan.FromSeconds(120);
    private static readonly InMemoryWalStorageProvider WalProvider = new();

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
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

    public enum Change { None, Grow, Shrink, Resize }

    [TestCase(Change.None)]
    [TestCase(Change.Grow)]
    [TestCase(Change.Shrink)]
    [TestCase(Change.Resize)]
    public async Task Atomic_batches_survive_a_silo_restart_in_the_middle_of_a_topology_change(Change change)
    {
        var treeId = $"restart-{change.ToString().ToLowerInvariant()}-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 4, MaxLeafKeys = 4 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        var probe = new AtomicRoundProbe(tree, "restart-tx",
            isToleratedWriteFault: IsSiloChurnFault, isToleratedReadFault: IsSiloChurnFault);
        await probe.SeedAsync();
        probe.StartReaders();

        Func<CancellationToken, Task<bool>> step;
        Func<Task> firstPass;
        int expectedShards;
        switch (change)
        {
            case Change.Grow:
                await tree.ReshardAsync(8);
                step = TopologyDrivers.ReshardStep(_cluster.Client, treeId);
                firstPass = () => _cluster.Client.GetGrain<ITreeReshardGrain>(treeId).RunReshardPassAsync();
                expectedShards = 8;
                break;
            case Change.Shrink:
                await tree.ReshardAsync(2);
                step = TopologyDrivers.ReshardStep(_cluster.Client, treeId);
                firstPass = () => _cluster.Client.GetGrain<ITreeReshardGrain>(treeId).RunReshardPassAsync();
                expectedShards = 2;
                break;
            case Change.None:
                step = _ => Task.FromResult(true);
                firstPass = () => Task.CompletedTask;
                expectedShards = 4;
                break;
            default:
                await _cluster.Client.GetGrain<ITreeResizeGrain>(treeId).ResizeAsync(8, 8);
                step = TopologyDrivers.ResizeStep(_cluster.Client, treeId);
                firstPass = () => Task.CompletedTask;
                expectedShards = 4;
                break;
        }

        var steps = 0;
        var restarted = false;
        var phase = await probe.RunPhaseAsync($"{change} with a silo restart mid-flight", async ct =>
        {
            switch (++steps)
            {
                case 1:
                    // Start the first migrations without driving them, so the
                    // restart lands while they are in flight.
                    await firstPass();
                    return false;
                case 2:
                    await _cluster.RestartSiloAsync(_cluster.SecondarySilos[0]);
                    await _cluster.WaitForLivenessToStabilizeAsync();
                    restarted = true;
                    return false;
                default:
                    return await step(ct);
            }
        }, PhaseBudget, tailRounds: 5, isToleratedStepFault: IsSiloChurnFault);

        await probe.StopReadersAsync();
        var problems = await probe.VerifyQuiescedAsync($"after {change}");
        var shards = await TopologyDrivers.PhysicalShardsAsync(_cluster.Client, treeId);
        TestContext.Out.WriteLine($"{phase}{Environment.NewLine}{probe.Summary()}");

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                $"Atomic visibility violation across a silo restart mid-{change}:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            Assert.That(restarted, Is.True, "precondition: the silo restarted while the change was in flight");
            Assert.That(phase.Completed, Is.True, $"the {change} must complete after the restart");
            Assert.That(phase.RoundsDuringChange, Is.GreaterThan(0));
            Assert.That(probe.RoundsCommitted, Is.GreaterThan(0));
            Assert.That(shards, Has.Count.EqualTo(expectedShards));
        });
    }

    /// <summary>
    /// The faults a caller may see while a silo leaves and rejoins the cluster: a
    /// request to a grain hosted on the departing silo is rejected or times out, its
    /// activation times out while re-placing, or a saga it was coordinating is parked
    /// for resumption on the next activation. Inner exceptions are searched, because a
    /// saga or fan-out wraps the fault of the call that failed.
    /// </summary>
    private static bool IsSiloChurnFault(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException)
        {
            if (e is TimeoutException or SiloUnavailableException or OrleansMessageRejectionException
                or ShardActivationTimeoutException or LatticeShuttingDownException)
                return true;
            if (e is InvalidOperationException && e.Message.Contains("rolled back", StringComparison.Ordinal))
                return true;
        }

        return ex is AggregateException agg && agg.InnerExceptions.Any(IsSiloChurnFault);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Both registrations precede AddLattice so they win its TryAdd; see
            // MultiSiloRestartChaosTests for why a per-silo WAL or grain store
            // would erase state on restart rather than exercise reactivation.
            siloBuilder.AddWalStorage(_ => WalProvider);
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new PublicApiContract.ProcessScopeMemoryGrainStorage()));
            siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = 0);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
