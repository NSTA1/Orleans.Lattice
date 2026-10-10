using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Atomic visibility of <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
/// across silo restarts, with no topology change in flight. A chain of atomic
/// batches over one key universe runs while continuous readers poll the whole
/// universe and the secondary silo of a two-silo cluster is restarted
/// repeatedly. Every poll must see every key at one round, and every batch whose
/// call returned must stay visible.
/// <para>
/// A restart parks the saga in flight on the departing silo (its caller sees
/// <see cref="LatticeShuttingDownException"/>) and reactivates leaves from the
/// write-ahead log, which produces two shapes the steady state never does: a
/// leaf holding a long-undecided prepare beside every later saga's prepare on
/// the same keys, and a leaf whose activation replay drains one saga's commit
/// after it has already absorbed a later saga's prepare. Both used to tear
/// batches (see <c>docs/lattice/atomic-writes.md</c>, "Silo restarts"); the
/// deterministic regressions are
/// <c>BPlusLeafGrainTests.GetManyAsync_surfaces_the_committed_saga_when_an_older_in_flight_prepare_also_covers_the_key</c>
/// and
/// <c>BPlusLeafGrainTests.A_commit_after_activation_lands_over_an_earlier_saga_drained_in_replay_pass_two</c>.
/// </para>
/// <para>
/// Grain state and the write-ahead log are process-scoped and shared by both
/// silos (as in <see cref="MultiSiloRestartChaosTests"/>), so a restart exercises
/// membership churn and reactivation, not the disappearance of storage.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class AtomicWriteSiloRestartChaosTests
{
    private const int Restarts = 3;
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

    [Test]
    public async Task Atomic_batches_stay_all_or_nothing_across_silo_restarts()
    {
        var treeId = $"atomic-restart-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 4, MaxLeafKeys = 4 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        var probe = new AtomicRoundProbe(tree, "restart-tx",
            isToleratedWriteFault: IsSiloChurnFault, isToleratedReadFault: IsSiloChurnFault);
        await probe.SeedAsync();
        probe.StartReaders();

        var restarts = 0;
        var phase = await probe.RunPhaseAsync("silo restarts", async ct =>
        {
            // The first restart lands once the first batch is in flight, while
            // the seeded leaves are still spread across both silos. A restart
            // leaves every grain on the surviving silo, so later restarts wait
            // for new activations to land on the rejoined secondary again.
            await Task.Delay(TimeSpan.FromMilliseconds(restarts == 0 ? 50 : 2000), ct);
            await _cluster.RestartSiloAsync(_cluster.SecondarySilos[0]);
            await _cluster.WaitForLivenessToStabilizeAsync();
            return ++restarts >= Restarts;
        }, PhaseBudget, tailRounds: 10, isToleratedStepFault: IsSiloChurnFault);

        await probe.StopReadersAsync();
        var problems = await probe.VerifyQuiescedAsync("after silo restarts");
        TestContext.Out.WriteLine($"{phase}{Environment.NewLine}{probe.Summary()}");

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic visibility violation across a silo restart:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            Assert.That(restarts, Is.EqualTo(Restarts), "precondition: every restart ran");
            Assert.That(phase.Completed, Is.True);
            Assert.That(probe.RoundsCommitted, Is.GreaterThan(Restarts));
            Assert.That(probe.Polls, Is.GreaterThan(0));
        });
    }

    /// <summary>
    /// The faults a caller may see while a silo leaves and rejoins the cluster: a
    /// request to a grain hosted on the departing silo is rejected or times out, its
    /// activation times out while re-placing, or a saga it was coordinating is parked
    /// for resumption on the next activation. Inner exceptions are searched, because a
    /// saga or fan-out wraps the fault of the call that failed.
    /// </summary>
    internal static bool IsSiloChurnFault(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException)
        {
            if (e is TimeoutException or SiloUnavailableException or OrleansMessageRejectionException
                or ShardActivationTimeoutException or LatticeShuttingDownException)
                return true;
            if (e is InvalidOperationException && e.Message.Contains("rolled back", StringComparison.Ordinal))
                return true;
            // Orleans' InMemoryReminderTable disables access at ApplicationServices stop.
            if (e is InvalidOperationException
                && string.Equals(e.Message, "The reminder service is not currently available.", StringComparison.Ordinal))
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
