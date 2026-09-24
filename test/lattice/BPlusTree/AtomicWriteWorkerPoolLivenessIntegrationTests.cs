using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Liveness regression for concurrent atomic sagas on one tree. An atomic entry
/// point on <see cref="ILattice"/> awaits its saga for the saga's whole
/// duration, and the saga calls back through <see cref="ILattice"/> for its
/// routing and prepare legs. Those callbacks land in the same per-silo
/// <c>LatticeGrain</c> stateless-worker pool (32 workers), so if the entry
/// points did not interleave, 32 concurrent sagas would park every worker and
/// the pool would self-deadlock until the response timeout. Before the fix, 31
/// concurrent sagas completed and 33 hung.
/// </summary>
[TestFixture]
[Category("Integration")]
public class AtomicWriteWorkerPoolLivenessIntegrationTests
{
    private static readonly TimeSpan ClusterResponseTimeout = TimeSpan.FromSeconds(45);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    /// <summary>
    /// Well below the 45 s response timeout, so a regression fails the test
    /// rather than waiting for Orleans to time every stuck call out.
    /// </summary>
    private static readonly TimeSpan LivenessBound = TimeSpan.FromSeconds(30);

    private const int KeySpace = 32;

    // 31 sits just under the 32-worker pool, 33 just over it (the smallest count
    // that hung before the fix), and 96 is three times the pool.
    [TestCase(31)]
    [TestCase(33)]
    [TestCase(96)]
    public async Task Concurrent_two_key_sagas_beyond_the_worker_pool_all_complete(int sagaCount)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"atomic-pool-{Guid.NewGuid():N}");
        await tree.SetAsync("warm", Encoding.UTF8.GetBytes("w"));

        var sagas = new Task[sagaCount];
        for (var i = 0; i < sagaCount; i++)
        {
            sagas[i] = tree.SetManyAtomicAsync(TwoKeyBatch(i));
        }

        await AssertAllCompleteWithinBoundAsync(sagas);
    }

    [Test]
    public async Task Concurrent_guarded_sagas_beyond_the_worker_pool_all_commit()
    {
        const int sagaCount = 48;
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"atomic-pool-where-{Guid.NewGuid():N}");

        // A guarded saga treats a key with no live pre-saga value as a miss, so
        // seed every key the batches touch with a JSON document first.
        var seed = new List<KeyValuePair<string, byte[]>>(KeySpace);
        for (var k = 0; k < KeySpace; k++)
        {
            seed.Add(new($"k{k}", Json(-1)));
        }

        await tree.SetManyAsync(seed);

        // 1 == 1 matches every valid JSON document, so every saga must commit.
        var alwaysTrue = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Const(LatticeConstant.Integer(1)),
            LatticePredicateNode.Const(LatticeConstant.Integer(1)));

        var sagas = new Task<AtomicWriteOutcome>[sagaCount];
        for (var i = 0; i < sagaCount; i++)
        {
            sagas[i] = tree.SetManyAtomicWhereAsync(TwoKeyBatch(i), alwaysTrue);
        }

        await AssertAllCompleteWithinBoundAsync(sagas);
        Assert.That(sagas.Select(s => s.Result), Is.All.EqualTo(AtomicWriteOutcome.Committed));
    }

    // Two keys drawn from disjoint halves of the key space, so concurrent sagas
    // overlap on keys and contend for them as the rig workload does.
    private static List<KeyValuePair<string, byte[]>> TwoKeyBatch(int i) =>
    [
        new($"k{i % (KeySpace / 2)}", Json(i)),
        new($"k{(i + 7) % (KeySpace / 2) + (KeySpace / 2)}", Json(i)),
    ];

    private static byte[] Json(int value) => Encoding.UTF8.GetBytes($"{{\"v\":{value}}}");

    private static async Task AssertAllCompleteWithinBoundAsync(Task[] sagas)
    {
        var all = Task.WhenAll(sagas);
        var finished = await Task.WhenAny(all, Task.Delay(LivenessBound));
        var stuck = sagas.Count(t => !t.IsCompleted);
        Assert.That(finished, Is.SameAs(all),
            $"{stuck} of {sagas.Length} concurrent atomic sagas made no progress within {LivenessBound.TotalSeconds} s.");
        await all;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.Configure<SiloMessagingOptions>(o =>
            {
                o.ResponseTimeout = ClusterResponseTimeout;
                o.SystemResponseTimeout = ClusterResponseTimeout;
            });
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.Services.Configure<ClientMessagingOptions>(
                o => o.ResponseTimeout = ClusterResponseTimeout);
        }
    }
}
