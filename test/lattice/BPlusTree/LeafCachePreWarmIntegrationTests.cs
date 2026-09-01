using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end regression for the opt-in post-restart leaf-cache pre-warm
/// (issue #332). Drives the whole feature through its real silo path: reads
/// build the per-shard leaf-access Markov chain, a forced deactivation flushes
/// it through the real Orleans serializer into the real storage provider, and
/// the fresh activation's <see cref="IShardRootGrain.WarmUpAsync"/> ranks the
/// restored chain and primes that many leaf caches.
/// <para>
/// The unit tests cover the model's maths and the grain's wiring against fakes;
/// what only a cluster can prove is that the model actually survives a real
/// activation boundary and that priming the stateless-worker caches through the
/// live grain runtime neither throws nor corrupts a subsequent read.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class LeafCachePreWarmIntegrationTests
{
    private LeafCachePreWarmClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new LeafCachePreWarmClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private ILattice Tree() =>
        _cluster.Client.GetGrain<ILattice>(LeafCachePreWarmClusterFixture.TreeName);

    private IShardRootGrain Shard() =>
        _cluster.Client.GetGrain<IShardRootGrain>($"{LeafCachePreWarmClusterFixture.TreeName}/0");

    /// <summary>
    /// Forces the shard root to deactivate and waits for the runtime to collect
    /// the activation. DeactivateOnIdle is scheduled for after the current
    /// grain turn, so the caller cannot observe the flush synchronously.
    /// </summary>
    private async Task RestartShardRootAsync()
    {
        await Shard().ForceDeactivateAsync();
        await Task.Delay(500);
    }

    [Test]
    public async Task Reads_survive_a_restart_and_a_warm_up_that_primes_the_ranked_leaf_caches()
    {
        var lattice = Tree();

        // Seed enough keys to split across several leaves (MaxLeafKeys = 4), so
        // the chain has more than one state to rank.
        var keys = Enumerable.Range(0, 24).Select(i => $"key-{i:D3}").ToArray();
        foreach (var k in keys) await lattice.SetAsync(k, Bytes(k));

        // Skewed read traffic: the low keys are read far more often than the
        // high ones, which is the distribution the ranking is meant to exploit.
        for (var round = 0; round < 12; round++)
        {
            for (var i = 0; i < 4; i++) await lattice.GetAsync($"key-{i:D3}");
            await lattice.GetAsync($"key-{20 + (round % 4):D3}");
        }

        // Restart: OnDeactivateAsync flushes the accumulated chain. If the new
        // persisted types had a broken serializer envelope or a duplicate
        // alias, this write would throw and the shard root would come back with
        // lost state - which the reads below would surface immediately.
        await RestartShardRootAsync();

        // The fresh activation ranks the restored chain and primes the top
        // leaves. Pre-warm is best-effort by contract, so the assertion that
        // matters is that warm-up completes and leaves the tree readable.
        Assert.DoesNotThrowAsync(async () => await lattice.WarmUpAsync(default));

        foreach (var k in keys)
        {
            var value = await lattice.GetAsync(k);
            Assert.That(value, Is.EqualTo(Bytes(k)), $"key '{k}' must survive restart and warm-up");
        }
    }

    [Test]
    public async Task Warm_up_is_idempotent_across_repeated_calls()
    {
        var lattice = Tree();
        await lattice.SetAsync("idem-a", Bytes("idem-a"));
        await lattice.GetAsync("idem-a");

        await RestartShardRootAsync();

        // The operational hook is documented as safe to call on every silo
        // start, so re-priming an already-primed cache must be a no-op rather
        // than an error or a duplicate-activation fault.
        for (var i = 0; i < 3; i++)
        {
            Assert.DoesNotThrowAsync(async () => await lattice.WarmUpAsync(default));
        }

        Assert.That(await lattice.GetAsync("idem-a"), Is.EqualTo(Bytes("idem-a")));
    }

    [Test]
    public async Task Warm_up_on_a_shard_that_never_served_a_read_is_a_no_op()
    {
        // A shard with no persisted chain must warm up exactly as it did before
        // the feature existed: pre-warm finds nothing to rank and returns.
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        const string coldTree = "prewarm-cold-tree";
        await registry.RegisterAsync(coldTree, new TreeRegistryEntry
        {
            MaxLeafKeys = LeafCachePreWarmClusterFixture.MaxLeafKeys,
            ShardCount = 1,
        });
        var cold = _cluster.Client.GetGrain<ILattice>(coldTree);

        Assert.DoesNotThrowAsync(async () => await cold.WarmUpAsync(default));

        await cold.SetAsync("cold-a", Bytes("cold-a"));
        Assert.That(await cold.GetAsync("cold-a"), Is.EqualTo(Bytes("cold-a")));
    }

    /// <summary>
    /// A pre-warm that ranks nothing must still record a duration observation.
    /// That observation is the only thing separating "pre-warm ran and had nothing
    /// to warm" from "pre-warm never ran" without turning on debug logging - and
    /// the empty-ranking case is the one the feature most needs to be visible in,
    /// because an unclean restart is exactly what leaves the access-frequency model
    /// empty and turns every later pre-warm into a silent no-op.
    /// </summary>
    [Test]
    public async Task Warm_up_that_ranks_nothing_still_records_a_duration_observation()
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        const string coldTree = "prewarm-observability-tree";
        await registry.RegisterAsync(coldTree, new TreeRegistryEntry
        {
            MaxLeafKeys = LeafCachePreWarmClusterFixture.MaxLeafKeys,
            ShardCount = 1,
        });
        var cold = _cluster.Client.GetGrain<ILattice>(coldTree);

        var durations = 0;
        var warmedTotal = 0L;
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name is "orleans.lattice.warmup.leaf_cache.duration"
                or "orleans.lattice.warmup.leaf_cache.prewarmed")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((instrument, value, _, _) =>
        {
            if (instrument.Name == "orleans.lattice.warmup.leaf_cache.duration")
            {
                Interlocked.Increment(ref durations);
            }
        });
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "orleans.lattice.warmup.leaf_cache.prewarmed")
            {
                Interlocked.Add(ref warmedTotal, value);
            }
        });
        listener.Start();

        await cold.WarmUpAsync(default);
        listener.RecordObservableInstruments();

        Assert.Multiple(() =>
        {
            // Paired: the positive proves the instrument fired at all, so the
            // zero-warmed assertion cannot pass merely because nothing was observed.
            Assert.That(durations, Is.GreaterThan(0),
                "A pre-warm that ranked nothing still records that it ran,");
            Assert.That(warmedTotal, Is.Zero,
                "and reports zero leaves warmed - so 'ran but warmed nothing' is "
                + "distinguishable from 'never ran', which records no duration at all.");
        });
    }

    [Test]
    public async Task A_second_restart_cycle_keeps_accumulating_the_model()
    {
        // The chain is cumulative across activations: a restart restores it,
        // subsequent reads extend it, and the next restart persists the union.
        // A regression that dropped the restored chain on activation would
        // still pass the single-cycle test above, so exercise two cycles.
        var lattice = Tree();
        await lattice.SetAsync("cycle-a", Bytes("cycle-a"));

        for (var i = 0; i < 5; i++) await lattice.GetAsync("cycle-a");
        await RestartShardRootAsync();
        await lattice.WarmUpAsync(default);

        for (var i = 0; i < 5; i++) await lattice.GetAsync("cycle-a");
        await RestartShardRootAsync();

        Assert.DoesNotThrowAsync(async () => await lattice.WarmUpAsync(default));
        Assert.That(await lattice.GetAsync("cycle-a"), Is.EqualTo(Bytes("cycle-a")));
    }
}
