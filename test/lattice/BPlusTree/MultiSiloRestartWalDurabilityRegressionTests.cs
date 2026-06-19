using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Deterministic regression coverage for the same defect surface the
/// flaky <see cref="MultiSiloRestartChaosTests"/> picks up at random
/// under load: a two-silo <see cref="TestCluster"/> seeded with a known
/// universe, the secondary silo restarted with no concurrent writers,
/// and a full universe re-read after the restart. With no writers in
/// flight the only thing that can change is what the restart erases,
/// so any post-restart "key missing" surfaces here as a deterministic
/// failure rather than a load-dependent flake.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this regression guards.</b> Per-key leaf entries are not
/// persisted in the leaf grain row - they live only in the
/// per-activation <c>LeafEntryCache</c> plus the WAL, and the
/// activation-time materialiser rebuilds the cache from the WAL on
/// every reactivation. The default <c>IWalStorageProvider</c>
/// installed by <c>AddLattice</c> is a per-silo singleton
/// (<c>InMemoryWalStorageProvider</c>) that dies with the silo, so a
/// multi-silo test fixture that restarts a silo mid-test must wire a
/// process-scope WAL provider via <c>siloBuilder.AddWalStorage(_ =&gt;
/// shared)</c> or every <c>WalShardGrain</c> that had been hosted on
/// the dying silo reactivates against an empty WAL and the leaves
/// silently surface every key under those partitions as "missing".
/// Because <c>WalShardGrain</c> uses Orleans default placement
/// (<c>RandomPlacement</c>) and <c>LatticeOptions.WalPartitions</c>
/// defaults to 8, the missing-key fraction is roughly the secondary
/// silo's WAL ownership fraction on the run, fingerprinted at CI as a
/// flaky "envelope / presence violation" volume in the chaos test.
/// </para>
/// <para>
/// <b>Reproduction.</b> Remove <c>siloBuilder.AddWalStorage(_ =&gt;
/// WalProvider)</c> from <see cref="SiloConfigurator"/> and the
/// universe-preservation assertion below fails on most runs of this
/// fixture - a deterministic, fast (about three seconds), no-writer
/// repro of the chaos-test flake.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Integration")]
public class MultiSiloRestartWalDurabilityRegressionTests
{
    private TestCluster _cluster = null!;

    private const int UniverseSize = 64;

    /// <summary>
    /// Fixture-scope shared <see cref="InMemoryWalStorageProvider"/>.
    /// Wired into every silo via <see cref="SiloConfigurator"/> so the
    /// WAL state survives <see cref="TestCluster.RestartSiloAsync(SiloHandle)"/>.
    /// Removing this and the matching <c>AddWalStorage</c> wire-up is
    /// the regression this fixture exists to surface.
    /// </summary>
    private static readonly InMemoryWalStorageProvider WalProvider = new();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
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

    private static string KeyOf(int i) => $"silor-{i:D5}";

    private static byte[] ValueOf(int i) => Encoding.UTF8.GetBytes($"v-{i}");

    [Test]
    public async Task Secondary_silo_restart_preserves_universe_when_no_concurrent_writers()
    {
        var treeId = $"silor-regress-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 8,
            ShardCount = 2,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        // Seed the universe. No concurrent writers, no chaos.
        for (int i = 0; i < UniverseSize; i++)
        {
            await tree.SetAsync(KeyOf(i), ValueOf(i));
        }

        // Sanity: the universe is fully readable BEFORE the restart.
        // This catches a different defect (write path lost a key) so we
        // can attribute any post-restart loss to the restart itself.
        var preCount = await tree.CountAsync();
        Assert.That(preCount, Is.EqualTo(UniverseSize),
            "Pre-restart CountAsync must match the seeded universe size " +
            "- a mismatch here indicates a write-path defect unrelated to silo restart.");

        var preMissing = new List<string>();
        for (int i = 0; i < UniverseSize; i++)
        {
            var v = await tree.GetAsync(KeyOf(i));
            if (v is null) preMissing.Add(KeyOf(i));
        }
        Assert.That(preMissing, Is.Empty,
            "Pre-restart full-universe read must return every seeded key.");

        // Restart the secondary silo. No writers are in flight, so the
        // only state change between the seed and the post-restart read
        // is whatever the silo lifecycle destroys (or doesn't preserve).
        var secondary = _cluster.SecondarySilos.FirstOrDefault();
        Assert.That(secondary, Is.Not.Null,
            "Test cluster must have at least one SecondarySilo to restart.");
        await _cluster.RestartSiloAsync(secondary!);

        // Allow Orleans time to settle: WalShardGrain / leaf activations
        // whose home silo was the secondary need to reactivate on the
        // primary before the read fan-out can land. A fixed sleep is not
        // enough on a slow/contended CI runner - reactivation can take
        // longer than any single hard-coded delay, and the first read
        // then surfaces a transient ShardActivationTimeoutException that
        // has nothing to do with durability. Instead we poll the reads on
        // a generous deadline, tolerating only the transient reactivation
        // exceptions (a genuinely lost key returns null, which is NOT
        // retried and still fails the durability assertions below).
        var settleDeadline = TimeSpan.FromSeconds(60);

        // Post-restart invariants. Universe must be intact.
        var postCount = await ReadWithReactivationRetryAsync(
            () => tree.CountAsync(), settleDeadline);
        var postMissing = new List<string>();
        var postBadValue = new List<string>();
        for (int i = 0; i < UniverseSize; i++)
        {
            var v = await ReadWithReactivationRetryAsync(
                () => tree.GetAsync(KeyOf(i)), settleDeadline);
            if (v is null)
            {
                postMissing.Add(KeyOf(i));
            }
            else if (!v.SequenceEqual(ValueOf(i)))
            {
                postBadValue.Add(
                    $"{KeyOf(i)}: expected '{Encoding.UTF8.GetString(ValueOf(i))}' " +
                    $"but got '{Encoding.UTF8.GetString(v)}'");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(postMissing, Is.Empty,
                $"Post-restart full-universe read observed {postMissing.Count} missing keys " +
                "(no concurrent writers - the restart erased durable state). First 20: " +
                string.Join(", ", postMissing.Take(20)));

            Assert.That(postBadValue, Is.Empty,
                $"Post-restart full-universe read observed {postBadValue.Count} envelope mismatches " +
                "(corrupted value materialised across the restart). First 5: " +
                string.Join(" | ", postBadValue.Take(5)));

            Assert.That(postCount, Is.EqualTo(UniverseSize),
                "Post-restart CountAsync must match the seeded universe size " +
                "- there were no writers, so the count cannot have moved.");
        });
    }

    /// <summary>
    /// Polls a post-restart read on a bounded deadline, swallowing only
    /// the transient exceptions a silo reactivation legitimately raises
    /// while shards are still coming up on the surviving silo. A read
    /// that returns a value (including <c>null</c> for a genuinely absent
    /// key) is returned immediately so the durability assertions still
    /// observe real key loss; only throwing reads are retried. Once the
    /// deadline elapses the last transient exception is rethrown so a
    /// permanently-wedged reactivation still fails the test loudly.
    /// </summary>
    private static async Task<T> ReadWithReactivationRetryAsync<T>(
        Func<Task<T>> read, TimeSpan deadline)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            try
            {
                return await read();
            }
            catch (Exception ex) when (IsTransientReactivationException(ex))
            {
                if (sw.Elapsed >= deadline)
                {
                    throw;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(250));
            }
        }
    }

    /// <summary>
    /// True for the transient exceptions a read may observe while the
    /// restarted silo's shards are still reactivating: the lattice
    /// activation-readiness timeout and Orleans' own transient
    /// messaging/placement failures. Genuine durability defects surface
    /// as a <c>null</c> read result, not as one of these exceptions.
    /// </summary>
    private static bool IsTransientReactivationException(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException!)
        {
            if (e is ShardActivationTimeoutException or TimeoutException)
            {
                return true;
            }
            var typeName = e.GetType().Name;
            if (typeName is "OrleansMessageRejectionException"
                or "SiloUnavailableException"
                or "OrleansException")
            {
                return true;
            }
        }
        return false;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Wire the fixture-scope shared WAL provider BEFORE
            // AddLattice so AddLattice's TryAddSingleton is a no-op and
            // our shared instance becomes the resolved one. The default
            // per-silo InMemoryWalStorageProvider dies with the silo
            // and would erase every WalShardGrain partition that had
            // been hosted on the secondary - exactly the regression
            // this fixture exists to surface. Removing this line and
            // re-running the [Test] below reproduces the failure mode
            // deterministically.
            siloBuilder.AddWalStorage(_ => WalProvider);

            // Mirror the MultiSiloRestartChaosTests SiloConfigurator's
            // process-scope grain storage hookup so ShardRootGrain
            // topology survives the restart.
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new Orleans.Lattice.Tests.BPlusTree.PublicApiContract.ProcessScopeMemoryGrainStorage()));
            siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = 0);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}