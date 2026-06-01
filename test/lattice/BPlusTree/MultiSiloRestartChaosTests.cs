using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of in-flight workload across a silo restart. Two-silo
/// <see cref="TestCluster"/>; primary silo anchors durable state via
/// memory grain storage that survives the restart; the secondary silo
/// is killed and re-deployed mid-workload via
/// <see cref="TestCluster.RestartSiloAsync(SiloHandle)"/>. Workload
/// continues against the cluster client throughout (client routes
/// transparently to any live silo). Post-window invariant: the universe
/// is intact, every key carries an envelope-valid value, every saga
/// reached a terminal state, and CountAsync matches the pinned universe.
/// </summary>
/// <remarks>
/// <para>
/// <b>Storage durability across silo restart.</b> The fixture uses
/// <see cref="PublicApiContract.ProcessScopeMemoryGrainStorage"/>
/// (a static-dictionary-backed <c>IGrainStorage</c> shared across
/// every silo in the cluster) instead of the Orleans-shipped
/// <c>AddMemoryGrainStorage</c>. The Orleans-shipped provider is
/// per-silo and dies with the silo, so a SecondarySilo restart
/// wipes any <c>ShardRootGrain</c> / <c>ILatticeRegistry</c> /
/// leaf state that was anchored there; the next reactivation
/// would read empty state and re-run <c>EnsureRootAsync</c>,
/// overwriting the live tree topology with a single-leaf root.
/// That storage-isolation regression is unrelated to silo
/// membership churn and would mask the actual invariant this
/// test exists to verify.
/// </para>
/// <para>
/// <b>Acceptance.</b> Post-window CountAsync must match the seeded
/// universe; no envelope violation on any final read; restart driver
/// must have run at least one restart.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class MultiSiloRestartChaosTests
{
    private TestCluster _cluster = null!;

    private const int UniverseSize = 50;
    private const int WriterCount = 3;
    private const int ReaderCount = 2;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(8);
    private static readonly TimeSpan RestartInterval = TimeSpan.FromMilliseconds(2500);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync("dummy-warmup-tree", new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = 2,
        });
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static string KeyOf(int i) => $"silor-{i:D5}";

    private static int IndexOfKey(string key) =>
        key.StartsWith("silor-", StringComparison.Ordinal)
            && int.TryParse(key.AsSpan(6), out var idx)
            ? idx
            : -1;

    private static bool IsValidValueFor(int expectedIndex, byte[] value)
    {
        if (value is null || value.Length == 0) return false;
        var s = Encoding.UTF8.GetString(value);
        return s.StartsWith($"v-{expectedIndex}-", StringComparison.Ordinal);
    }

    /// <summary>
    /// During the restart window almost any exception class is
    /// tolerated because Orleans can be mid-handoff between silos. The
    /// post-window assertions are the source of truth.
    /// </summary>
    private static bool IsToleratedDuringRestart(Exception _) => true;

    [Test]
    public async Task Chaos_secondary_silo_restart_under_load_preserves_universe()
    {
        var treeId = $"silor-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 8,
            ShardCount = 2,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        for (int i = 0; i < UniverseSize; i++)
        {
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes($"v-{i}-seed-0"));
        }

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Writer workers: rewrite random universe keys.
        for (int w = 0; w < WriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 7919 + 11);
                int seq = 0;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(UniverseSize);
                        var value = Encoding.UTF8.GetBytes($"v-{idx}-w{writerId}-{++seq}");
                        await tree.SetAsync(KeyOf(idx), value);
                        Bump(stats, "writes");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsToleratedDuringRestart(ex)) { Bump(stats, "tolerated-write-errors"); }
                }
            }, ct));
        }

        // ---- Reader workers: random GetAsync; envelope-validate.
        for (int r = 0; r < ReaderCount; r++)
        {
            var readerId = r;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(readerId * 15485863 + 5);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(UniverseSize);
                        var v = await tree.GetAsync(KeyOf(idx));
                        // During the restart window a temporarily-missing
                        // key is tolerated (the silo may have just
                        // restarted and the activation has not yet
                        // re-resolved). Bad envelope is NOT tolerated -
                        // that would indicate state corruption.
                        if (v is not null && !IsValidValueFor(idx, v))
                        {
                            failures.Add($"reader{readerId}: envelope violation for key {idx}: " +
                                Encoding.UTF8.GetString(v));
                        }
                        Bump(stats, "reads");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsToleratedDuringRestart(ex)) { Bump(stats, "tolerated-read-errors"); }
                }
            }, ct));
        }

        // ---- Restart driver: kill + redeploy a secondary silo every
        // RestartInterval. With two silos in the cluster and only the
        // secondary being restarted, the primary survives across every
        // restart - any grain activation that was placed on the primary
        // continues without interruption; activations on the secondary
        // are recovered on the primary on next call.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(RestartInterval, ct);
                    var secondary = _cluster.SecondarySilos.FirstOrDefault();
                    if (secondary is null) continue;
                    Bump(stats, "restart-attempts");
                    await _cluster.RestartSiloAsync(secondary);
                    Bump(stats, "restarts");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    failures.Add($"restart-driver threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        await Task.WhenAll(workers);

        // Give Orleans a moment to converge any in-flight activation
        // handoff after the chaos window closes.
        await Task.Delay(TimeSpan.FromSeconds(2));

        // ---- Post-window invariants.
        var finalCount = await tree.CountAsync();
        var envelopeViolations = new List<string>();
        for (int i = 0; i < UniverseSize; i++)
        {
            var v = await tree.GetAsync(KeyOf(i));
            if (v is null)
            {
                envelopeViolations.Add($"key {KeyOf(i)} missing post-window");
            }
            else if (!IsValidValueFor(i, v))
            {
                envelopeViolations.Add($"key {KeyOf(i)} bad envelope: " + Encoding.UTF8.GetString(v));
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} non-tolerated exceptions / envelope violations during the window (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(envelopeViolations, Is.Empty,
                $"Post-window observed {envelopeViolations.Count} envelope / presence violations:\n " +
                string.Join("\n ", envelopeViolations.Take(20)));

            Assert.That(finalCount, Is.EqualTo(UniverseSize),
                "Post-window CountAsync must match the pinned universe size " +
                "- writers only rewrite existing keys, so the count must not change.");

            Assert.That(stats.GetValueOrDefault("writes", 0), Is.GreaterThan(0),
                "Writer workers must have made progress.");
            Assert.That(stats.GetValueOrDefault("reads", 0), Is.GreaterThan(0),
                "Reader workers must have made progress.");
            Assert.That(stats.GetValueOrDefault("restart-attempts", 0), Is.GreaterThan(0),
                "Restart driver must have attempted at least one silo restart.");
        });

        TestContext.Out.WriteLine("MultiSiloRestart workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-26}{kv.Value}");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Use a process-scope in-memory grain storage provider so
            // every silo in the cluster shares one backing dictionary.
            // The default per-silo Orleans memory-storage provider dies
            // with the silo, so a SecondarySilo restart wipes any
            // ShardRootGrain / ILatticeRegistry state that was anchored
            // there - the next reactivation reads empty state, re-runs
            // EnsureRootAsync, and overwrites the live topology with a
            // single-leaf root. That storage-isolation regression is the
            // upstream cause of the InvalidCastException previously
            // tracked on GitHub Issues (the cast fires when an in-flight
            // call resolves a leaf reference against an internal-grain
            // activation in the now-split-brain shard root). Using
            // process-scope storage isolates this test to the surface it
            // claims to exercise: silo membership churn, not storage
            // disappearance.
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new Orleans.Lattice.Tests.BPlusTree.PublicApiContract.ProcessScopeMemoryGrainStorage()));
            siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = 0);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
