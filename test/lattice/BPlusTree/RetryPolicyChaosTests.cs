using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos theory with fault injection that proves the
/// <see cref="BoundedExponentialRetryPolicy"/> + ambient
/// <see cref="LatticeIdempotencyContext"/> pipeline actually
/// masks transient storage failures from the caller. Companion
/// to <see cref="ChaosWithFaultsIntegrationTests"/>: that suite
/// tolerates exceptions during the chaos window and only
/// requires post-quiescence convergence; this suite REQUIRES
/// that every caller-side mutation succeed despite armed
/// one-shot write faults, because the retry policy is supposed
/// to absorb them. Failure modes the test catches:
/// <list type="bullet">
///   <item>retry policy never invoked (caller sees the injected exception),</item>
///   <item>retry policy invoked but exhausts its budget (caller still sees the injected exception),</item>
///   <item>retries succeed but produce N distinct mutations because the idempotency key did not collapse them (PnCounter double-count, multiple stored HLCs).</item>
/// </list>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class RetryPolicyChaosTests
{
    private RetryPolicyChaosClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int UniverseSize = 80;
    private const int WriterCount = 4;
    private const int CounterIncrementCount = 60;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(6);
    private static readonly TimeSpan FaultInjectionInterval = TimeSpan.FromMilliseconds(15);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new RetryPolicyChaosClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"rpchaos-{i:D5}";

    /// <summary>
    /// Computes the deterministic leaf GrainId for a given shard, matching
    /// <c>ShardRootGrain.EnsureRootAsync</c>'s SHA-256 derivation. Used to
    /// pre-register target grains for the fault injector.
    /// </summary>
    private GrainId GetInitialLeafGrainId(string treeId, int shardIndex)
    {
        var shardKey = $"{treeId}/{shardIndex}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(shardKey));
        var guid = new Guid(hash.AsSpan(0, 16));
        return _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(guid).GetGrainId();
    }

    private async Task<List<GrainId>> DiscoverFaultTargetsAsync(string treeId)
    {
        var targets = new List<GrainId>();
        for (int s = 0; s < RetryPolicyChaosClusterFixture.TestShardCount; s++)
        {
            targets.Add(GetInitialLeafGrainId(treeId, s));
            var shardRoot = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{s}");
            await shardRoot.GetAsync(KeyOf(0));
            targets.Add(shardRoot.GetGrainId());
        }
        return targets;
    }

    [TestCase(0.05, TestName = "5pct_fault_probability_no_caller_visible_errors")]
    [TestCase(0.15, TestName = "15pct_fault_probability_no_caller_visible_errors")]
    [TestCase(0.30, TestName = "30pct_fault_probability_no_caller_visible_errors")]
    public async Task RetryPolicy_under_idempotency_scope_masks_transient_write_faults(double faultProbability)
    {
        var treeId = $"rpchaos-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var faultCtl = _cluster.GrainFactory.GetGrain<IStorageFaultGrain>(
            LatticeOptions.StorageProviderName);

        // Verify the fixture wired the retry policy globally so every
        // tree on the silo inherits it. This is the supported turnkey
        // path documented in AddLatticeRetryPolicy.
        var monitor = _cluster.Silos
            .OfType<InProcessSiloHandle>()
            .First()
            .SiloHost
            .Services
            .GetRequiredService<IOptionsMonitor<LatticeOptions>>();
        Assert.That(monitor.Get(treeId).RetryPolicy, Is.Not.Null,
            "Fixture must register a global retry policy so the chaos run actually exercises it.");

        // Discover targets for fault injection BEFORE the chaos window
        // starts so injector activation costs do not eat into the
        // workload's deadline.
        var targets = await DiscoverFaultTargetsAsync(treeId);

        var visibleFailures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k)
            => s.AddOrUpdate(k, 1, (_, v) => v + 1);

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Fault injector: arms one-shot write faults on random targets.
        workers.Add(Task.Run(async () =>
        {
            var rng = new Random(91239);
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(FaultInjectionInterval, ct);
                    if (rng.NextDouble() >= faultProbability) continue;
                    var target = targets[rng.Next(targets.Count)];
                    try
                    {
                        await faultCtl.AddFaultOnWrite(target,
                            new InvalidOperationException("Injected chaos fault"));
                        Bump(stats, "faults-armed");
                    }
                    catch (ArgumentException)
                    {
                        // Fault already armed for this target - skip.
                        Bump(stats, "faults-already-armed");
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    visibleFailures.Add($"fault-injector threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        // ---- Point writers under ambient idempotency scope + the
        // globally-registered retry policy. Each iteration mints a
        // fresh key (one logical operation per iteration). Any
        // injected fault on the write path must be absorbed by the
        // retry policy - the caller MUST NOT observe the exception.
        for (int w = 0; w < WriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 7919 + 1);
                while (!ct.IsCancellationRequested)
                {
                    var idx = rng.Next(UniverseSize);
                    try
                    {
                        using (LatticeIdempotencyContext.NewScope())
                        {
                            await tree.SetAsync(KeyOf(idx),
                                Encoding.UTF8.GetBytes($"v-{idx}-{writerId}"));
                            Bump(stats, "point-writes");
                        }
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        // Under the retry policy, any caller-visible
                        // exception is a regression - the policy was
                        // supposed to mask it.
                        visibleFailures.Add(
                            $"writer{writerId}: SetAsync surfaced {ex.GetType().Name}: {ex.Message}");
                        Bump(stats, "caller-visible-write-errors");
                    }
                }
            }, ct));
        }

        await Task.WhenAll(workers);

        // ---- Strong invariants. Failures here imply the retry
        // policy / idempotency pipeline did NOT mask the transient
        // failures and the feature is regressed.
        Assert.Multiple(() =>
        {
            Assert.That(visibleFailures, Is.Empty,
                $"Observed {visibleFailures.Count} caller-visible failures (first 10):\n " +
                string.Join("\n ", visibleFailures.Take(10)));

            Assert.That(stats.GetValueOrDefault("faults-armed", 0), Is.GreaterThan(0),
                "Fault injector must have armed at least one fault, otherwise the " +
                "negative-control assertion is vacuous.");

            Assert.That(stats.GetValueOrDefault("point-writes", 0), Is.GreaterThan(0),
                "Workload must have completed at least one write under the retry policy.");

            Assert.That(stats.GetValueOrDefault("caller-visible-write-errors", 0), Is.EqualTo(0),
                "Retry policy + idempotency scope must have masked every transient fault " +
                "from the caller - any non-zero count here is a regression.");
        });

        TestContext.Out.WriteLine($"RetryPolicyChaos stats (p={faultProbability}):");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-30}{kv.Value}");
    }

    [Test]
    public async Task PnCounter_IncrementAsync_under_retry_policy_collapses_retries_to_single_advance()
    {
        // Strong end-to-end semantic check: the PnCounter pre-CAS
        // dedup guard depends on the foreground write re-stamping
        // the same HLC. If the retry policy were to retry under a
        // FRESH idempotency key (the regression mode this feature
        // is designed to prevent), the counter would double-count
        // every retried increment. We arm faults aggressively
        // while issuing increments, then verify the counter
        // advanced by exactly the number of caller-visible commits.
        var treeId = $"rpchaos-pn-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var counter = tree.PnCounter("pn");
        var faultCtl = _cluster.GrainFactory.GetGrain<IStorageFaultGrain>(
            LatticeOptions.StorageProviderName);
        var targets = await DiscoverFaultTargetsAsync(treeId);

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;
        var rng = new Random(424242);
        var injector = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(FaultInjectionInterval, ct);
                    var target = targets[rng.Next(targets.Count)];
                    try
                    {
                        await faultCtl.AddFaultOnWrite(target,
                            new InvalidOperationException("Injected chaos fault"));
                    }
                    catch (ArgumentException) { /* already armed */ }
                }
                catch (OperationCanceledException) { }
            }
        }, ct);

        var visibleFailures = new List<string>();
        var actuallyCommitted = 0;
        for (int i = 0; i < CounterIncrementCount; i++)
        {
            try
            {
                using (LatticeIdempotencyContext.NewScope())
                {
                    await counter.IncrementAsync("r1", 1);
                }
                actuallyCommitted++;
            }
            catch (Exception ex)
            {
                visibleFailures.Add(
                    $"increment {i}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        cts.Cancel();
        try { await injector; } catch (OperationCanceledException) { }

        var value = await counter.ValueAsync();
        Assert.Multiple(() =>
        {
            Assert.That(visibleFailures, Is.Empty,
                $"Observed {visibleFailures.Count} caller-visible failures:\n " +
                string.Join("\n ", visibleFailures.Take(10)));

            Assert.That(value, Is.EqualTo(actuallyCommitted),
                $"PnCounter value ({value}) must equal the number of caller-side " +
                $"commits ({actuallyCommitted}) - any difference proves a retry " +
                "double-counted, which means the idempotency dedup guard did not fire.");
        });
    }
}

/// <summary>
/// Cluster fixture for the retry-policy chaos suite. Wires the
/// fault-injection grain storage AND a turnkey
/// <see cref="BoundedExponentialRetryPolicy"/> with a tight budget
/// so the chaos window completes within the test deadline. The
/// budget is deliberately small (10 attempts, zero delay) so the
/// test fails fast if the policy is not actually retrying - a real
/// regression where the policy never kicks in would surface as
/// caller-visible exceptions, not as a wall-clock timeout.
/// </summary>
public sealed class RetryPolicyChaosClusterFixture
{
    public const string TreeName = "rp-chaos-tree";
    public const int TestShardCount = 4;
    public const int SmallMaxLeafKeys = 4;
    public const int SmallMaxInternalChildren = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();

        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(TreeName, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            MaxInternalChildren = SmallMaxInternalChildren,
            ShardCount = TestShardCount,
        });
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((_, name) =>
                siloBuilder.Services.AddFaultInjectionMemoryStorage(
                    name,
                    (MemoryGrainStorageOptions _) => { },
                    (FaultInjectionGrainStorageOptions _) => { }));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeRetryPolicy(o =>
            {
                // Tight budget: enough attempts to mask the per-tick
                // fault rate, zero delay so the chaos window does not
                // bottleneck on backoff sleeps.
                o.MaxAttempts = 10;
                o.InitialDelay = TimeSpan.Zero;
                o.MaxDelay = TimeSpan.Zero;
            });
        }
    }
}
