using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Deterministic regression coverage for issue #926: CRDT writes made
/// through <see cref="ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], System.Threading.CancellationToken)"/>
/// must survive a silo restart on a <b>non-replicated</b> tree.
/// <para>
/// <b>The defect.</b> A CRDT-mode <c>Set</c> record is delta-only on the
/// WAL: the producer never materialises the post-merge value and the
/// encoder strips it, so a durable replay sees <c>Value == null</c> with
/// the typed delta in <c>Delta</c> and the convergence rule in
/// <c>Mode</c>. Before the fix the encoder did not persist <c>Mode</c>
/// and the storage read-back re-derived it from the
/// <see cref="ILatticeMergeModeResolver"/>, which returns
/// <see langword="null"/> -> <see cref="LatticeMergeMode.LwwRegister"/>
/// for every tree it does not know (every tree on a single-cluster host,
/// and any host tree absent from the configured replicated set). Replay
/// then skipped the CRDT fold and installed the stripped null value via
/// LWW, silently emptying the key.
/// </para>
/// <para>
/// <b>Why this fixture reproduces it without the replication package.</b>
/// No replication package is registered, so the resolver is the core
/// default that returns <see langword="null"/> for every tree - exactly
/// the production condition for a single-cluster host using CRDT deltas.
/// The shared <see cref="InMemoryWalStorageProvider"/> plus process-scope
/// grain storage survive <see cref="TestCluster.RestartSiloAsync(SiloHandle)"/>,
/// so the restart exercises the genuine WAL replay path rather than an
/// erased-state artefact.
/// </para>
/// <para>
/// <b>Reproduction.</b> Revert the encoder/read-back fix (re-detag
/// <c>WalRecord.Mode</c>) and this fixture fails post-restart with a
/// large fraction of the OR-Set members missing - mirroring the original
/// 36/64-lost observation recorded on issue #926.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Integration")]
public class OriginCrdtRestartWalDurabilityRegressionTests
{
    private TestCluster _cluster = null!;

    private const int UniverseSize = 64;
    private const int MultiMemberCount = 8;

    /// <summary>
    /// Fixture-scope shared <see cref="InMemoryWalStorageProvider"/> wired
    /// into every silo so WAL state survives the secondary-silo restart -
    /// without it the restart would erase the WAL outright and mask the
    /// mode-recovery defect this fixture targets.
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

    private static string KeyOf(int i) => $"crdtr-{i:D5}";

    private static string ElementOf(int i) => $"elem-{i}";

    private static byte[] OrSetAddDelta(string element, string replica, long counter)
    {
        var delta = new OrSetDelta
        {
            Adds = new[]
            {
                new OrSetDeltaDot
                {
                    Element = Encoding.UTF8.GetBytes(element),
                    ReplicaId = replica,
                    Counter = counter,
                },
            },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        return JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);
    }

    private static byte[] PnCounterIncrementDelta(string replica, long amount)
    {
        var delta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { [replica] = amount },
            Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
        };
        return JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(delta);
    }

    [Test]
    public async Task Crdt_delta_writes_survive_secondary_silo_restart_on_non_replicated_tree()
    {
        var treeId = $"crdtr-regress-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 8,
            ShardCount = 2,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        // Seed a universe of single-member OR-Sets, one per key. Each
        // ApplyCrdtDeltaAsync appends a delta-only CRDT WAL record whose
        // mode must survive the restart for the fold to reconstruct it.
        for (int i = 0; i < UniverseSize; i++)
        {
            await tree.ApplyCrdtDeltaAsync(
                KeyOf(i), LatticeMergeMode.OrSet, OrSetAddDelta(ElementOf(i), "r1", 1));
        }

        // One key that accumulates many members across many deltas, so
        // the restart also exercises incremental fold composition across
        // multiple replayed delta records for the same key.
        const string multiKey = "crdtr-multi";
        for (int m = 0; m < MultiMemberCount; m++)
        {
            await tree.ApplyCrdtDeltaAsync(
                multiKey, LatticeMergeMode.OrSet, OrSetAddDelta($"m-{m}", $"r{m}", 1));
        }

        // A PnCounter key accumulating increments, covering a second
        // typed CRDT mode through the same restart.
        const string counterKey = "crdtr-counter";
        long expectedCount = 0;
        for (int k = 1; k <= 5; k++)
        {
            await tree.ApplyCrdtDeltaAsync(
                counterKey, LatticeMergeMode.PnCounter, PnCounterIncrementDelta($"r{k}", k));
            expectedCount += k;
        }

        // Sanity: everything is readable and correct BEFORE the restart,
        // so any post-restart loss is attributable to the restart.
        await AssertOrSetUniversePresentAsync(tree, "pre-restart", TimeSpan.Zero);
        await AssertMultiMemberPresentAsync(tree, multiKey, "pre-restart", TimeSpan.Zero);
        await AssertCounterAsync(tree, counterKey, expectedCount, "pre-restart", TimeSpan.Zero);

        // Restart the secondary silo. No writers are in flight, so the
        // only state change is whatever the WAL replay reconstructs on
        // the partitions whose home silo was the secondary.
        var secondary = _cluster.SecondarySilos.FirstOrDefault();
        Assert.That(secondary, Is.Not.Null,
            "Test cluster must have at least one SecondarySilo to restart.");
        await _cluster.RestartSiloAsync(secondary!);

        var settleDeadline = TimeSpan.FromSeconds(60);

        // Post-restart invariants. Every CRDT write must have survived.
        await AssertOrSetUniversePresentAsync(tree, "post-restart", settleDeadline);
        await AssertMultiMemberPresentAsync(tree, multiKey, "post-restart", settleDeadline);
        await AssertCounterAsync(tree, counterKey, expectedCount, "post-restart", settleDeadline);
    }

    private static async Task AssertOrSetUniversePresentAsync(
        ILattice tree, string phase, TimeSpan deadline)
    {
        var missing = new List<string>();
        var badMembership = new List<string>();
        for (int i = 0; i < UniverseSize; i++)
        {
            var raw = await ReadWithReactivationRetryAsync(() => tree.GetAsync(KeyOf(i)), deadline);
            if (raw is null)
            {
                missing.Add(KeyOf(i));
                continue;
            }
            var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(raw);
            if (!observed.Contains(Encoding.UTF8.GetBytes(ElementOf(i))))
            {
                badMembership.Add(KeyOf(i));
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(missing, Is.Empty,
                $"{phase}: {missing.Count}/{UniverseSize} OR-Set keys read back null " +
                "(delta-only CRDT record replayed as an LWW null - issue #926). First 20: " +
                string.Join(", ", missing.Take(20)));
            Assert.That(badMembership, Is.Empty,
                $"{phase}: {badMembership.Count}/{UniverseSize} OR-Sets lost their member " +
                "(fold did not reconstruct the typed delta). First 20: " +
                string.Join(", ", badMembership.Take(20)));
        });
    }

    private static async Task AssertMultiMemberPresentAsync(
        ILattice tree, string key, string phase, TimeSpan deadline)
    {
        var raw = await ReadWithReactivationRetryAsync(() => tree.GetAsync(key), deadline);
        Assert.That(raw, Is.Not.Null, $"{phase}: multi-member OR-Set '{key}' read back null.");
        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(raw!);
        var absent = new List<string>();
        for (int m = 0; m < MultiMemberCount; m++)
        {
            if (!observed.Contains(Encoding.UTF8.GetBytes($"m-{m}")))
            {
                absent.Add($"m-{m}");
            }
        }
        Assert.That(absent, Is.Empty,
            $"{phase}: multi-member OR-Set '{key}' lost {absent.Count}/{MultiMemberCount} members " +
            "across the restart (incremental fold composition broken). Missing: " +
            string.Join(", ", absent));
    }

    private static async Task AssertCounterAsync(
        ILattice tree, string key, long expected, string phase, TimeSpan deadline)
    {
        var raw = await ReadWithReactivationRetryAsync(() => tree.GetAsync(key), deadline);
        Assert.That(raw, Is.Not.Null, $"{phase}: PnCounter '{key}' read back null.");
        var observed = JsonLatticeSerializer<PnCounter>.Default.Deserialize(raw!);
        Assert.That(observed.Value, Is.EqualTo(expected),
            $"{phase}: PnCounter '{key}' value drifted across the restart.");
    }

    /// <summary>
    /// Polls a post-restart read on a bounded deadline, swallowing only
    /// the transient exceptions a silo reactivation legitimately raises
    /// while shards are still coming up on the surviving silo. A read that
    /// returns a value (including <c>null</c> for a genuinely absent key)
    /// is returned immediately so the durability assertions still observe
    /// real loss; only throwing reads are retried. A zero deadline does a
    /// single attempt (the pre-restart sanity reads).
    /// </summary>
    private static async Task<byte[]?> ReadWithReactivationRetryAsync(
        Func<Task<byte[]?>> read, TimeSpan deadline)
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
            // Shared WAL provider survives RestartSiloAsync; the default
            // per-silo InMemoryWalStorageProvider would die with the silo
            // and mask the mode-recovery defect.
            siloBuilder.AddWalStorage(_ => WalProvider);

            // Process-scope grain storage so ShardRootGrain topology and
            // leaf checkpoints survive the restart, exactly as the sibling
            // MultiSiloRestartWalDurabilityRegressionTests fixture wires.
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new Orleans.Lattice.Tests.BPlusTree.PublicApiContract.ProcessScopeMemoryGrainStorage()));
            siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = 0);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
