using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for #5551: under strict ingest, a row a receiver bootstrap
/// drains from a peer's export is judged at the write choke point exactly as a
/// replicated atomic-batch entry is. An export carries committed state only, so a
/// batch the source withheld (for example after a coordinated restore's lineage
/// gap) reaches the receiver as plain committed last-writer-wins rows. Those rows
/// used to be merged below the choke point, so a value the receiver's policy
/// refuses was stored as an ordinary row instead of being diverted to the schema
/// dead-letter queue.
/// <para>
/// Each export row is converted by the drain's own
/// <see cref="LatticeBootstrapCoordinatorGrain.ToSnapshotWalRecord"/> and applied
/// through the real canonical applier inside the drain's
/// <see cref="LatticeBootstrapApplyContext"/> scope, as the coordinator applies it.
/// The interceptor implements the strict-ingest contract of the schema write
/// interceptor at the core seam: a refused system-origin write records one
/// dead-letter entry and returns a dead-letter decision.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictIngestBootstrapExportDivertIntegrationTests
{
    private const string Source = "sbe-source";
    private const string RefusedPrefix = "refused-";

    private TestCluster _cluster = null!;
    private ReplicationApplier _applier = null!;
    private RefusingStrictIngestInterceptor _interceptor = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _applier = services.GetRequiredService<ReplicationApplier>();
        _interceptor = (RefusingStrictIngestInterceptor)services.GetRequiredService<ILatticeWriteInterceptor>();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static SnapshotEntry Committed(string key, byte value, long ticks) => new()
    {
        Key = key,
        Value = [value],
        Timestamp = Hlc(ticks),
    };

    private static SnapshotEntry Prepared(string key, byte value, Guid txid, int index) => new()
    {
        Key = key,
        Value = [value],
        Timestamp = Hlc(2_000 + index),
        IsPrepared = true,
        TransactionId = txid,
        AtomicBatchSize = 2,
        AtomicBatchIndex = index,
    };

    /// <summary>
    /// Applies <paramref name="rows"/> as the bootstrap coordinator's drain does:
    /// each row converted by the drain's own conversion and applied through the
    /// canonical applier inside the drain's ambient scope.
    /// </summary>
    private async Task DrainAsync(string tree, params SnapshotEntry[] rows)
    {
        using var drain = LatticeBootstrapApplyContext.BeginScope();
        foreach (var row in rows)
        {
            var converted = LatticeBootstrapCoordinatorGrain.ToSnapshotWalRecord(row, tree, Source, LatticeMergeMode.LwwRegister);
            Assert.That(converted.HasValue, Is.True, $"export row '{row.Key}' converts to a record the drain applies");
            var result = await _applier.ApplyAsync(converted.GetValueOrDefault());
            Assert.That(result.Deferred, Is.False, $"export row '{row.Key}' is not deferred");
        }
    }

    [Test]
    public async Task Refused_committed_export_row_under_strict_ingest_is_diverted_not_applied()
    {
        const string tree = "sbe-committed";
        const string refusedKey = RefusedPrefix + "order-1";
        const string validKey = "valid-order-2";

        await DrainAsync(tree, Committed(refusedKey, 1, 1_000), Committed(validKey, 2, 1_001));

        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var refused = await lattice.GetAsync(refusedKey);
        var valid = await lattice.GetAsync(validKey);

        Assert.Multiple(() =>
        {
            Assert.That(_interceptor.DeadLettered(tree, refusedKey), Is.EqualTo(1),
                "a refused export row is judged at the write choke point and diverted to the schema dead-letter queue exactly once");
            Assert.That(refused, Is.Null, "a refused export row is diverted, never stored as an ordinary row");
            Assert.That(valid, Is.EqualTo(new byte[] { 2 }), "a compliant export row is applied");
            Assert.That(_interceptor.DeadLettered(tree, validKey), Is.Zero);
        });
    }

    [Test]
    public async Task Partially_exported_saga_does_not_partially_apply_and_its_refused_prepare_is_diverted()
    {
        const string tree = "sbe-saga";
        const string refusedKey = RefusedPrefix + "order-1";
        const string validKey = "valid-order-2";
        var txid = Guid.NewGuid();

        // The export carries the saga's prepares but not its decision, so the drain
        // must stage them, never surface either one on its own.
        await DrainAsync(tree, Prepared(refusedKey, 1, txid, index: 0), Prepared(validKey, 2, txid, index: 1));

        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var refused = await lattice.GetAsync(refusedKey);
        var valid = await lattice.GetAsync(validKey);

        Assert.Multiple(() =>
        {
            Assert.That(valid, Is.Null, "a saga staged from an export with no decision is not visible in part");
            Assert.That(refused, Is.Null, "a refused prepare is never visible");
            Assert.That(_interceptor.DeadLettered(tree, refusedKey), Is.EqualTo(1),
                "a refused prepare carried by an export is diverted to the schema dead-letter queue exactly once");
            Assert.That(_interceptor.DeadLettered(tree, validKey), Is.Zero);
        });
    }

    /// <summary>
    /// Strict ingest at the core seam: a system-origin write to a refused key is
    /// dead-lettered (one entry recorded per decision), a local one is rejected.
    /// </summary>
    private sealed class RefusingStrictIngestInterceptor : ILatticeWriteInterceptor
    {
        private readonly ConcurrentDictionary<(string Tree, string Key), int> _deadLettered = new();

        public bool InterceptsSystemOrigin => true;

        public int DeadLettered(string tree, string key) =>
            _deadLettered.TryGetValue((tree, key), out var count) ? count : 0;

        public ValueTask<LatticeWriteDecision> OnWriteAsync(
            in LatticeWriteRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.Key.StartsWith(RefusedPrefix, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(LatticeWriteDecision.Accept());
            }

            if (!LatticeAccessGateContext.IsSystemOrigin)
            {
                return ValueTask.FromResult(LatticeWriteDecision.Reject("refused by policy"));
            }

            _deadLettered.AddOrUpdate((request.TreeId, request.Key), 1, (_, count) => count + 1);
            return ValueTask.FromResult(LatticeWriteDecision.DeadLetter("refused by policy"));
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "sbe-receiver";
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
            siloBuilder.Services.Replace(
                ServiceDescriptor.Singleton<ILatticeWriteInterceptor>(new RefusingStrictIngestInterceptor()));
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
