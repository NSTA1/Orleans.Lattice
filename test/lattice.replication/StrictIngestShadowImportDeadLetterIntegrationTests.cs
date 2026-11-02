using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Schema;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5684, the receiver half. Under strict ingest, a peer's row that a re-seed
/// import delivers is judged by the logical tree's policy and, when refused, is
/// filed among the logical tree's dead letters and never served. This holds
/// whatever shape the export gives the row (committed, or prepared with its
/// decision before, after or absent), and while the import stages on a bootstrap
/// shadow copy of a tree whose alias points at another physical copy.
/// <para>
/// The real bootstrap coordinator drains a fake source's export into a held
/// shadow copy (<c>tree/resized/...</c>) and cuts over. The real schema
/// enforcement and versioning stages run, both strict, as a Contoso journey silo
/// registers them.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictIngestShadowImportDeadLetterIntegrationTests
{
    private const string ClusterId = "ssi-receiver";
    private const string SourceCluster = "ssi-source";
    private const uint SchemaId = 1;
    private const string RefusedKey = "order/refused";
    private const string ValidKey = "order/valid";

    private static readonly byte[] Remediated = Json("{\"total\":0,\"status\":\"open\"}");
    private static readonly byte[] Negative = Json("{\"total\":-25,\"status\":\"open\"}");
    private static readonly byte[] Positive = Json("{\"total\":40,\"status\":\"open\"}");

    private TestCluster _cluster = null!;
    private ILatticeSchemaAdmin _schemaAdmin = null!;
    private ILatticeSchemaVersionAdmin _versionAdmin = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _schemaAdmin = services.GetRequiredService<ILatticeSchemaAdmin>();
        _versionAdmin = services.GetRequiredService<ILatticeSchemaVersionAdmin>();
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

    private static byte[] Json(string text) => Encoding.UTF8.GetBytes(text);

    private static HybridLogicalClock Now(int counter = 0) => new() { WallClockTicks = DateTime.UtcNow.Ticks, Counter = counter };

    private ILatticeBootstrapCoordinator Coordinator => new LatticeBootstrapCoordinator(_cluster.Client);

    private static SnapshotEntry Committed(string key, byte[] value) => new() { Key = key, Value = value, Timestamp = Now() };

    private static SnapshotEntry Prepared(string key, byte[] value, Guid saga, int index) => new()
    {
        Key = key,
        Value = value,
        Timestamp = Now(index),
        IsPrepared = true,
        TransactionId = saga,
        AtomicBatchSize = 2,
        AtomicBatchIndex = index,
    };

    private static SnapshotEntry Decision(Guid saga) => new()
    {
        Key = string.Empty,
        Value = null!,
        TransactionId = saga,
        SettledDecision = true,
    };

    /// <summary>
    /// Creates the strict logical tree aliased to a populated physical copy, as a
    /// remediated or restored tree is, with the remediated value of the refused key.
    /// </summary>
    private async Task<string> StrictAliasedTreeAsync(string name)
    {
        var tree = $"{name}-{Guid.NewGuid():N}";
        var copy = tree + "/remediated/0";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(tree, new TreeRegistryEntry());
        await registry.RegisterAsync(copy, new TreeRegistryEntry());
        await _cluster.Client.GetGrain<ILattice>(copy).SetAsync(RefusedKey, Remediated);
        await registry.SetAliasAsync(tree, copy);

        await _versionAdmin.SetVersionConfigAsync(tree, new LatticeSchemaVersionConfig(SchemaId, 2, strictIngest: true));
        await _schemaAdmin.SetPolicyAsync(tree, new LatticeSchemaPolicy(
            [
                LatticeSchemaRule.Structured(LatticePredicateNode.Compare(
                    LatticeComparisonOperator.GreaterThanOrEqual,
                    LatticePredicateNode.Member("total"),
                    LatticePredicateNode.Const(LatticeConstant.Integer(0)))),
            ],
            strictIngest: true));
        return tree;
    }

    private async Task ImportAsync(string tree, params SnapshotEntry[] rows)
    {
        ExportRows.Rows[tree] = rows;
        await Coordinator.BootstrapAsync(tree, SourceCluster);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (true)
        {
            var status = await Coordinator.GetStatusAsync(tree);
            if (status.Phase == LatticeBootstrapState.LiveIncremental)
            {
                break;
            }

            Assert.That(status.Phase, Is.Not.EqualTo(LatticeBootstrapState.Failed), "the import must complete");
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), $"the import did not complete; last phase {status.Phase}");
            await Task.Delay(100);
        }

        Assert.That(await _cluster.Client.GetLatticeRegistry().ResolveAsync(tree), Does.Contain("/resized/"),
            "PRECONDITION: the import staged on a bootstrap shadow copy and cut over to it");
    }

    private async Task<int> DeadLettersAsync(string tree, string key)
    {
        var count = 0;
        await foreach (var entry in _schemaAdmin.ListDeadLettersAsync(tree))
        {
            if (entry.Key == key)
            {
                count++;
            }
        }

        return count;
    }

    private async Task<decimal?> TotalAsync(string tree, string key)
    {
        var value = await _cluster.Client.GetGrain<ILattice>(tree).GetAsync(key);
        if (value is null)
        {
            return null;
        }

        var body = LatticeSchemaEnvelope.IsEnveloped(value) ? LatticeSchemaEnvelope.StripToBody(value) : value;
        using var document = System.Text.Json.JsonDocument.Parse(body);
        return document.RootElement.GetProperty("total").GetDecimal();
    }

    private async Task AssertRefusedRowIsDeadLetteredUnderTheLogicalTreeAsync(string tree, bool validVisible)
    {
        var physical = await _cluster.Client.GetLatticeRegistry().ResolveAsync(tree);
        var refusedTotal = await TotalAsync(tree, RefusedKey);
        var validTotal = await TotalAsync(tree, ValidKey);
        var underLogical = await DeadLettersAsync(tree, RefusedKey);
        var underPhysical = await DeadLettersAsync(physical, RefusedKey);
        var underOldCopy = await DeadLettersAsync(tree + "/remediated/0", RefusedKey);

        Assert.Multiple(() =>
        {
            Assert.That(underLogical, Is.EqualTo(1),
                "the refused row a re-seed import delivers is filed once among the logical tree's dead letters");
            Assert.That(underPhysical + underOldCopy, Is.Zero,
                "no dead letter is filed under a physical copy's id");
            Assert.That(refusedTotal, Is.EqualTo(0m), "the refused value is never served; the remediated value stays");
            Assert.That(validTotal, validVisible ? Is.EqualTo(40m) : Is.Null);
        });
    }

    [Test]
    public async Task A_refused_committed_row_of_a_shadow_import_is_dead_lettered_under_the_logical_tree()
    {
        var tree = await StrictAliasedTreeAsync("ssi-committed");

        await ImportAsync(tree, Committed(RefusedKey, Negative), Committed(ValidKey, Positive));

        await AssertRefusedRowIsDeadLetteredUnderTheLogicalTreeAsync(tree, validVisible: true);
    }

    [Test]
    public async Task A_refused_prepared_row_decided_after_it_in_a_shadow_import_is_dead_lettered_under_the_logical_tree()
    {
        var tree = await StrictAliasedTreeAsync("ssi-prepared-then-decision");
        var saga = Guid.NewGuid();

        await ImportAsync(tree, Prepared(RefusedKey, Negative, saga, 0), Prepared(ValidKey, Positive, saga, 1), Decision(saga));

        await AssertRefusedRowIsDeadLetteredUnderTheLogicalTreeAsync(tree, validVisible: true);
    }

    [Test]
    public async Task A_refused_prepared_row_decided_before_it_in_a_shadow_import_is_dead_lettered_under_the_logical_tree()
    {
        var tree = await StrictAliasedTreeAsync("ssi-decision-then-prepared");
        var saga = Guid.NewGuid();

        await ImportAsync(tree, Decision(saga), Prepared(RefusedKey, Negative, saga, 0), Prepared(ValidKey, Positive, saga, 1));

        await AssertRefusedRowIsDeadLetteredUnderTheLogicalTreeAsync(tree, validVisible: true);
    }

    [Test]
    public async Task A_refused_prepared_row_of_an_undecided_saga_in_a_shadow_import_is_dead_lettered_under_the_logical_tree()
    {
        var tree = await StrictAliasedTreeAsync("ssi-undecided");
        var saga = Guid.NewGuid();

        await ImportAsync(tree, Prepared(RefusedKey, Negative, saga, 0), Prepared(ValidKey, Positive, saga, 1));

        await AssertRefusedRowIsDeadLetteredUnderTheLogicalTreeAsync(tree, validVisible: false);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IBootstrapSnapshotSource, ExportRows>();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = ClusterId;
                opts.AutoBootstrapOnFallOffLog = false;
                opts.BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
                {
                    MaxAttempts = 1,
                    InitialDelay = TimeSpan.Zero,
                    MaxDelay = TimeSpan.Zero,
                };
            });
            siloBuilder.AddLatticeSchemaEnforcement(o => o.StrictIngest = true);
            siloBuilder.AddLatticeSchemaVersioning(
                registry => registry
                    .AddSchema(SchemaId, 1, "order-v1")
                    .AddSchema(SchemaId, 2, "order-v2")
                    .AddUpcaster(SchemaId, 1, 2, LatticeValueTransform.Passthrough(
                        LatticeValueTransform.SetMember(
                            "currency", LatticeValueTransform.Const(LatticeConstant.Text("EUR"))))),
                o => o.StrictIngest = true);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>The source's export: the rows a test armed for the tree. Static because the silo constructs it.</summary>
    private sealed class ExportRows : IBootstrapSnapshotSource
    {
        public static readonly ConcurrentDictionary<string, SnapshotEntry[]> Rows = new(StringComparer.Ordinal);

        public Task<SnapshotStream> ExportAsync(string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default) =>
            Task.FromResult(new SnapshotStream(treeName, asOfHlc, new VersionVector(), EnumerateAsync(treeName)));

        private static async IAsyncEnumerable<SnapshotEntry> EnumerateAsync(string treeName)
        {
            await Task.CompletedTask;
            foreach (var row in Rows.GetValueOrDefault(treeName) ?? [])
            {
                yield return row;
            }
        }
    }
}
