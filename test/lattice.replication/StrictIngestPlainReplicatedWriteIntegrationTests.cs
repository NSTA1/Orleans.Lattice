using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Schema;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5684: under strict ingest a plain last-writer-wins replicated write is
/// judged at the write choke point as an atomic entry is, whichever path delivers
/// it - a single entry, a merged batch, a repair re-ship, or a write routed to a
/// bootstrap shadow copy. A value the receiver's strict policy refuses is diverted
/// to the logical tree's dead letters once and never stored; an admitted value is
/// stored verbatim. A tree that is not strict stores every plain write as
/// delivered. The real canonical applier, dead-letter decorator and schema
/// enforcement and versioning stages run.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictIngestPlainReplicatedWriteIntegrationTests
{
    private const string Origin = "spw-source";

    private static readonly byte[] Open = Encoding.UTF8.GetBytes("{\"status\":\"open\"}");
    private static readonly byte[] Pending = Encoding.UTF8.GetBytes("{\"status\":\"pending\"}");

    private TestCluster _cluster = null!;
    private ReplicationApplier _applier = null!;
    private DeadLetterTrackingReplicationApplier _receiver = null!;
    private ILatticeSchemaAdmin _schemaAdmin = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _applier = services.GetRequiredService<ReplicationApplier>();
        _receiver = new DeadLetterTrackingReplicationApplier(
            _applier,
            services.GetRequiredService<IGrainFactory>(),
            services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        _schemaAdmin = services.GetRequiredService<ILatticeSchemaAdmin>();
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

    private static WalRecord PlainSet(string tree, string key, byte[] value, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Mode = LatticeMergeMode.LwwRegister,
        Value = value,
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
        OriginClusterId = Origin,
    };

    private async Task<string> StrictTreeAsync(string name, bool strictIngest = true)
    {
        var tree = $"{name}-{Guid.NewGuid():N}";
        await _schemaAdmin.SetPolicyAsync(tree, new LatticeSchemaPolicy(
            new[]
            {
                LatticeSchemaRule.Json(),
                LatticeSchemaRule.Regex("^(open|closed)$", memberPath: "status"),
            },
            strictIngest: strictIngest));
        return tree;
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

    private async Task<byte[]?> StoredAsync(string tree, string key) =>
        (await _cluster.Client.GetGrain<IReplicationApplyGrain>(tree).ReadStoredWithVersionAsync(key)).Value;

    [Test]
    public async Task A_refused_plain_write_is_dead_lettered_under_the_logical_tree_and_not_stored()
    {
        var tree = await StrictTreeAsync("spw-single");

        var result = await _receiver.ApplyAsync(PlainSet(tree, "k", Pending, 1_000));

        Assert.Multiple(async () =>
        {
            Assert.That(result.Deferred, Is.False, "a diverted plain write is acknowledged, not re-sent");
            Assert.That(await StoredAsync(tree, "k"), Is.Null, "a refused plain write is never stored");
            Assert.That(await DeadLettersAsync(tree, "k"), Is.EqualTo(1), "it is diverted once, under the logical tree");
        });
    }

    [Test]
    public async Task A_refused_plain_write_in_a_merge_batch_is_diverted_and_its_siblings_land()
    {
        var tree = await StrictTreeAsync("spw-batch");

        var result = await _applier.ApplyBatchAsync(
        [
            PlainSet(tree, "a", Open, 1_000),
            PlainSet(tree, "refused", Pending, 1_001),
            PlainSet(tree, "b", Open, 1_002),
        ]);

        Assert.Multiple(async () =>
        {
            Assert.That(result.Deferred, Is.False);
            Assert.That(await StoredAsync(tree, "a"), Is.EqualTo(Open), "a valid sibling lands verbatim");
            Assert.That(await StoredAsync(tree, "b"), Is.EqualTo(Open), "a valid sibling lands verbatim");
            Assert.That(await StoredAsync(tree, "refused"), Is.Null, "the refused row is never stored");
            Assert.That(await DeadLettersAsync(tree, "refused"), Is.EqualTo(1), "the refused row is diverted once");
            Assert.That(await DeadLettersAsync(tree, "a"), Is.Zero);
        });
    }

    [Test]
    public async Task A_plain_write_to_a_shadow_routed_tree_is_judged_under_the_logical_policy()
    {
        var tree = await StrictTreeAsync("spw-shadow");
        await _cluster.Client.GetGrain<ILattice>(tree).SetAsync("seed", Open);
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(tree);
        var operationId = Guid.NewGuid().ToString("N");
        var copy = await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.GetBootstrapCopyTreeIdAsync(), Is.EqualTo(copy),
            "PRECONDITION: replicated applies are routed to the held bootstrap copy");

        await _applier.ApplyBatchAsync([PlainSet(tree, "refused", Pending, 1_000), PlainSet(tree, "valid", Open, 1_001)]);
        await resize.CompleteBootstrapCopyAsync(operationId);

        Assert.Multiple(async () =>
        {
            Assert.That(await DeadLettersAsync(tree, "refused"), Is.EqualTo(1),
                "a write routed to the copy is judged by, and dead-lettered under, the logical tree");
            Assert.That(await DeadLettersAsync(copy, "refused"), Is.Zero, "nothing is filed under the copy's id");
            Assert.That(await StoredAsync(tree, "refused"), Is.Null);
            Assert.That(await StoredAsync(tree, "valid"), Is.EqualTo(Open));
        });
    }

    [Test]
    public async Task A_non_strict_tree_stores_a_plain_write_verbatim()
    {
        var tree = await StrictTreeAsync("spw-trusted", strictIngest: false);

        await _receiver.ApplyAsync(PlainSet(tree, "single", Pending, 1_000));
        await _applier.ApplyBatchAsync([PlainSet(tree, "batched", Pending, 1_001), PlainSet(tree, "other", Open, 1_002)]);

        Assert.Multiple(async () =>
        {
            Assert.That(await StoredAsync(tree, "single"), Is.EqualTo(Pending), "a tree without strict ingest trusts a plain write");
            Assert.That(await StoredAsync(tree, "batched"), Is.EqualTo(Pending));
            Assert.That(await _schemaAdmin.CountDeadLettersAsync(tree), Is.Zero);
        });
    }

    [Test]
    public async Task A_repaired_refused_row_is_diverted_not_stored()
    {
        // An anti-entropy repair re-ships the source's rows the receiver lacks as an
        // ordinary replicated batch (TransportLeafReReplaySink); one refused by the
        // receiver's strict policy is diverted like any other plain write.
        var tree = await StrictTreeAsync("spw-repair");
        await _applier.ApplyBatchAsync([PlainSet(tree, "held", Open, 1_000)]);

        await _applier.ApplyBatchAsync([PlainSet(tree, "held", Open, 1_000), PlainSet(tree, "missing", Pending, 900)]);

        Assert.Multiple(async () =>
        {
            Assert.That(await StoredAsync(tree, "missing"), Is.Null, "a repaired row the strict policy refuses is never stored");
            Assert.That(await DeadLettersAsync(tree, "missing"), Is.EqualTo(1));
            Assert.That(await StoredAsync(tree, "held"), Is.EqualTo(Open));
        });
    }

    [Test]
    public void Repair_over_a_dead_lettered_divergence_does_not_re_ship_forever()
    {
        Assert.Ignore("#5714: a dead-lettered row is a standing digest divergence, so auto-remediation re-ships it every round and each re-delivery files another dead letter.");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "spw-receiver";
                opts.MaxApplyRetries = 3;
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.AddLatticeSchemaEnforcement(o => o.StrictIngest = true);
            siloBuilder.AddLatticeSchemaVersioning(registry => registry.AddSchema(1, 1, "spw-v1"), o => o.StrictIngest = true);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
