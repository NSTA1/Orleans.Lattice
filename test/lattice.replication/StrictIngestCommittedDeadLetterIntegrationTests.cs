using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Schema;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for #5297: under strict ingest with the real schema add-ons and
/// the real dead-letter store, the dead-letter record of a refused entry of a
/// replicated atomic batch is an ordinary committed write of the
/// <c>sys-schema-dlq</c> tree, whatever context the refused entry was judged in.
/// The append used to inherit the prepare's ambient context, so a refused prepare
/// that arrived before the commit decision staged its record in a pending bucket
/// of the dead-letter tree under the batch's transaction id, which no terminal
/// ever commits: the record was lost.
/// <para>
/// Each case delivers one refused and one valid entry of the same batch - with
/// the prepare before or after the decision, with or without a bootstrap hold
/// (#4791) - and asserts one durable dead-letter record for the refused entry,
/// which is never applied, none for the valid entry, which commits, and that the
/// record's write-ahead-log entry carries nothing of the batch: it is not
/// prepared, not under the batch's transaction id, not stamped with the batch's
/// position, the source's origin or clock, or the entry's CRDT delta. The real
/// canonical applier, dead-letter decorator, transaction registry, bootstrap
/// hold, schema stores and grains run; peer entries are synthesized as the
/// source region would ship them.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictIngestCommittedDeadLetterIntegrationTests
{
    private const string Origin = "dlqc-source";
    private const string DeadLetterTree = "sys-schema-dlq";
    private const char DeadLetterKeySeparator = '\u001f';
    private const string CrdtTreePrefix = "dlqc-crdt";
    private const string RefusedKey = "a-refused";
    private const string ValidKey = "b-valid";
    private const uint SchemaId = 71;
    private const int MaxApplyRetries = 3;
    private const long SourceClockCeiling = 10_000;

    private static readonly byte[] Open = Encoding.UTF8.GetBytes("{\"status\":\"open\"}");
    private static readonly byte[] Pending = Encoding.UTF8.GetBytes("{\"status\":\"pending\"}");

    private TestCluster _cluster = null!;
    private IServiceProvider _services = null!;
    private DeadLetterTrackingReplicationApplier _receiver = null!;
    private ILatticeSchemaAdmin _schemaAdmin = null!;
    private ILatticeSchemaVersionAdmin _versionAdmin = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _receiver = new DeadLetterTrackingReplicationApplier(
            _services.GetRequiredService<ReplicationApplier>(),
            _services.GetRequiredService<IGrainFactory>(),
            _services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        _schemaAdmin = _services.GetRequiredService<ILatticeSchemaAdmin>();
        _versionAdmin = _services.GetRequiredService<ILatticeSchemaVersionAdmin>();
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

    private static byte[] Stamped(uint version, byte[] body) => LatticeSchemaEnvelope.Encode(SchemaId, version, body);

    private static WalRecord LwwPrepare(string tree, string key, byte[] value, Guid txid, int index) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Mode = LatticeMergeMode.LwwRegister,
        Value = value,
        Timestamp = Hlc(1_000 + index),
        OriginClusterId = Origin,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = 2,
        AtomicBatchIndex = index,
    };

    private static WalRecord CrdtPrepare(string tree, string key, uint version, Guid txid, int index)
    {
        var state = new PnCounter();
        state.Increment(Origin, 3);
        return new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Mode = LatticeMergeMode.PnCounter,
            Value = Stamped(version, JsonLatticeSerializer<PnCounter>.Default.Serialize(state)),
            Delta = Stamped(version, JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(new PnCounterDelta
            {
                Increments = new Dictionary<string, long>(StringComparer.Ordinal) { [Origin] = 3 },
                Decrements = new Dictionary<string, long>(StringComparer.Ordinal),
            })),
            Timestamp = Hlc(1_000 + index),
            OriginClusterId = Origin,
            TransactionId = txid,
            IsPrepared = true,
            AtomicBatchSize = 2,
            AtomicBatchIndex = index,
        };
    }

    private static WalRecord[] Commits(string tree, Guid txid, params string[] keys)
    {
        var shards = keys
            .Select(k => LatticeSharding.GetShardIndex(k, LatticeConstants.DefaultShardCount))
            .Distinct()
            .ToArray();
        return shards.Select((shard, i) => new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.TxCommit,
            Mode = ModeOf(tree),
            Key = shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = Hlc(1_100 + i),
            OriginClusterId = Origin,
            TransactionId = txid,
            ShardIndex = shard,
            AtomicShardCount = shards.Length,
        }).ToArray();
    }

    // A terminal carries its tree's merge mode: the applier's replication-mode gate
    // rejects (and acknowledges) one whose mode differs from the tree's declared mode.
    private static LatticeMergeMode ModeOf(string tree) =>
        tree.StartsWith(CrdtTreePrefix, StringComparison.Ordinal) ? LatticeMergeMode.PnCounter : LatticeMergeMode.LwwRegister;

    private async Task DeliverAsync(WalRecord entry)
    {
        var result = await _receiver.ApplyAsync(entry);
        Assert.That(result.Deferred, Is.False, $"{entry.Op} '{entry.Key}' is acknowledged");
    }

    private async Task DeliverBatchAsync(string tree, Guid txid, WalRecord refused, WalRecord valid, bool decisionFirst)
    {
        if (decisionFirst)
        {
            // The receiver's registry already holds the commit (a decision a
            // snapshot bootstrap recorded, or a terminal that outran its prepares),
            // so each prepare is settled against it instead of staged (#4482).
            await TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid).MarkCommittedAsync(txid);
            await DeliverAsync(refused);
            await DeliverAsync(valid);
            return;
        }

        await DeliverAsync(refused);
        await DeliverAsync(valid);
        foreach (var commit in Commits(tree, txid, refused.Key, valid.Key))
        {
            await DeliverAsync(commit);
        }
    }

    private async Task UseStrictLwwTreeAsync(string tree)
    {
        await _schemaAdmin.SetPolicyAsync(tree, new LatticeSchemaPolicy(
            new[]
            {
                LatticeSchemaRule.Json(),
                LatticeSchemaRule.Regex("^(open|closed)$", memberPath: "status"),
            },
            strictIngest: true));
        await _versionAdmin.SetVersionConfigAsync(tree, new LatticeSchemaVersionConfig(SchemaId, 1, true));
    }

    /// <summary>Opens a bootstrap hold on <paramref name="tree"/>, so every apply is mirrored.</summary>
    private async Task<(ITreeResizeGrain Resize, string OperationId)> BeginHoldAsync(string tree)
    {
        await Lattice(tree).SetAsync("seed", Open);
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(tree);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True, "precondition: the hold is active");
        return (resize, operationId);
    }

    private ILattice Lattice(string tree) => _cluster.Client.GetGrain<ILattice>(tree);

    private async Task<int> DeadLettersForAsync(string tree, string key)
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

    private async Task<IReadOnlyList<WalRecord>> DeadLetterWalRecordsAsync(string tree, string key)
    {
        var partitions = await _services.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(DeadLetterTree);
        var prefix = tree + DeadLetterKeySeparator;
        var item = DeadLetterKeySeparator + key + DeadLetterKeySeparator;
        return await LeafWalReader.WaitForRecordsAsync(
            _cluster.Client,
            DeadLetterTree,
            r => r.Key is { } k && k.StartsWith(prefix, StringComparison.Ordinal) && k.Contains(item, StringComparison.Ordinal),
            partitions: partitions);
    }

    private async Task AssertOneOrdinaryCommittedDeadLetterAsync(string tree, Guid txid)
    {
        var refusedDeadLetters = await DeadLettersForAsync(tree, RefusedKey);
        var validDeadLetters = await DeadLettersForAsync(tree, ValidKey);
        var records = await DeadLetterWalRecordsAsync(tree, RefusedKey);

        Assert.Multiple(() =>
        {
            Assert.That(refusedDeadLetters, Is.EqualTo(1),
                "the refused entry's dead-letter record is durable and visible: one record per delivery");
            Assert.That(validDeadLetters, Is.Zero, "the valid entry is not dead-lettered");
            Assert.That(records, Has.Count.EqualTo(1), "the record is written to the dead-letter tree exactly once");
        });

        var record = records[0];
        Assert.Multiple(() =>
        {
            Assert.That(record.IsPrepared, Is.False,
                "the record is a committed write, not staged in a pending bucket that no terminal of the dead-letter tree ever commits");
            Assert.That(record.TransactionId, Is.Not.EqualTo(txid),
                "the record is not written under the refused batch's transaction");
            Assert.That(record.AtomicBatchSize, Is.Zero, "the record carries no atomic-batch position");
            Assert.That(record.Delta, Is.Null, "the record carries no CRDT delta of the refused entry");
            Assert.That(record.OriginClusterId, Is.Not.EqualTo(Origin),
                "the record is the receiver's own write, not attributed to the source region");
            Assert.That(record.Timestamp.WallClockTicks, Is.GreaterThan(SourceClockCeiling),
                "the record is stamped by the receiver's clock, not at the refused entry's carried source clock");
        });
    }

    [Test]
    public async Task Refused_atomic_entry_prepared_before_the_decision_leaves_one_ordinary_committed_dead_letter_record()
    {
        const string tree = "dlqc-on-time";
        await UseStrictLwwTreeAsync(tree);
        var txid = Guid.NewGuid();

        await DeliverBatchAsync(tree, txid,
            LwwPrepare(tree, RefusedKey, Stamped(1, Pending), txid, index: 0),
            LwwPrepare(tree, ValidKey, Stamped(1, Open), txid, index: 1),
            decisionFirst: false);

        Assert.That(await Lattice(tree).GetAsync(RefusedKey), Is.Null, "the refused entry is not applied");
        Assert.That(await Lattice(tree).GetAsync(ValidKey), Is.EqualTo(Open), "the rest of the batch commits");
        await AssertOneOrdinaryCommittedDeadLetterAsync(tree, txid);
    }

    [Test]
    public async Task Refused_atomic_entry_prepared_after_the_decision_leaves_one_ordinary_committed_dead_letter_record()
    {
        const string tree = "dlqc-late";
        await UseStrictLwwTreeAsync(tree);
        var txid = Guid.NewGuid();

        await DeliverBatchAsync(tree, txid,
            LwwPrepare(tree, RefusedKey, Stamped(2, Open), txid, index: 0),
            LwwPrepare(tree, ValidKey, Stamped(1, Open), txid, index: 1),
            decisionFirst: true);

        Assert.That(await Lattice(tree).GetAsync(RefusedKey), Is.Null, "the refused entry is not applied");
        Assert.That(await Lattice(tree).GetAsync(ValidKey), Is.EqualTo(Open), "the rest of the batch commits");
        await AssertOneOrdinaryCommittedDeadLetterAsync(tree, txid);
    }

    [Test]
    public async Task Refused_atomic_entry_prepared_before_the_decision_under_a_bootstrap_hold_leaves_one_ordinary_committed_dead_letter_record()
    {
        const string tree = "dlqc-hold-on-time";
        await UseStrictLwwTreeAsync(tree);
        var (resize, operationId) = await BeginHoldAsync(tree);
        var txid = Guid.NewGuid();

        await DeliverBatchAsync(tree, txid,
            LwwPrepare(tree, RefusedKey, Stamped(1, Pending), txid, index: 0),
            LwwPrepare(tree, ValidKey, Stamped(1, Open), txid, index: 1),
            decisionFirst: false);
        await resize.CompleteBootstrapCopyAsync(operationId);

        Assert.That(await Lattice(tree).GetAsync(RefusedKey), Is.Null, "the refused entry is not applied");
        Assert.That(await Lattice(tree).GetAsync(ValidKey), Is.EqualTo(Open), "the rest of the batch commits");
        await AssertOneOrdinaryCommittedDeadLetterAsync(tree, txid);
    }

    [Test]
    public async Task Refused_atomic_entry_prepared_after_the_decision_under_a_bootstrap_hold_leaves_one_ordinary_committed_dead_letter_record()
    {
        const string tree = "dlqc-hold-late";
        await UseStrictLwwTreeAsync(tree);
        var (resize, operationId) = await BeginHoldAsync(tree);
        var txid = Guid.NewGuid();

        await DeliverBatchAsync(tree, txid,
            LwwPrepare(tree, RefusedKey, Stamped(1, Pending), txid, index: 0),
            LwwPrepare(tree, ValidKey, Stamped(1, Open), txid, index: 1),
            decisionFirst: true);
        await resize.CompleteBootstrapCopyAsync(operationId);

        Assert.That(await Lattice(tree).GetAsync(RefusedKey), Is.Null, "the refused entry is not applied");
        Assert.That(await Lattice(tree).GetAsync(ValidKey), Is.EqualTo(Open), "the rest of the batch commits");
        await AssertOneOrdinaryCommittedDeadLetterAsync(tree, txid);
    }

    [Test]
    public async Task Refused_crdt_atomic_entry_prepared_before_the_decision_leaves_one_ordinary_committed_dead_letter_record()
    {
        const string tree = CrdtTreePrefix + "-on-time";
        await _versionAdmin.SetVersionConfigAsync(tree, new LatticeSchemaVersionConfig(SchemaId, 1, true));
        var txid = Guid.NewGuid();

        await DeliverBatchAsync(tree, txid,
            CrdtPrepare(tree, RefusedKey, version: 2, txid, index: 0),
            CrdtPrepare(tree, ValidKey, version: 1, txid, index: 1),
            decisionFirst: false);

        // A staged CRDT prepare is judged with the entry's delta ambient, so this
        // case is the one that shows the record does not inherit the delta.
        Assert.That(await Lattice(tree).GetAsync(RefusedKey), Is.Null, "the refused entry is not applied");
        Assert.That((await Lattice(tree).PnCounter(ValidKey).GetAsync()).Value, Is.EqualTo(3),
            "the rest of the batch commits");
        await AssertOneOrdinaryCommittedDeadLetterAsync(tree, txid);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "dlqc-receiver";
                opts.MaxApplyRetries = MaxApplyRetries;
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.AddLatticeSchemaEnforcement(o => o.StrictIngest = true);
            siloBuilder.AddLatticeSchemaVersioning(
                registry => registry
                    .AddSchema(SchemaId, 1, "dlqc-v1")
                    .AddSchema(SchemaId, 2, "dlqc-v2")
                    .AddUpcaster(SchemaId, 1, 2, LatticeValueTransform.Passthrough(LatticeValueTransform.SetMember("rev", LatticeValueTransform.Const(LatticeConstant.Integer(2))))),
                o => o.StrictIngest = true);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, ModeResolver>();
        }
    }

    private sealed class ModeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith(CrdtTreePrefix, StringComparison.Ordinal)
                ? LatticeMergeMode.PnCounter
                : LatticeMergeMode.LwwRegister;
    }
}
