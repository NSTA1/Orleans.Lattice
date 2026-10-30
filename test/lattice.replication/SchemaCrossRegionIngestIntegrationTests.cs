using System.Text;
using System.Text.Json;
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
/// Caller-level detectors for the <c>SchemaCrossRegion</c> model (spec/schema),
/// run at a replication receiver region with the real schema enforcement and
/// schema versioning add-ons registered, both globally strict. Schema definitions
/// are region-local: the receiver judges what it ingests by its own policy and
/// its own catalog, never by the source's.
/// <para>
/// A plain LWW entry is merged below the write choke point, so the receiver
/// ingests it verbatim whatever it is stamped at; a value stamped past the
/// receiver's target is stored as delivered and its read fails explicitly until
/// the receiver holds the definition, after which it reads. A typed-CRDT delta
/// and an LWW entry of a replicated atomic batch are judged at the choke point
/// under strict ingest: what the receiver cannot serve (stamped past its target,
/// or refused by its policy) is diverted to the schema dead-letter queue exactly
/// once and not applied, an older CRDT delta is lifted to the target once at
/// ingest, and the rest of the atomic batch commits. A tree without strict
/// ingest trusts the stamp. The real canonical applier, dead-letter decorator,
/// transaction registry, schema stores and grains run; peer entries are
/// synthesized as the source region would ship them.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaCrossRegionIngestIntegrationTests
{
    private const string Origin = "xr-source";
    private const string CrdtTreePrefix = "xr-crdt";
    private const uint SchemaId = 61;
    private const string UpcastMember = "rev";
    private const int MaxApplyRetries = 3;

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

    private static byte[] CounterDelta(long increment) =>
        JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["site-a"] = increment },
            Decrements = new Dictionary<string, long>(),
        });

    private static WalRecord PlainSet(string tree, string key, byte[] value, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Mode = LatticeMergeMode.LwwRegister,
        Value = value,
        Timestamp = Hlc(ticks),
        OriginClusterId = Origin,
    };

    private static WalRecord DeltaEntry(string tree, string key, byte[] delta, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Mode = LatticeMergeMode.PnCounter,
        Value = null,
        Delta = delta,
        Timestamp = Hlc(ticks),
        OriginClusterId = Origin,
    };

    private static WalRecord Prepare(string tree, string key, byte[] value, Guid txid, int index) => new()
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
            Key = shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = Hlc(1_100 + i),
            OriginClusterId = Origin,
            TransactionId = txid,
            ShardIndex = shard,
            AtomicShardCount = shards.Length,
        }).ToArray();
    }

    private async Task DeliverAsync(WalRecord entry)
    {
        var result = await _receiver.ApplyAsync(entry);
        Assert.That(result.Deferred, Is.False, $"{entry.Op} '{entry.Key}' is acknowledged");
    }

    private Task UseVersionConfigAsync(string tree, uint target, bool strictIngest) =>
        _versionAdmin.SetVersionConfigAsync(tree, new LatticeSchemaVersionConfig(SchemaId, target, strictIngest));

    private Task UseStatusPolicyAsync(string tree) =>
        _schemaAdmin.SetPolicyAsync(tree, new LatticeSchemaPolicy(
            new[]
            {
                LatticeSchemaRule.Json(),
                LatticeSchemaRule.Regex("^(open|closed)$", memberPath: "status"),
            },
            strictIngest: true));

    private ILattice Lattice(string tree) => _cluster.Client.GetGrain<ILattice>(tree);

    private async Task<byte[]?> StoredAsync(string tree, string key) =>
        (await _cluster.Client.GetGrain<IReplicationApplyGrain>(tree).ReadStoredWithVersionAsync(key)).Value;

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

    private async Task<WalRecord> PersistedDeltaAsync(string tree, string key)
    {
        var partitions = await _services.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree);
        var records = await LeafWalReader.WaitForRecordsAsync(
            _cluster.Client, tree, r => r.Key == key && r.Delta is not null, partitions: partitions);
        Assert.That(records, Has.Count.EqualTo(1), "the applied delta is persisted as exactly one WAL record");
        return records[0];
    }

    private static uint StampOf(byte[]? value)
    {
        Assert.That(value, Is.Not.Null);
        Assert.That(LatticeSchemaEnvelope.TryReadHeader(value!, out var schemaId, out var version), Is.True,
            "the persisted bytes carry a schema-version envelope");
        Assert.That(schemaId, Is.EqualTo(SchemaId));
        return version;
    }

    private static Exception Unwrap(Exception exception)
    {
        while (exception is AggregateException { InnerException: { } inner })
        {
            exception = inner;
        }

        return exception;
    }

    [Test]
    public async Task Plain_lww_entry_stamped_past_the_receivers_target_is_stored_verbatim_under_strict_ingest_and_its_read_fails_explicitly()
    {
        const string tree = "xr-lww-verbatim";
        const string key = "newer";
        await UseStatusPolicyAsync(tree);
        await UseVersionConfigAsync(tree, target: 1, strictIngest: true);
        var delivered = Stamped(2, Open);

        await DeliverAsync(PlainSet(tree, key, delivered, 1_000));

        var stored = await StoredAsync(tree, key);
        var deadLetters = await _schemaAdmin.CountDeadLettersAsync(tree);
        Exception? readFailure = null;
        byte[]? read = null;
        try
        {
            read = await Lattice(tree).GetAsync(key);
        }
        catch (Exception ex)
        {
            readFailure = Unwrap(ex);
        }

        Assert.Multiple(() =>
        {
            Assert.That(stored, Is.EqualTo(delivered),
                "a plain LWW entry is merged below the write choke point, so it is stored as delivered: still stamped past the target, neither re-stamped nor stripped");
            Assert.That(StampOf(stored), Is.EqualTo(2u), "the stored value keeps the source's stamp");
            Assert.That(deadLetters, Is.Zero, "a plain LWW entry is not judged at ingest, so nothing is dead-lettered");
            Assert.That(read, Is.Null, "a value the receiver holds no definition for is never served as a value");
            Assert.That(readFailure, Is.TypeOf<NotSupportedException>(),
                "reading a value stamped past the receiver's target fails explicitly until the receiver holds the definition");
        });
    }

    [Test]
    public async Task Plain_lww_entry_stamped_past_the_receivers_target_becomes_readable_once_the_receiver_advances_to_it()
    {
        const string tree = "xr-lww-advance";
        const string newerKey = "newer";
        const string olderKey = "older";
        await UseStatusPolicyAsync(tree);
        await UseVersionConfigAsync(tree, target: 1, strictIngest: true);
        var newer = Stamped(2, Open);
        var older = Stamped(1, Open);

        await DeliverAsync(PlainSet(tree, newerKey, newer, 1_000));
        await DeliverAsync(PlainSet(tree, olderKey, older, 1_001));
        Assert.That(async () => await Lattice(tree).GetAsync(newerKey), Throws.InstanceOf<NotSupportedException>(),
            "before the advance the receiver cannot serve the newer value");

        var advanced = await _versionAdmin.AdvanceTargetVersionAsync(tree, 2);

        var newerRead = await Lattice(tree).GetAsync(newerKey);
        var olderRead = await Lattice(tree).GetAsync(olderKey);
        var newerStored = await StoredAsync(tree, newerKey);
        var olderStored = await StoredAsync(tree, olderKey);

        Assert.Multiple(() =>
        {
            Assert.That(advanced.TargetVersion, Is.EqualTo(2u));
            Assert.That(newerRead, Is.EqualTo(Open),
                "once the receiver holds the definition the value stamped at it reads its body");
            Assert.That(olderRead, Is.Not.Null, "the value stamped below the new target reads");
            using var olderDocument = JsonDocument.Parse(olderRead!);
            Assert.That(olderDocument.RootElement.GetProperty("status").GetString(), Is.EqualTo("open"),
                "the older value reads its own body");
            Assert.That(olderDocument.RootElement.GetProperty(UpcastMember).GetInt64(), Is.EqualTo(2),
                "the older value is upcast through the receiver's v1 to v2 hop on read");
            Assert.That(newerStored, Is.EqualTo(newer), "advancing the target does not rewrite the stored value");
            Assert.That(olderStored, Is.EqualTo(older), "advancing the target does not rewrite the stored value");
        });
    }

    [Test]
    public async Task Strict_crdt_delta_stamped_past_the_receivers_target_is_dead_lettered_and_not_applied()
    {
        const string tree = CrdtTreePrefix + "-newer";
        const string key = "counter";
        await UseVersionConfigAsync(tree, target: 1, strictIngest: true);

        await DeliverAsync(DeltaEntry(tree, key, Stamped(2, CounterDelta(5)), 1_000));

        var stored = await Lattice(tree).GetAsync(key);
        var deadLettered = await DeadLettersForAsync(tree, key);
        var total = await _schemaAdmin.CountDeadLettersAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(stored, Is.Null, "a delta the receiver cannot serve is not applied");
            Assert.That(deadLettered, Is.EqualTo(1),
                "a typed-CRDT delta stamped past the receiver's target is judged at ingest and diverted to the schema dead-letter queue exactly once");
            Assert.That(total, Is.EqualTo(1), "nothing else on the tree is dead-lettered");
        });
    }

    [Test]
    public async Task Strict_crdt_delta_stamped_below_the_receivers_target_is_lifted_to_the_target_once_at_ingest()
    {
        const string tree = CrdtTreePrefix + "-lift";
        const string key = "counter";
        await UseVersionConfigAsync(tree, target: 2, strictIngest: true);

        await DeliverAsync(DeltaEntry(tree, key, Stamped(1, CounterDelta(5)), 1_000));

        var counter = await Lattice(tree).PnCounter(key).GetAsync();
        var persisted = await PersistedDeltaAsync(tree, key);
        var deadLetters = await _schemaAdmin.CountDeadLettersAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(counter.Value, Is.EqualTo(5), "the older delta is applied");
            Assert.That(deadLetters, Is.Zero, "an upcastable older delta is not dead-lettered");
            Assert.That(StampOf(persisted.Delta), Is.EqualTo(2u),
                "the receiver persists the delta lifted to its own target, so every later fold and replay reads the same at-target bytes");
            using var body = JsonDocument.Parse(LatticeSchemaEnvelope.StripToBody(persisted.Delta!));
            Assert.That(body.RootElement.GetProperty(UpcastMember).GetInt64(), Is.EqualTo(2),
                "the persisted delta body was upcast through the receiver's v1 to v2 hop at ingest");
        });
    }

    [Test]
    public async Task Strict_atomic_entry_stamped_past_the_receivers_target_is_dead_lettered_and_its_batch_siblings_commit_together()
    {
        const string tree = "xr-lww-atomic-newer";
        const string refusedKey = "a-newer";
        const string validKey = "b-current";
        await UseStatusPolicyAsync(tree);
        await UseVersionConfigAsync(tree, target: 1, strictIngest: true);
        var valid = Stamped(1, Open);
        var txid = Guid.NewGuid();

        await DeliverAsync(Prepare(tree, refusedKey, Stamped(2, Open), txid, index: 0));
        await DeliverAsync(Prepare(tree, validKey, valid, txid, index: 1));
        foreach (var commit in Commits(tree, txid, refusedKey, validKey))
        {
            await DeliverAsync(commit);
        }

        var refusedRead = await Lattice(tree).GetAsync(refusedKey);
        var refusedStored = await StoredAsync(tree, refusedKey);
        var validRead = await Lattice(tree).GetAsync(validKey);
        var validStored = await StoredAsync(tree, validKey);
        var refusedDeadLetters = await DeadLettersForAsync(tree, refusedKey);
        var validDeadLetters = await DeadLettersForAsync(tree, validKey);

        Assert.Multiple(() =>
        {
            Assert.That(refusedRead, Is.Null, "an atomic entry stamped past the receiver's target is not applied");
            Assert.That(refusedStored, Is.Null, "an atomic entry stamped past the receiver's target is not stored");
            Assert.That(refusedDeadLetters, Is.EqualTo(1),
                "an atomic entry the receiver cannot serve is judged at ingest and diverted to the schema dead-letter queue exactly once");
            Assert.That(validRead, Is.EqualTo(Open), "the rest of the batch commits and reads its body");
            Assert.That(validStored, Is.EqualTo(valid), "the committed sibling is stored with its own stamp, v1");
            Assert.That(validDeadLetters, Is.Zero);
        });
    }

    [Test]
    public async Task Strict_atomic_entry_the_receivers_policy_refuses_is_dead_lettered_and_its_batch_siblings_commit_together()
    {
        const string tree = "xr-lww-atomic-policy";
        const string refusedKey = "a-pending";
        const string validKey = "b-open";
        await UseStatusPolicyAsync(tree);
        await UseVersionConfigAsync(tree, target: 1, strictIngest: true);
        var valid = Stamped(1, Open);
        var txid = Guid.NewGuid();

        await DeliverAsync(Prepare(tree, refusedKey, Stamped(1, Pending), txid, index: 0));
        await DeliverAsync(Prepare(tree, validKey, valid, txid, index: 1));
        foreach (var commit in Commits(tree, txid, refusedKey, validKey))
        {
            await DeliverAsync(commit);
        }

        var refusedRead = await Lattice(tree).GetAsync(refusedKey);
        var refusedStored = await StoredAsync(tree, refusedKey);
        var validRead = await Lattice(tree).GetAsync(validKey);
        var refusedDeadLetters = await DeadLettersForAsync(tree, refusedKey);
        var validDeadLetters = await DeadLettersForAsync(tree, validKey);

        Assert.Multiple(() =>
        {
            Assert.That(refusedRead, Is.Null, "an atomic entry the receiver's own policy refuses is not applied");
            Assert.That(refusedStored, Is.Null, "an atomic entry the receiver's own policy refuses is not stored");
            Assert.That(refusedDeadLetters, Is.EqualTo(1),
                "an atomic entry is judged by the receiver's policy, not the source's, and diverted to the schema dead-letter queue exactly once");
            Assert.That(validRead, Is.EqualTo(Open), "the rest of the batch commits and reads its body");
            Assert.That(validDeadLetters, Is.Zero);
        });
    }

    [Test]
    public async Task Crdt_delta_stamped_past_the_target_of_a_tree_without_strict_ingest_is_stored_verbatim()
    {
        const string tree = CrdtTreePrefix + "-trusted";
        const string key = "counter";
        await UseVersionConfigAsync(tree, target: 1, strictIngest: false);
        var delta = Stamped(2, CounterDelta(5));

        await DeliverAsync(DeltaEntry(tree, key, delta, 1_000));

        var counter = await Lattice(tree).PnCounter(key).GetAsync();
        var persisted = await PersistedDeltaAsync(tree, key);
        var deadLetters = await _schemaAdmin.CountDeadLettersAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(counter.Value, Is.EqualTo(5), "a tree without strict ingest trusts the delta and applies it");
            Assert.That(deadLetters, Is.Zero, "a tree without strict ingest dead-letters nothing");
            Assert.That(persisted.Delta, Is.EqualTo(delta),
                "a trusted delta is persisted verbatim, still stamped past the target");
            Assert.That(StampOf(persisted.Delta), Is.EqualTo(2u));
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "xr-receiver";
                opts.MaxApplyRetries = MaxApplyRetries;
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.AddLatticeSchemaEnforcement(o => o.StrictIngest = true);
            siloBuilder.AddLatticeSchemaVersioning(
                registry => registry
                    .AddSchema(SchemaId, 1, "xr-v1")
                    .AddSchema(SchemaId, 2, "xr-v2")
                    .AddUpcaster(SchemaId, 1, 2, LatticeValueTransform.Passthrough(
                        LatticeValueTransform.SetMember(
                            UpcastMember, LatticeValueTransform.Const(LatticeConstant.Integer(2))))),
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
